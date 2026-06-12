package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stripe/stripe-go/v72"
	"github.com/stripe/stripe-go/v72/webhook"
)

// WebhookHandler handles Stripe webhooks with strict idempotency.
type WebhookHandler struct {
	db             *sql.DB
	endpointSecret string
	businessLogic  func(ctx context.Context, eventID string) error
	onSuccess      func(ctx context.Context, eventID string)
}

// NewWebhookHandler creates a new WebhookHandler.
func NewWebhookHandler(db *sql.DB, endpointSecret string, businessLogic func(ctx context.Context, eventID string) error, onSuccess func(ctx context.Context, eventID string)) *WebhookHandler {
	return &WebhookHandler{
		db:             db,
		endpointSecret: endpointSecret,
		businessLogic:  businessLogic,
		onSuccess:      onSuccess,
	}
}

// InitSchema initializes the database schema.
func InitSchema(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS processed_stripe_events (
		event_id VARCHAR(255) PRIMARY KEY,
		status VARCHAR(50) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`
	_, err := db.Exec(query)
	return err
}

func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	var eventID string
	if h.endpointSecret != "" {
		sigHeader := r.Header.Get("Stripe-Signature")
		event, err := webhook.ConstructEvent(payload, sigHeader, h.endpointSecret)
		if err != nil {
			http.Error(w, fmt.Sprintf("Bad signature: %v", err), http.StatusBadRequest)
			return
		}
		eventID = event.ID
	} else {
		// Fallback to direct JSON parsing if no secret is configured (useful for testing)
		var event struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(payload, &event); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}
		eventID = event.ID
	}

	if eventID == "" {
		http.Error(w, "Missing event ID", http.StatusBadRequest)
		return
	}

	// Start transaction with context timeout
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// Attempt to insert the event with status 'processing'
	_, err = tx.ExecContext(ctx, `
		INSERT INTO processed_stripe_events (event_id, status, created_at, updated_at)
		VALUES (?, 'processing', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`, eventID)

	if err != nil {
		// Insert failed. Rollback the current transaction first.
		tx.Rollback()

		// Query the status of the event using a new query context
		var status string
		queryCtx, queryCancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer queryCancel()

		errQuery := h.db.QueryRowContext(queryCtx, `
			SELECT status FROM processed_stripe_events WHERE event_id = ?
		`, eventID).Scan(&status)

		if errQuery != nil {
			if errors.Is(errQuery, sql.ErrNoRows) {
				http.Error(w, fmt.Sprintf("Database error: %v", err), http.StatusInternalServerError)
				return
			}
			http.Error(w, "Database error", http.StatusInternalServerError)
			return
		}

		switch status {
		case "completed":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Event already processed"))
			return
		case "processing":
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte("Event is currently being processed"))
			return
		case "failed":
			// If it failed previously, we can try to re-process it.
			reTx, errTx := h.db.BeginTx(ctx, nil)
			if errTx != nil {
				http.Error(w, "Database error", http.StatusInternalServerError)
				return
			}
			defer reTx.Rollback()

			res, errUpdate := reTx.ExecContext(ctx, `
				UPDATE processed_stripe_events
				SET status = 'processing', updated_at = CURRENT_TIMESTAMP
				WHERE event_id = ? AND status = 'failed'
			`, eventID)
			if errUpdate != nil {
				http.Error(w, "Database error", http.StatusInternalServerError)
				return
			}
			rowsAffected, _ := res.RowsAffected()
			if rowsAffected == 0 {
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte("Event is currently being processed"))
				return
			}

			// Execute business logic (inside transaction)
			if h.businessLogic != nil {
				if err := h.businessLogic(ctx, eventID); err != nil {
					http.Error(w, fmt.Sprintf("Business logic failed: %v", err), http.StatusInternalServerError)
					return
				}
			}

			// Update status to 'completed'
			_, errUpdate = reTx.ExecContext(ctx, `
				UPDATE processed_stripe_events
				SET status = 'completed', updated_at = CURRENT_TIMESTAMP
				WHERE event_id = ?
			`, eventID)
			if errUpdate != nil {
				http.Error(w, "Database error", http.StatusInternalServerError)
				return
			}

			// Commit the transaction
			if err := reTx.Commit(); err != nil {
				http.Error(w, "Database commit failed", http.StatusInternalServerError)
				return
			}

			// Execute deferred external actions (outside transaction, after successful commit)
			if h.onSuccess != nil {
				h.onSuccess(ctx, eventID)
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Event processed successfully"))
			return
		default:
			http.Error(w, "Unknown event status", http.StatusInternalServerError)
			return
		}
	}

	// Execute business logic (inside transaction)
	if h.businessLogic != nil {
		if err := h.businessLogic(ctx, eventID); err != nil {
			http.Error(w, fmt.Sprintf("Business logic failed: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Update status to 'completed'
	_, err = tx.ExecContext(ctx, `
		UPDATE processed_stripe_events
		SET status = 'completed', updated_at = CURRENT_TIMESTAMP
		WHERE event_id = ?
	`, eventID)
	if err != nil {
		http.Error(w, "Database error", http.StatusInternalServerError)
		return
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		http.Error(w, "Database commit failed", http.StatusInternalServerError)
		return
	}

	// Execute deferred external actions (outside transaction, after successful commit)
	if h.onSuccess != nil {
		h.onSuccess(ctx, eventID)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Event processed successfully"))
}

func main() {
	dbPath := os.Getenv("DATABASE_URL")
	if dbPath == "" {
		dbPath = "stripe_events.db"
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := InitSchema(db); err != nil {
		log.Fatalf("Failed to initialize schema: %v", err)
	}

	endpointSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")

	businessLogic := func(ctx context.Context, eventID string) error {
		log.Printf("Processing event %s in transaction...", eventID)
		return nil
	}

	onSuccess := func(ctx context.Context, eventID string) {
		log.Printf("Event %s successfully committed. Sending email/notification...", eventID)
	}

	handler := NewWebhookHandler(db, endpointSecret, businessLogic, onSuccess)
	http.Handle("/webhook", handler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("Server starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
