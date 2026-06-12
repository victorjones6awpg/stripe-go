package main

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory db: %v", err)
	}
	if err := InitSchema(db); err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}
	return db
}

func TestWebhookHandler_Success(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	var businessLogicCalled int32
	var onSuccessCalled int32

	businessLogic := func(ctx context.Context, eventID string) error {
		atomic.AddInt32(&businessLogicCalled, 1)
		return nil
	}

	onSuccess := func(ctx context.Context, eventID string) {
		atomic.AddInt32(&onSuccessCalled, 1)
	}

	handler := NewWebhookHandler(db, "", businessLogic, onSuccess)

	// First request
	payload := []byte(`{"id": "evt_test_123"}`)
	req := httptest.NewRequest("POST", "/webhook", bytes.NewBuffer(payload))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	if atomic.LoadInt32(&businessLogicCalled) != 1 {
		t.Errorf("expected business logic to be called 1 time, got %d", businessLogicCalled)
	}

	if atomic.LoadInt32(&onSuccessCalled) != 1 {
		t.Errorf("expected onSuccess to be called 1 time, got %d", onSuccessCalled)
	}

	// Second request (duplicate)
	req2 := httptest.NewRequest("POST", "/webhook", bytes.NewBuffer(payload))
	rec2 := httptest.NewRecorder()

	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("expected status 200 for duplicate, got %d", rec2.Code)
	}

	if atomic.LoadInt32(&businessLogicCalled) != 1 {
		t.Errorf("expected business logic to still be called 1 time, got %d", businessLogicCalled)
	}

	if atomic.LoadInt32(&onSuccessCalled) != 1 {
		t.Errorf("expected onSuccess to still be called 1 time, got %d", onSuccessCalled)
	}
}

func TestWebhookHandler_Concurrency(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	var businessLogicCalled int32
	startChan := make(chan struct{})
	blockChan := make(chan struct{})

	businessLogic := func(ctx context.Context, eventID string) error {
		atomic.AddInt32(&businessLogicCalled, 1)
		close(startChan)
		<-blockChan
		return nil
	}

	handler := NewWebhookHandler(db, "", businessLogic, nil)

	payload := []byte(`{"id": "evt_concurrent_123"}`)

	var wg sync.WaitGroup
	results := make(chan int, 5)

	// Start the first request which will block in business logic
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest("POST", "/webhook", bytes.NewBuffer(payload))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		results <- rec.Code
	}()

	// Wait until the first request is inside the business logic
	<-startChan

	// Fire 4 concurrent requests while the first one is still processing
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/webhook", bytes.NewBuffer(payload))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			results <- rec.Code
		}()
	}

	// Let the first request finish
	close(blockChan)
	wg.Wait()
	close(results)

	var successCount, conflictCount int
	for code := range results {
		if code == http.StatusOK {
			successCount++
		} else if code == http.StatusTooManyRequests {
			conflictCount++
		} else {
			t.Errorf("unexpected status code: %d", code)
		}
	}

	if successCount != 1 {
		t.Errorf("expected exactly 1 success, got %d", successCount)
	}

	if conflictCount != 4 {
		t.Errorf("expected exactly 4 conflicts (429), got %d", conflictCount)
	}

	if atomic.LoadInt32(&businessLogicCalled) != 1 {
		t.Errorf("expected business logic to be called exactly once, got %d", businessLogicCalled)
	}
}

func TestWebhookHandler_Timeout(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	var businessLogicCalled int32
	var onSuccessCalled int32

	businessLogic := func(ctx context.Context, eventID string) error {
		atomic.AddInt32(&businessLogicCalled, 1)
		select {
		case <-time.After(11 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	onSuccess := func(ctx context.Context, eventID string) {
		atomic.AddInt32(&onSuccessCalled, 1)
	}

	handler := NewWebhookHandler(db, "", businessLogic, onSuccess)

	payload := []byte(`{"id": "evt_timeout_123"}`)
	req := httptest.NewRequest("POST", "/webhook", bytes.NewBuffer(payload))
	
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500 on timeout, got %d", rec.Code)
	}

	if atomic.LoadInt32(&onSuccessCalled) != 0 {
		t.Errorf("expected onSuccess NOT to be called on timeout, got %d", onSuccessCalled)
	}

	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM processed_stripe_events WHERE event_id = 'evt_timeout_123'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query db: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records in DB after rollback, got %d", count)
	}
}
