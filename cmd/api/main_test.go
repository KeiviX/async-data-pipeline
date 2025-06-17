package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLogHandler(t *testing.T) {

	t.Run("handles valid POST request", func(t *testing.T) {
		reqBody := strings.NewReader(`{"level":"info","message":"test log"}`)
		req := httptest.NewRequest(http.MethodPost, "/log", reqBody)

		rr := httptest.NewRecorder()

		handler := logHandler(nil)

		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusAccepted {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusAccepted)
		}
	})

	t.Run("rejects non-POST requests", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/log", nil)

		rr := httptest.NewRecorder()

		handler := logHandler(nil)

		handler.ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusMethodNotAllowed {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusMethodNotAllowed)
		}
	})
}
