// package integration contains end-to-end tests for the data pipeline.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPipeline(t *testing.T) {
	ctx := context.Background()

	// --- ARRANGE ---

	// 1. Define and start PostgreSQL container
	pgRequest := testcontainers.ContainerRequest{
		Image:        "postgres:16-alpine",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "user",
			"POSTGRES_PASSWORD": "password",
			"POSTGRES_DB":       "test-db",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(10 * time.Second),
	}
	pgContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: pgRequest, Started: true})
	if err != nil {
		t.Fatalf("failed to start postgres container: %s", err)
	}
	defer pgContainer.Terminate(ctx)

	// 2. Define and start RabbitMQ container
	rabbitRequest := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3-management-alpine",
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete").WithOccurrence(1).WithStartupTimeout(10 * time.Second),
	}
	rabbitContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: rabbitRequest, Started: true})
	if err != nil {
		t.Fatalf("failed to start rabbitmq container: %s", err)
	}
	defer rabbitContainer.Terminate(ctx)

	// 3. Get connection strings for the containers
	pgHost, _ := pgContainer.Host(ctx)
	pgPort, _ := pgContainer.MappedPort(ctx, "5432/tcp")
	pgConnStr := fmt.Sprintf("postgres://user:password@%s:%s/test-db?sslmode=disable", pgHost, pgPort.Port())

	rabbitHost, _ := rabbitContainer.Host(ctx)
	rabbitPort, _ := rabbitContainer.MappedPort(ctx, "5672/tcp")
	rabbitConnStr := fmt.Sprintf("amqp://guest:guest@%s:%s/", rabbitHost, rabbitPort.Port())

	// 4. Create table in the test database
	db, err := pgx.Connect(ctx, pgConnStr)
	if err != nil {
		t.Fatalf("failed to connect to test postgres db: %s", err)
	}
	defer db.Close(ctx)
	_, err = db.Exec(ctx, `CREATE TABLE IF NOT EXISTS logs (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), data JSONB NOT NULL, inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW());`)
	if err != nil {
		t.Fatalf("failed to create logs table: %s", err)
	}

	// 5. Start our Go services as background processes
	apiCmd := exec.Command("../bin/api")
	apiCmd.Env = append(os.Environ(), fmt.Sprintf("RABBITMQ_URL=%s", rabbitConnStr))
	apiCmd.Stdout = os.Stdout
	apiCmd.Stderr = os.Stderr
	if err := apiCmd.Start(); err != nil {
		t.Fatalf("failed to start api service: %s", err.Error())
	}
	defer apiCmd.Process.Kill()

	workerCmd := exec.Command("../bin/worker")
	workerCmd.Env = append(os.Environ(), fmt.Sprintf("RABBITMQ_URL=%s", rabbitConnStr), fmt.Sprintf("POSTGRES_URL=%s", pgConnStr))
	workerCmd.Stdout = os.Stdout
	workerCmd.Stderr = os.Stderr
	if err := workerCmd.Start(); err != nil {
		t.Fatalf("failed to start worker service: %s", err.Error())
	}
	defer workerCmd.Process.Kill()

	// 5a. Wait for the API service to be ready using a readiness probe.
	log.Println("Waiting for API service to be ready...")
	apiReadyCtx, cancelApiReady := context.WithTimeout(ctx, 15*time.Second)
	defer cancelApiReady()
	apiReady := false
	httpClient := &http.Client{Timeout: 1 * time.Second}
	for !apiReady {
		select {
		case <-apiReadyCtx.Done():
			t.Fatalf("timed out waiting for api service to become healthy")
		case <-time.After(500 * time.Millisecond):
			resp, err := httpClient.Get("http://localhost:8080/health")
			if err == nil && resp.StatusCode == http.StatusOK {
				apiReady = true
				log.Println("API service is healthy.")
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
	}

	// --- ACT ---
	logPayload := `{"level":"integration-test","message":"this is a test"}`
	resp, err := http.Post("http://localhost:8080/log", "application/json", strings.NewReader(logPayload))
	if err != nil {
		t.Fatalf("failed to send http request to api: %s", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("api returned wrong status code: got %d want %d, body: %s", resp.StatusCode, http.StatusAccepted, string(body))
	}
	resp.Body.Close()

	// --- ASSERT ---
	log.Println("Polling database for result...")
	pollConn, err := pgx.Connect(ctx, pgConnStr)
	if err != nil {
		t.Fatalf("failed to create polling connection to postgres: %s", err)
	}
	defer pollConn.Close(ctx)
	var savedData string
	var found bool
	queryCtx, cancelQuery := context.WithTimeout(ctx, 10*time.Second)
	defer cancelQuery()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for !found {
		select {
		case <-queryCtx.Done():
			t.Fatalf("timed out waiting for log to appear in database")
		case <-ticker.C:
			err := pollConn.QueryRow(queryCtx, "SELECT data::text FROM logs LIMIT 1").Scan(&savedData)
			if err == nil {
				found = true
			} else if err != pgx.ErrNoRows {
				t.Fatalf("error querying database: %s", err)
			}
		}
	}
	// Check if the saved data matches the expected log payload
	var want map[string]interface{}
	if err := json.Unmarshal([]byte(logPayload), &want); err != nil {
		t.Fatalf("failed to unmarshal wanted payload: %s", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal([]byte(savedData), &got); err != nil {
		t.Fatalf("failed to unmarshal saved data: %s", err)
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("data mismatch:\ngot:  %v\nwant: %v", got, want)
	}
}
