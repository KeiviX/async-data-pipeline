// package main implements the primary API server for the data pipeline.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found, reading from environment")
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL environment variable is not set")
	}

	rabbitConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer rabbitConn.Close()
	log.Println("Successfully connected to RabbitMQ")

	http.HandleFunc("/log", logHandler(rabbitConn))
	http.HandleFunc("/health", healthCheckHandler)

	log.Println("Starting API server on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("could not start server: %s", err)
	}
}

func logHandler(conn *amqp.Connection) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusInternalServerError)
			return
		}
		if err := publishToRabbit(conn, body); err != nil {
			log.Printf("Failed to publish to RabbitMQ: %s", err)
			http.Error(w, "Failed to process log", http.StatusInternalServerError)
			return
		}
		log.Printf("Published log to RabbitMQ: %s", string(body))
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, "Log accepted")
	}
}

func publishToRabbit(conn *amqp.Connection, body []byte) error {
	if conn == nil {
		log.Printf("Skipping RabbitMQ publish due to nil connection (test mode)")
		return nil
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	defer ch.Close()
	q, err := ch.QueueDeclare("logs", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ch.PublishWithContext(ctx, "", q.Name, false, false, amqp.Publishing{ContentType: "application/json", Body: body})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}
	return nil
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}
