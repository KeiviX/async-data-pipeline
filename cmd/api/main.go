// File: cmd/api/main.go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Global variable for the RabbitMQ connection
var rabbitConn *amqp.Connection

func main() {
	var err error
	// Connect to RabbitMQ
	rabbitConn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer rabbitConn.Close()

	log.Println("Successfully connected to RabbitMQ")

	// The rest of the setup is the same
	http.HandleFunc("/log", logHandler)
	http.HandleFunc("/health", healthCheckHandler)

	log.Println("Starting API server on port 8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("could not start server: %s\n", err)
	}
}

func logHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	// Publish the log to RabbitMQ instead of printing
	if err := publishToRabbit(body); err != nil {
		log.Printf("Failed to publish to RabbitMQ: %s", err)
		http.Error(w, "Failed to process log", http.StatusInternalServerError)
		return
	}

	log.Printf("Published log to RabbitMQ: %s\n", string(body))

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, "Log accepted")
}

func publishToRabbit(body []byte) error {
	ch, err := rabbitConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	defer ch.Close()

	// Declare a queue to send to. This is idempotent.
	q, err := ch.QueueDeclare(
		"logs", // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare a queue: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish the message
	err = ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key (queue name)
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish a message: %w", err)
	}
	return nil
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "OK")
}
