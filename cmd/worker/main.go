// File: cmd/worker/main.go
package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// --- Connect to RabbitMQ (same as before) ---
	rabbitConn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer rabbitConn.Close()

	ch, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("logs", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	// --- Connect to PostgreSQL ---
	// Database connection string (DSN)
	dsn := "postgres://myuser:mysecretpassword@localhost:5433/mydatabase" // <-- CRITICAL: Port updated to 5433!
	dbConn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}
	defer dbConn.Close(context.Background())
	log.Println("Successfully connected to PostgreSQL on port 5433")

	// --- Start consuming messages ---
	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
	}

	var forever chan struct{}
	go func() {
		for d := range msgs {
			// --- Insert into database instead of printing ---
			log.Printf("Worker received a log: %s", d.Body)

			// The SQL query to insert the JSONB data
			sqlStatement := `INSERT INTO logs (data) VALUES ($1)`

			// Execute the query
			_, err := dbConn.Exec(context.Background(), sqlStatement, d.Body)
			if err != nil {
				// It's important to handle errors, maybe send to a "dead-letter" queue
				log.Printf("Failed to insert log into database: %v", err)
			} else {
				log.Printf("Successfully inserted log into database")
			}
		}
	}()

	log.Printf(" [*] Worker started. To exit press CTRL+C")
	<-forever
}
