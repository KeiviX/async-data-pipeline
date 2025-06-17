// package main implements the background worker for the data pipeline.
package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
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
	postgresURL := os.Getenv("POSTGRES_URL")
	if postgresURL == "" {
		log.Fatal("POSTGRES_URL environment variable is not set")
	}

	rabbitConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}
	defer rabbitConn.Close()
	log.Println("Successfully connected to RabbitMQ")

	ch, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %s", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("logs", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %s", err)
	}

	dbConn, err := pgx.Connect(context.Background(), postgresURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer dbConn.Close(context.Background())
	log.Println("Successfully connected to PostgreSQL")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %s", err)
	}

	var forever chan struct{}
	go func() {
		for d := range msgs {
			log.Printf("Worker received a log: %s", d.Body)
			sqlStatement := `INSERT INTO logs (data) VALUES ($1)`
			_, err := dbConn.Exec(context.Background(), sqlStatement, d.Body)
			if err != nil {
				log.Printf("Failed to insert log into database: %v", err)
			} else {
				log.Printf("Successfully inserted log into database")
			}
		}
	}()

	log.Printf(" [*] Worker started. To exit press CTRL+C")
	<-forever
}
