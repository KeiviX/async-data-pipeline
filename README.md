# Log Ingestion & Analytics Platform

A high-throughput, scalable, and asynchronous data ingestion platform built with Go, Docker, RabbitMQ, and PostgreSQL. This project demonstrates a decoupled microservices architecture designed for reliability and performance.

---

### Architecture Diagram

```mermaid
graph TD
    A[User] -- HTTP POST --> B(API Service - Go);
    B -- Publish Message --> C((RabbitMQ));
    C -- Consume Message --> D(Worker Service - Go);
    D -- Insert Data --> E[(PostgreSQL)];

    subgraph Future Roadmap
        F(Query Service - Go) -- Query Data --> E;
        F <-.-> G([Redis Cache]);
    end
```
*Flow: An HTTP request is sent to the API Service, which publishes a message to a RabbitMQ queue. A Worker Service consumes the message and persists the data to a PostgreSQL Database. A separate Query Service (with a Redis cache) reads from the database.*

---

### Key Features

* **Asynchronous Processing:** Ingests data via a lightweight API and processes it in the background using a message queue, ensuring no data is lost and the API remains highly responsive.
* **Scalable Services:** Both the API and Worker services are decoupled and can be scaled horizontally and independently to handle increased load.
* **Persistent Storage:** Log data is securely stored in a PostgreSQL database with an efficient `JSONB` format.
* **Comprehensive Testing:** Includes a suite of unit and integration tests to ensure system reliability and correctness.
* **Ready for Caching:** Designed with a clear path for implementing a Redis caching layer to accelerate data retrieval.
* **Containerized Environment:** The entire stack (services, databases, message queue) is designed to run within Docker containers.

---

### Tech Stack

* **Backend:** Go (Golang)
* **Database:** PostgreSQL
* **Messaging:** RabbitMQ
* **Caching:** Redis
* **Containerization:** Docker & Testcontainers
* **Infrastructure as Code:** Terraform

---

### Running the Project Locally

**Prerequisites:**
* Go (version 1.22+)
* Docker

1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/KeiviX/async-data-pipeline.git](https://github.com/KeiviX/async-data-pipeline.git)
    cd async-data-pipeline
    ```

2.  **Set up the Environment File:**
    Create a `.env` file in the root of the project and paste the following content. This file is used for local development.
    ```env
    # Configuration for local development
    RABBITMQ_URL=amqp://guest:guest@localhost:5672/
    POSTGRES_URL=postgres://myuser:mysecretpassword@localhost:5433/mydatabase
    ```

3.  **Start Infrastructure Services:**
    This project uses Docker to run PostgreSQL, RabbitMQ, and Redis. Ensure Docker Desktop is running.
    ```bash
    # Command to start Postgres on port 5433
    docker run -d -p 5433:5432 -e POSTGRES_USER=myuser -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=mydatabase --name my-postgres -v postgres-data:/var/lib/postgresql/data postgres:16

    # Command to start RabbitMQ
    docker run -d -p 5672:5672 -p 15672:15672 --name my-rabbitmq rabbitmq:3-management

    # Command to start Redis
    docker run -d -p 6379:6379 --name my-redis redis:7
    ```

4.  **Connect to the Database & Create the Table:**
    Using a database client (DBeaver, Postico), connect to the PostgreSQL container on port `5433` and run the following SQL command:
    ```sql
    CREATE TABLE IF NOT EXISTS logs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        data JSONB NOT NULL,
        inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ```

5.  **Run the Go Services:**
    Open two separate terminal windows in the project directory.

    * In terminal 1, run the API service:
        ```bash
        go run ./cmd/api/main.go
        ```
    * In terminal 2, run the Worker service:
        ```bash
        go run ./cmd/worker/main.go
        ```

6.  **Send a Test Log:**
    Open a third terminal and use `curl` to send data to the platform:
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{"level":"info","message":"testing the platform"}' http://localhost:8080/log
    ```
    The log will be processed and stored in the database.

---

### Testing

This project includes a comprehensive test suite to ensure code quality and system reliability.

#### Unit Tests
Unit tests are located alongside the application code (e.g., `cmd/api/main_test.go`). They verify the logic of individual functions in isolation using Go's standard testing library and `net/http/httest`.

To run all unit tests in the project:
```bash
go test ./...