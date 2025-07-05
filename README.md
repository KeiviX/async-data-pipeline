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
    This project uses Docker Compose to run PostgreSQL, RabbitMQ, and Redis. Ensure Docker Desktop is running.
    ```bash
    docker-compose up -d
    ```
    This command will start all the services in the background.

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

### Infrastructure as Code

This project uses Terraform to manage the infrastructure as code. The Terraform files are located in the `terraform` directory.

**Prerequisites:**
* Terraform (version 1.0+)
* AWS Account and configured credentials

1.  **Initialize Terraform:**
    ```bash
    cd terraform
    terraform init
    ```

2.  **Plan the infrastructure:**
    ```bash
    terraform plan
    ```

3.  **Apply the infrastructure:**
    ```bash
    terraform apply
    ```

4.  **Destroy the infrastructure:**
    ```bash
    terraform destroy
    ```

---

### CI/CD

This project uses GitHub Actions for Continuous Integration. The workflow is defined in the `.github/workflows/ci.yml` file.

On every push or pull request to the `main` branch, the following actions are performed:

*   The code is checked out.
*   The Go environment is set up.
*   The unit tests are run.

---

### Testing

This project includes a comprehensive test suite to ensure code quality and system reliability.

#### Unit Tests
Unit tests are located alongside the application code (e.g., `cmd/api/main_test.go`). They verify the logic of individual functions in isolation using Go's standard testing library and `net/http/httest`.

To run all unit tests in the project:
```bash
go test ./...