# Log Ingestion & Analytics Platform

A high-throughput, scalable, and asynchronous data ingestion platform built with Go, Docker, RabbitMQ, and PostgreSQL. This project demonstrates a decoupled microservices architecture designed for reliability and performance.

---

### Architecture Diagram

*(This is the most impressive part of a README! Use a free tool like [draw.io](https://app.diagrams.net/) or [Excalidraw](https://excalidraw.com/) to create a simple diagram showing the flow, save it as an image in your project folder, and embed it here.)*

![Architecture Diagram](./architecture.png)
*Example Flow: HTTP Request -> API Service -> RabbitMQ -> Worker Service -> PostgreSQL Database. A separate Query Service with a Redis cache reads from the database.*

---

### Key Features

* **Asynchronous Processing:** Ingests data via a lightweight API and processes it in the background using a message queue, ensuring no data is lost and the API remains highly responsive.
* **Scalable Services:** Both the API and Worker services are decoupled and can be scaled horizontally and independently to handle increased load.
* **Persistent Storage:** Log data is securely stored in a PostgreSQL database with an efficient `JSONB` format.
* **Ready for Caching:** Designed with a clear path for implementing a Redis caching layer to accelerate data retrieval.
* **Containerized Environment:** The entire stack (services, databases, message queue) is designed to run within Docker containers.

---

### Tech Stack

* **Backend:** Go (Golang)
* **Database:** PostgreSQL
* **Messaging:** RabbitMQ
* **Caching:** Redis
* **Containerization:** Docker
* **Infrastructure as Code:** Terraform

---

### Running the Project Locally

**Prerequisites:**
* Go (version 1.22+)
* Docker

1.  **Clone the repository:**
    ```bash
    git clone [YOUR_GITHUB_URL]
    cd [YOUR_REPO_NAME]
    ```

2.  **Start Infrastructure Services:**
    This project uses Docker to run PostgreSQL, RabbitMQ, and Redis. Ensure Docker Desktop is running.
    ```bash
    # Command to start Postgres on port 5433
    docker run -d -p 5433:5432 -e POSTGRES_USER=myuser -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=mydatabase --name my-postgres -v postgres-data:/var/lib/postgresql/data postgres:16

    # Command to start RabbitMQ
    docker run -d -p 5672:5672 -p 15672:15672 --name my-rabbitmq rabbitmq:3-management

    # Command to start Redis
    docker run -d -p 6379:6379 --name my-redis redis:7
    ```

3.  **Connect to the Database & Create the Table:**
    Using a database client (DBeaver, Postico), connect to the PostgreSQL container on port `5433` and run the following SQL command:
    ```sql
    CREATE TABLE IF NOT EXISTS logs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        data JSONB NOT NULL,
        inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ```

4.  **Run the Go Services:**
    Open two separate terminal windows in the project directory.

    * In terminal 1, run the API service:
        ```bash
        go run ./cmd/api/main.go
        ```
    * In terminal 2, run the Worker service:
        ```bash
        go run ./cmd/worker/main.go
        ```

5.  **Send a Test Log:**
    Open a third terminal and use `curl` to send data to the platform:
    ```bash
    curl -X POST -H "Content-Type: application/json" -d '{"level":"info","message":"testing the platform"}' http://localhost:8080/log
    ```
    The log will be processed and stored in the database.

---

### Future Improvements

* [ ] Implement the Query Service with a Redis caching layer.
* [ ] Create `Dockerfile`s for the Go services.
* [ ] Create a `docker-compose.yml` file to manage the entire stack.
* [ ] Add unit and integration tests.
* [ ] Set up a CI/CD pipeline using GitHub Actions.