
# 🧠 COVID-19 Kafka → Spark Streaming Data Pipeline

This project demonstrates a **complete data engineering pipeline** using:
- **Kafka** (as streaming message broker)
- **Spark Structured Streaming** (for ingestion & transformation)
- **Docker Compose** (for orchestration)
- **Confluent Kafka Python producer** (for data ingestion from API or offline generator)

The pipeline continuously fetches COVID-19 data (live API or offline simulation),
publishes it to Kafka (`covid_events` topic), processes it via Spark Structured Streaming,
and writes partitioned **Parquet** and **CSV** outputs.

---

## 📁 Project Structure

```

covid-pipeline/
├── docker-compose.yml
├── .env
├── producer/
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
└── spark/
└── app.py

```

---

## 🚀 Architecture Overview

```
      +--------------------+
      |  disease.sh API    |
      |  (or offline mock) |
      +---------+----------+
                |
                v
     +----------+-----------+
     |  Confluent Kafka     |
     |  Python Producer     |
     |  (container: producer)|
     +----------+-----------+
                |
                v
  +-------------+--------------+
  |     Kafka Broker (9092)    |
  |     Topic: covid_events    |
  +-------------+--------------+
                |
                v
   +------------+-------------+
   |  Spark Structured Stream |
   |  (container: spark-streaming) |
   +------------+-------------+
                |
                v
+----------------------------------------+
|  Data Lake Output: /data/output/covid  |
|     ├── parquet/p_date=YYYY-MM-DD/     |
|     └── csv/p_date=YYYY-MM-DD/         |
+----------------------------------------+

````

---

## 🧩 Components & Roles

| Service | Description |
|----------|-------------|
| **zookeeper** | Stores Kafka cluster metadata (dev-mode single instance). |
| **kafka** | Core broker (`kafka:9092`), hosts topic `covid_events`. |
| **kafka-init** | One-shot topic initializer. Ensures `covid_events` exists before producer/Spark start. |
| **spark-master / spark-worker** | Run a small Spark cluster with UI on port 8080. |
| **spark-streaming** | Executes the PySpark streaming job (`spark/app.py`). Reads from Kafka, writes Parquet + CSV. |
| **producer** | Publishes COVID data to Kafka at regular intervals (offline or via API). |
````
---

## ⚙️ Environment Variables (`.env`)

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=covid_events

# Producer settings
PRODUCER_OFFLINE=true               # true = generate mock data; false = call live API
PRODUCER_INTERVAL_SECONDS=15
PRODUCER_MODE=summary               # summary | historical
PRODUCER_COUNTRY_FILTER=            # e.g. IN,US

# Producer performance tuning
PRODUCER_COMPRESSION=lz4
PRODUCER_LINGER_MS=50
PRODUCER_RETRIES=3

# API endpoints
PRODUCER_SUMMARY_URL=https://disease.sh/v3/covid-19/countries
PRODUCER_HISTORICAL_URL=https://disease.sh/v3/covid-19/historical?lastdays=all

# Spark stream settings
SPARK_DEBUG_CONSOLE=false           # true = print rows in Spark logs
SPARK_TRIGGER_SEC=20                # micro-batch interval in seconds
````

---

## 🏗️ Setup & Run

### 1️⃣ Build and start everything

```bash
docker compose up -d --build --wait
```

This spins up:

* Zookeeper & Kafka
* Kafka-init (creates `covid_events`)
* Spark master + worker
* Spark streaming job
* Producer loop

To see live logs:

```bash
docker compose logs -f producer
docker compose logs -f spark-streaming
```

---

### 2️⃣ Verify Kafka health & topics

```bash
docker exec -it kafka bash -lc '/usr/bin/kafka-topics --bootstrap-server localhost:9092 --list'
```

Expected output:

```
__consumer_offsets
covid_events
```

To inspect topic details:

```bash
docker exec -it kafka bash -lc '/usr/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic covid_events'
```

---

### 3️⃣ Check produced messages

```bash
docker exec -it kafka bash -lc '/usr/bin/kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic covid_events \
  --from-beginning \
  --max-messages 5'
```

Example message:

```json
{"country":"India","country_code":"IN","total_confirmed":100,"total_deaths":1,"_ingest_ts":"2025-11-11T10:00:00Z"}
```

---

### 4️⃣ Validate Spark output

Output directories inside `data/output/covid`:

```
data/output/covid/
├── parquet/
│   └── p_date=2025-11-11/
│       └── part-00000-...snappy.parquet
└── csv/
    └── p_date=2025-11-11/
        └── part-00000-...csv
```

View locally:

```bash
ls -R data/output/covid/
head -n 5 data/output/covid/csv/p_date=*/part-*.csv
```

---

## 🧠 Data Flow Summary

1. **Producer** generates or fetches country-wise COVID data (every 15 sec).
2. Each record is serialized to JSON and pushed to **Kafka topic** `covid_events`.
3. **Spark Structured Streaming** subscribes to that topic, parses JSON into a DataFrame.
4. Spark enriches timestamps and partitions by `p_date`.
5. Each batch writes:

   * compact **Parquet** file for analytics
   * compact **CSV** for quick inspection
6. Checkpointing guarantees fault-tolerant resume behavior.

---

## 🧩 Key Design Choices

| Feature                                           | Reason                                              |
| ------------------------------------------------- | --------------------------------------------------- |
| **Auto & explicit topic creation (`kafka-init`)** | Prevents “topic not found” race conditions.         |
| **`coalesce(1)` in Spark sink**                   | Avoids excessive small files in dev mode.           |
| **Offline mode**                                  | Enables local testing without external APIs.        |
| **Checkpointing**                                 | Supports restart & exactly-once delivery semantics. |
| **Dockerized Spark cluster**                      | Reproduces near-production distributed behavior.    |

---

## 🧰 Useful Commands

| Purpose                                  | Command                                  |
| ---------------------------------------- | ---------------------------------------- |
| View all container statuses              | `docker compose ps`                      |
| Follow logs from producer                | `docker compose logs -f producer`        |
| Follow logs from Spark stream            | `docker compose logs -f spark-streaming` |
| Restart only producer                    | `docker compose restart producer`        |
| Clean everything (⚠️ removes Kafka data) | `docker compose down -v`                 |

---

## 🔍 Troubleshooting

| Symptom                                   | Fix                                                                      |
| ----------------------------------------- | ------------------------------------------------------------------------ |
| **Kafka fails with “Invalid cluster.id”** | Run `docker volume rm covid-pipeline-kafka-data` and restart.            |
| **No CSV/Parquet files**                  | Check Spark logs; ensure producer is sending messages & topic not empty. |
| **Producer timeout**                      | Verify Kafka health (`docker exec -it kafka kafka-topics ... --list`).   |
| **Zookeeper unhealthy**                   | Restart Compose; ensure port 2181 not blocked.                           |

---

## 📊 Example Output (offline mode)

`producer` log:

```
✅ Published 2 messages to covid_events (mode=summary)
```

`spark-streaming` log (if `SPARK_DEBUG_CONSOLE=true`):

```
+------+-------------+-------------------+-------------------+---------+
|country|country_code|event_ts           |ingest_ts          |p_date   |
+------+-------------+-------------------+-------------------+---------+
|India |IN           |2025-11-11 10:00:00|2025-11-11 10:00:03|2025-11-11|
+------+-------------+-------------------+-------------------+---------+
```

---

## 🧹 Cleanup

```bash
docker compose down -v
```

This stops containers and removes volumes (including Kafka data).

---

## 🏁 Summary

This stack demonstrates a real-world mini data-engineering workflow:

* **Ingestion** (Kafka producer)
* **Streaming transport** (Kafka)
* **Processing + Enrichment** (Spark)
* **Persistence** (Parquet + CSV)
* **Orchestration** (Docker Compose)

It’s portable, reproducible, and ideal for interviews, workshops, or local prototyping.

---

🧩 **Author:** Sandeep Mohanty
📅 **Last updated:** November 2025

