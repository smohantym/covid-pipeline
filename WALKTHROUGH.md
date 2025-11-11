
# 1) `docker-compose.yml` — infra, wiring, and boot order

### zookeeper

```yaml
zookeeper:
  image: confluentinc/cp-zookeeper:7.9.3.arm64
  restart: unless-stopped
  environment:
    ZOOKEEPER_CLIENT_PORT: 2181     # clients (Kafka) connect here
    ZOOKEEPER_TICK_TIME: 2000       # ZK heartbeat baseline (ms)
  ports: ["2181:2181"]              # useful for host-side admin if ever needed
  healthcheck: ...                  # simple 'zk-shell ls /' readiness probe
  volumes:
    - zk-data:/var/lib/zookeeper/data
    - zk-txn-logs:/var/lib/zookeeper/log
```

* **Purpose:** Metadata store for Kafka in ZK mode (good for single-node dev).
* **Healthcheck:** prevents Kafka from starting before ZK is truly ready.
* **Volumes:** persist ZK’s data & logs between restarts.

### kafka

```yaml
kafka:
  image: confluentinc/cp-kafka:7.9.3.arm64
  restart: unless-stopped
  depends_on: { zookeeper: { condition: service_healthy } }
  ports: ["9092:9092"]
  environment:
    KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    KAFKA_BROKER_ID: 1
    # Single internal listener for other containers:
    KAFKA_LISTENERS: PLAINTEXT://:9092
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    # Topic defaults & broker housekeeping:
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    KAFKA_NUM_PARTITIONS: 3
    KAFKA_DEFAULT_REPLICATION_FACTOR: 1
    KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    KAFKA_LOG_DIRS: /var/lib/kafka/data
    KAFKA_HEAP_OPTS: "-Xms512m -Xmx512m"
  volumes:
    - kafka-data:/var/lib/kafka/data
  healthcheck: ...  # lists topics; if it works, broker is ready for clients
```

* **Listeners:** container-to-container only (`kafka:9092`). This avoids host/advertised confusion.
* **Auto-create topics:** convenient for dev; you also have `kafka-init` to deterministically create the topic.
* **Named volume:** `covid-pipeline-kafka-data` means stable persistence; easy to wipe if needed.

### kafka-init

```yaml
kafka-init:
  image: confluentinc/cp-kafka:7.9.3.arm64
  restart: on-failure
  depends_on: { kafka: { condition: service_healthy } }
  env_file: [.env]
  entrypoint: ["/bin/bash","-lc"]
  command: >
    set -euo pipefail;
    # wait until broker accepts admin RPCs
    for i in {1..60}; do
      if /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then break; fi
      sleep 2;
    done;
    # create topic with your desired shape (idempotent)
    /usr/bin/kafka-topics --bootstrap-server kafka:9092 \
      --create --if-not-exists \
      --topic "${KAFKA_TOPIC:-covid_events}" \
      --partitions "${KAFKA_NUM_PARTITIONS:-3}" \
      --replication-factor "${KAFKA_DEFAULT_REPLICATION_FACTOR:-1}";
    # show topics for visibility
    /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list
```

* **Why keep it** even with auto-create: ensures `covid_events` exists **before** apps start, with **expected partitions**.

### spark-master / spark-worker

* **Master** runs `spark://spark-master:7077` and a UI on `:8080`.
* **Worker** joins master (2 cores, 2GiB set via env).
* **Shared mount:** `./data/output → /opt/spark-output` is where Spark writes files.

### spark-streaming

```yaml
spark-streaming:
  depends_on:
    kafka: { condition: service_healthy }
    kafka-init: { condition: service_completed_successfully }
    spark-master: { condition: service_started }
  env_file: [.env]
  volumes:
    - ./spark:/opt/spark-app          # your app code
    - ./data/output:/opt/spark-output # writes land here
    - ./cache/ivy:/tmp/.ivy2          # cache Maven deps in container
  entrypoint: spark-submit ... /opt/spark-app/app.py
```

* **Picks up** Kafka & topic from `.env`.
* **Runs forever**: manages the structured streaming query.

### producer

```yaml
producer:
  depends_on:
    kafka: { condition: service_healthy }
    kafka-init: { condition: service_completed_successfully }
  env_file: [.env]
  environment: { PYTHONUNBUFFERED: "1" }
  command: ["python","-u","producer.py"]
```

* **Starts only after** topic exists.
* **Unbuffered logs** for live visibility.

---

# 2) `.env` — the levers you actually touch

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092   # matches Kafka's advertised listeners
KAFKA_TOPIC=covid_events

PRODUCER_OFFLINE=true                # synthetic data; flip to false for live API
PRODUCER_INTERVAL_SECONDS=15         # how often a publish batch runs
PRODUCER_MODE=summary                # or 'historical'
PRODUCER_COUNTRY_FILTER=             # e.g., IN,US,GB

PRODUCER_COMPRESSION=lz4
PRODUCER_LINGER_MS=50
PRODUCER_RETRIES=3

PRODUCER_SUMMARY_URL=...
PRODUCER_HISTORICAL_URL=...

SPARK_DEBUG_CONSOLE=false
SPARK_TRIGGER_SEC=20                 # micro-batch trigger
```

---

# 3) `producer/producer.py` — data generation & publishing

### Boot, env, graceful shutdown

```python
load_dotenv()
BS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "covid_events")
MODE = os.getenv("PRODUCER_MODE", "summary").lower()
OFFLINE = os.getenv("PRODUCER_OFFLINE","false").lower() in {"1","true","yes"}
INTERVAL = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "86400"))
...
signal.signal(signal.SIGTERM, _sigterm)  # graceful shutdown
signal.signal(signal.SIGINT, _sigterm)
```

* Picks up Kafka endpoint & behavior from `.env`.
* Handles SIGTERM/SIGINT so it can exit cleanly.

### Producer construction (Confluent client)

```python
def mk_producer():
    return Producer({
        "bootstrap.servers": BS,
        "client.id": "covid-producer",
        "compression.type": COMPRESSION,  # lz4 by default
        "linger.ms": LINGER_MS,           # micro-batching
        "retries": RETRIES,
        "message.timeout.ms": 15000,
        # "debug": "broker,topic,msg"     # enable if troubleshooting
    })
```

* **Compression** reduces network/write overhead (lz4 is fast).
* **linger** batches small sends without adding much latency.
* **retries / message.timeout** are conservative for dev.

### Data sources (offline vs live)

```python
def _offline_payload_summary():  # tiny IN/US fixture w/ 'today' and total fields
def _offline_payload_historical():  # one-day historical totals per country

def get_summary():
    return _offline_payload_summary() if OFFLINE else requests.get(SUMMARY_URL).json()

def get_hist():
    return _offline_payload_historical() if OFFLINE else requests.get(HIST_URL).json()
```

* **OFFLINE=true** → deterministic, fast smoke-tests.
* Flip to **false** for real data from `disease.sh`.

### Record shaping (keys & values)

```python
def iter_summary(items):
    now_ts = datetime.now(timezone.utc).isoformat()
    for c in items:
        cc = ((c.get("countryInfo") or {}).get("iso2") or "").upper()
        ...
        rec = {
          "_ingest_ts": now_ts,           # when we observed it
          "source_date": <from API>,      # event/updated time
          "country": ..., "country_code": cc, "slug": ...,
          "new_confirmed": ..., "total_confirmed": ...,
          "new_deaths": ..., "total_deaths": ...,
          "new_recovered": ..., "total_recovered": ...,
        }
        yield f"{cc}|{rec['_ingest_ts']}", rec  # key: drives partitioning
```

* **Key format:** `CC|timestamp` (or `CC|YYYY-MM-DD` in historical mode). That:

  * spreads load across partitions consistently,
  * makes country-level grouping trivial downstream.

```python
def iter_hist(items):
    # similar, but flattens timeline dates, normalizes "mm/dd/yy" → "YYYY-MM-DD"
```

### The publishing loop

```python
def run_once():
    items = get_hist() if MODE == "historical" else get_summary()
    it = iter_hist(items) if MODE == "historical" else iter_summary(items)
    p = mk_producer()
    count_ok = count_err = 0

    def dr(err, msg):
        nonlocal count_ok, count_err
        count_err += 1 if err else 1 and not err or 0
        if not err: count_ok += 1

    for k, v in it:
        p.produce(TOPIC, json.dumps(v), key=k, callback=dr)
    p.flush(15)  # wait up to 15s for all acks/callbacks
    print(f"STATS: delivered={count_ok} failed={count_err}")
```

* **produce(..., callback=dr):** enqueues; delivery report increments counters.
* **flush(15):** blocks until everything is acked or timed out; then logs stats.

```python
if __name__ == "__main__":
    while not _shutdown:
        run_once()
        sleep(INTERVAL)  # repeat every N seconds
```

---

# 4) `spark/app.py` — streaming read, parse, enrich, write to disk

### Session + knobs

```python
spark = (SparkSession.builder
  .appName("Covid19KafkaToParquet")
  .config("spark.sql.shuffle.partitions","1")  # fewer small files in dev
  .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

### The schema (matches producer’s record)

```python
schema = StructType([
  StructField("_ingest_ts", StringType()),
  StructField("source_date", StringType()),
  StructField("country", StringType()),
  StructField("country_code", StringType()),
  StructField("slug", StringType()),
  StructField("new_confirmed", LongType()),
  StructField("total_confirmed", LongType()),
  StructField("new_deaths", LongType()),
  StructField("total_deaths", LongType()),
  StructField("new_recovered", LongType()),
  StructField("total_recovered", LongType()),
])
```

### Kafka source → JSON parsing

```python
raw = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
  .option("subscribe", TOPIC)
  .option("startingOffsets", "earliest")   # read backlog on first run
  .option("failOnDataLoss", "false")
  .load())

parsed = raw.select(
  col("key").cast("string").alias("key"),
  from_json(col("value").cast("string"), schema).alias("v")
)

records = parsed.select("key", "v.*")
```

* `raw` contains Kafka columns (`key`, `value`, `offset`, `timestamp`, etc.)
* We cast `value` to string and decode JSON with your schema.

### Enrichment & partition column

```python
clean = (records
  .withColumn("ingest_ts", to_timestamp(col("_ingest_ts")))
  .withColumn("event_ts", to_timestamp(col("source_date")))
  .withColumn("processing_ts", current_timestamp())
  .withColumn("p_date", date_format(coalesce(col("ingest_ts"), col("processing_ts")),
                                    "yyyy-MM-dd")))
```

* `p_date` will **always** be non-null (falls back to `processing_ts`).

### Optional console sink (for smoke tests)

```python
if DEBUG_CONSOLE:
  (clean.writeStream.format("console")
   .option("truncate", False).option("numRows", 20)
   .trigger(processingTime=f"{TRIGGER_SEC} seconds")
   .outputMode("append").start())
```

### Disk sink (Parquet + CSV) with `foreachBatch`

```python
csv_cols = ["country","country_code","event_ts","ingest_ts",
            "new_confirmed","total_confirmed","new_deaths","total_deaths","p_date"]

def foreach_batch(df, batch_id: int):
    if df.rdd.isEmpty(): return
    subset = df.select(*csv_cols).cache()

    # Parquet (compact per-partition per-batch)
    (subset.coalesce(1).write.mode("append")
     .partitionBy("p_date").parquet(PARQUET_PATH))

    # CSV (compact per-partition per-batch)
    (subset.coalesce(1).write.mode("append")
     .option("header", True)
     .partitionBy("p_date").csv(CSV_PATH))

    subset.unpersist()

(clean.writeStream
  .foreachBatch(foreach_batch)
  .option("checkpointLocation", CHECKPOINT)
  .trigger(processingTime=f"{TRIGGER_SEC} seconds")
  .outputMode("append")
  .start()
  .awaitTermination())
```

* `foreachBatch` = **full control** over how you write (both formats, coalesce, partitioning).
* **Checkpointing** ensures exactly-once semantics for the sink and progress tracking.
* **Output layout** (example):

  ```
  data/output/covid/
    parquet/p_date=2025-11-11/part-*.snappy.parquet
    csv/p_date=2025-11-11/part-*.csv
  ```

---

# 5) End-to-end flow (quick mental model)

1. **kafka-init** guarantees `covid_events` exists (3 partitions).
2. **producer** (every 15s):

   * builds a batch of records (offline fixture or live API),
   * publishes to `covid_events` with keys like `IN|2025-11-11T10:00:00Z`,
   * prints delivered/failed counts.
3. **spark-streaming** (every 20s):

   * reads Kafka events since last checkpoint,
   * parses JSON, enriches timestamps, sets `p_date`,
   * writes 1 compact parquet + 1 compact csv per partition per batch.

---

# 6) What messages look like

**Producer → Kafka (value JSON):**

```json
{
  "_ingest_ts": "2025-11-11T10:05:33.123456+00:00",
  "source_date": "2025-11-11T10:05:30+00:00",
  "country": "India",
  "country_code": "IN",
  "slug": "india",
  "new_confirmed": 10,
  "total_confirmed": 100,
  "new_deaths": 0,
  "total_deaths": 1,
  "new_recovered": 5,
  "total_recovered": 90
}
```

**Key:** `"IN|2025-11-11T10:05:33.123456+00:00"`

**Spark output (CSV columns):**

```
country,country_code,event_ts,ingest_ts,new_confirmed,total_confirmed,new_deaths,total_deaths,p_date
India,IN,2025-11-11 10:05:30,2025-11-11 10:05:33,10,100,0,1,2025-11-11
```

---

# 7) Validate quickly

* **Topic exists**

  ```bash
  docker exec -it kafka bash -lc '/usr/bin/kafka-topics --bootstrap-server localhost:9092 --list'
  docker exec -it kafka bash -lc '/usr/bin/kafka-topics --bootstrap-server localhost:9092 --describe --topic covid_events'
  ```

* **See events**

  ```bash
  docker exec -it kafka bash -lc '/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic covid_events --from-beginning --max-messages 5'
  ```

* **Watch logs**

  ```bash
  docker compose logs -f producer
  docker compose logs -f spark-streaming
  ```

* **Files on disk**

  ```bash
  ls -R data/output/covid/{parquet,csv}
  head -n 5 data/output/covid/csv/p_date=*/part-*.csv
  ```