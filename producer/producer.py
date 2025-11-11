# producer/producer.py (Confluent client)
import json, os, time, sys, signal
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

print("BOOT: starting producer.py", flush=True)
load_dotenv()

BS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "covid_events")

MODE = os.getenv("PRODUCER_MODE", "summary").lower()
OFFLINE = os.getenv("PRODUCER_OFFLINE", "false").lower() in {"1", "true", "yes"}
INTERVAL = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "86400"))
SUMMARY_URL = os.getenv("PRODUCER_SUMMARY_URL", "https://disease.sh/v3/covid-19/countries")
HIST_URL = os.getenv("PRODUCER_HISTORICAL_URL", "https://disease.sh/v3/covid-19/historical?lastdays=all")
COUNTRY_FILTER = {c.strip().upper() for c in os.getenv("PRODUCER_COUNTRY_FILTER","").split(",") if c.strip()}

# optional tuning
COMPRESSION = os.getenv("PRODUCER_COMPRESSION", "lz4")
LINGER_MS = int(os.getenv("PRODUCER_LINGER_MS", "50"))
RETRIES = int(os.getenv("PRODUCER_RETRIES", "3"))

print(f"ENV: BS={BS} TOPIC={TOPIC} MODE={MODE} OFFLINE={OFFLINE} INTERVAL={INTERVAL}", flush=True)

_shutdown = False
def _sigterm(*_):
    global _shutdown
    _shutdown = True
signal.signal(signal.SIGTERM, _sigterm)
signal.signal(signal.SIGINT, _sigterm)

def mk_producer():
    print("STEP: creating Confluent Producer...", flush=True)
    p = Producer({
        "bootstrap.servers": BS,
        "client.id": "covid-producer",
        "compression.type": COMPRESSION,
        "linger.ms": LINGER_MS,
        "retries": RETRIES,
        "message.timeout.ms": 15000,
        # "debug": "broker,topic,msg",  # uncomment for deep troubleshooting
    })
    print("OK: Confluent Producer created", flush=True)
    return p

def _offline_payload_summary():
    nowms = int(datetime.now().timestamp()*1000)
    return [
        {"country":"India","countryInfo":{"iso2":"IN"},"updated":nowms,
         "todayCases":10,"cases":100,"todayDeaths":0,"deaths":1,"todayRecovered":5,"recovered":90},
        {"country":"United States","countryInfo":{"iso2":"US"},"updated":nowms,
         "todayCases":20,"cases":200,"todayDeaths":1,"deaths":5,"todayRecovered":10,"recovered":180},
    ]

def _offline_payload_historical():
    d = datetime.now(timezone.utc).date().isoformat()
    return [
        {"country":"India","countryInfo":{"iso2":"IN"},
         "timeline":{"cases":{d:100},"deaths":{d:1},"recovered":{d:90}}},
        {"country":"United States","countryInfo":{"iso2":"US"},
         "timeline":{"cases":{d:200},"deaths":{d:5},"recovered":{d:180}}},
    ]

def get_summary():
    if OFFLINE: return _offline_payload_summary()
    r = requests.get(SUMMARY_URL, timeout=30); r.raise_for_status(); return r.json()

def get_hist():
    if OFFLINE: return _offline_payload_historical()
    r = requests.get(HIST_URL, timeout=30); r.raise_for_status(); return r.json()

def iter_summary(items):
    now_ts = datetime.now(timezone.utc).isoformat()
    for c in items:
        cc = ((c.get("countryInfo") or {}).get("iso2") or "").upper()
        if COUNTRY_FILTER and cc not in COUNTRY_FILTER: continue
        rec = {
            "_ingest_ts": now_ts,
            "source_date": datetime.utcfromtimestamp(int(c.get("updated",0))/1000).replace(tzinfo=timezone.utc).isoformat() if c.get("updated") else now_ts,
            "country": c.get("country"), "country_code": cc, "slug": (c.get("country") or "").lower().replace(" ","-"),
            "new_confirmed": c.get("todayCases"), "total_confirmed": c.get("cases"),
            "new_deaths": c.get("todayDeaths"), "total_deaths": c.get("deaths"),
            "new_recovered": c.get("todayRecovered"), "total_recovered": c.get("recovered"),
        }
        yield f"{cc}|{rec['_ingest_ts']}", rec

def iter_hist(items):
    now_ts = datetime.now(timezone.utc).isoformat()
    for c in items:
        cc = ((c.get("countryInfo") or {}).get("iso2") or "").upper()
        if COUNTRY_FILTER and cc not in COUNTRY_FILTER: continue
        country = c.get("country"); slug = (country or "").lower().replace(" ","-")
        tl = c.get("timeline") or {}
        cases, deaths, recov = tl.get("cases") or {}, tl.get("deaths") or {}, tl.get("recovered") or {}
        for d_str in sorted(set(list(cases)+list(deaths)+list(recov))):
            try:
                m,d,y = d_str.split("/"); y=int(y); y += 2000 if y<100 else 0
                iso_date = f"{y:04d}-{int(m):02d}-{int(d):02d}"
            except Exception:
                iso_date = d_str
            yield f"{cc}|{iso_date}", {
                "_ingest_ts": now_ts, "source_date": iso_date,
                "country": country, "country_code": cc, "slug": slug,
                "new_confirmed": None, "total_confirmed": cases.get(d_str),
                "new_deaths": None, "total_deaths": deaths.get(d_str),
                "new_recovered": None, "total_recovered": recov.get(d_str),
            }

def run_once():
    items = get_hist() if MODE == "historical" else get_summary()
    it = iter_hist(items) if MODE == "historical" else iter_summary(items)
    p = mk_producer()
    count_ok = 0
    count_err = 0

    def dr(err, msg):
        nonlocal count_ok, count_err
        if err: count_err += 1
        else:   count_ok += 1

    for k, v in it:
        p.produce(TOPIC, json.dumps(v), key=k, callback=dr)
    p.flush(15)
    print(f"STATS: delivered={count_ok} failed={count_err}", flush=True)
    print(f"✅ Published {count_ok} messages to {TOPIC} (mode={MODE}) at {datetime.utcnow().isoformat()}Z", flush=True)

if __name__ == "__main__":
    print("MAIN: entering loop", flush=True)
    while not _shutdown:
        try:
            print("STEP: fetching data...", flush=True)
            run_once()
        except Exception as e:
            print("❌ LOOP ERROR:", repr(e), file=sys.stderr, flush=True)
        for _ in range(INTERVAL):
            if _shutdown: break
            time.sleep(1)
    print("EXIT: graceful shutdown", flush=True)
