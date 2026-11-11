import os, json, threading, time
from confluent_kafka import Consumer
import psycopg2
from fastapi import FastAPI, Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import uvicorn

KAFKA = os.environ["KAFKA_BOOTSTRAP"]
IN_TOPIC = os.environ.get("INPUT_TOPIC", "scores")
PG_DSN = os.environ["PG_DSN"]
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8001"))

written_total = Counter("writer_scores_written_total", "Rows written")
write_time    = Histogram("writer_write_seconds", "DB write time")
fraud_total   = Counter("writer_fraud_total", "Fraud rows written")
score_hist    = Histogram("writer_score", "Written score distribution",
                          buckets=[i/10 for i in range(11)])

def init_db():
    while True:
        try:
            with psycopg2.connect(PG_DSN) as c, c.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS scored_transactions (
                  id BIGSERIAL PRIMARY KEY,
                  transaction_id TEXT,
                  score DOUBLE PRECISION,
                  is_fraud BOOLEAN,
                  model_version TEXT,
                  us_state TEXT,
                  merch TEXT,
                  cat_id INT,
                  amount DOUBLE PRECISION,
                  created_at TIMESTAMPTZ DEFAULT now()
                );
                """)
                c.commit()
            return
        except Exception as e:
            print("wait pg", e, flush=True)
            time.sleep(2)

def consume():
    init_db()
    cons = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": "writer",
        "auto.offset.reset": "latest",
    })
    cons.subscribe([IN_TOPIC])
    while True:
        msg = cons.poll(1.0)
        if msg is None or msg.error():
            continue
        t0 = time.time()
        try:
            p = json.loads(msg.value())
        except Exception:
            continue
        try:
            with psycopg2.connect(PG_DSN) as c, c.cursor() as cur:
                cur.execute("""
                  INSERT INTO scored_transactions
                  (transaction_id, score, is_fraud, model_version, us_state, merch, cat_id, amount)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (p.get("id"), p.get("score"), p.get("is_fraud"),
                      p.get("model_version"), p.get("us_state"),
                      p.get("merch"), p.get("cat_id"), p.get("amount")))
                c.commit()
            written_total.inc()
            if p.get("is_fraud"):
                fraud_total.inc()
            score_hist.observe(float(p.get("score") or 0))
            write_time.observe(time.time() - t0)
        except Exception as e:
            print("db err", e, flush=True)

threading.Thread(target=consume, daemon=True).start()

app = FastAPI()

@app.get("/health")
def health(): return {"ok": True}

@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=METRICS_PORT)
