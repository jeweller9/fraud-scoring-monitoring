import os, threading, time, json, joblib, numpy as np
from confluent_kafka import Consumer, Producer
from fastapi import FastAPI
from prometheus_client import Counter, Histogram, Gauge
import uvicorn

KAFKA = os.environ["KAFKA_BOOTSTRAP"]
IN_TOPIC = os.environ.get("INPUT_TOPIC", "transactions")
OUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "scores")
MODEL_PATH = os.environ.get("MODEL_PATH", "/app/model.pkl")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))

tx_total = Counter("scoring_transactions_total", "Processed transactions")
proc_time = Histogram("scoring_processing_seconds", "Processing time per tx")
score_hist = Histogram(
    "scoring_score", "Distribution of scores", buckets=[i / 10 for i in range(11)]
)
fraud_total = Counter("scoring_fraud_total", "Transactions marked as fraud")
fraud_share = Gauge("scoring_fraud_share", "Share of fraud among processed")
consumer_lag = Gauge("scoring_kafka_consumer_lag_messages", "Kafka consumer lag")
model_reload = Counter("scoring_model_reload_total", "Model reload events", ["version"])
model_switch = Gauge(
    "scoring_model_switch_annotation",
    "Annotation 1 for 30s after model switch",
    ["version"],
)
model_info = Gauge(
    "scoring_model_info",
    "Current model version info",
    ["version"])


state = {"model": None, "version": "v0"}
_lock = threading.Lock()


def load_model():
    if not os.path.exists(MODEL_PATH):
        return
    m = joblib.load(MODEL_PATH)
    ver = f"v{int(time.time())}"
    with _lock:
        state["model"] = m
        state["version"] = ver
    model_info.clear()
    model_info.labels(version=ver).set(1)
    model_reload.labels(version=ver).inc()
    model_switch.labels(version=ver).set(1)

    def reset():
        time.sleep(30)
        model_switch.labels(version=ver).set(0)

    threading.Thread(target=reset, daemon=True).start()
    print("loaded model", ver, flush=True)


def watch_model():
    last = None
    while True:
        try:
            mtime = os.path.getmtime(MODEL_PATH) if os.path.exists(MODEL_PATH) else None
        except Exception:
            mtime = None
        if mtime and mtime != last:
            last = mtime
            load_model()
        time.sleep(2)


def consume_loop():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA,
            "group.id": "scoring",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([IN_TOPIC])
    producer = Producer({"bootstrap.servers": KAFKA})

    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        t0 = time.time()
        try:
            payload = json.loads(msg.value())
        except Exception:
            continue
        model = state["model"]
        if model is None:
            score, fraud = 0.0, False
        else:
            feats = np.array(
                [
                    [
                        payload.get("amount", 0.0),
                        payload.get("hour", 0),
                        payload.get("cat_id", 0),
                    ]
                ],
                dtype=float,
            )
            p = float(model.predict_proba(feats)[0][1])
            score, fraud = p, p > 0.5

        score_hist.observe(score)
        tx_total.inc()
        if fraud:
            fraud_total.inc()
        proc_time.observe(time.time() - t0)

        processed = tx_total._value.get()
        if processed > 0:
            fraud_share.set(fraud_total._value.get() / processed)

        try:
            consumer_lag.set(
                consumer.get_watermark_offsets(
                    consumer.position(consumer.assignment())[0], timeout=1
                )[1]
                - consumer.position(consumer.assignment())[0]
            )
        except Exception:
            pass

        out = dict(payload)
        out.update(
            {"score": score, "is_fraud": fraud, "model_version": state["version"]}
        )
        producer.produce(OUT_TOPIC, json.dumps(out).encode())


threading.Thread(target=consume_loop, daemon=True).start()
threading.Thread(target=watch_model, daemon=True).start()

app = FastAPI()


@app.get("/health")
def health():
    return {"ok": True, "model_version": state["version"]}


@app.get("/metrics")
def metrics():
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from fastapi import Response

    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=METRICS_PORT)
