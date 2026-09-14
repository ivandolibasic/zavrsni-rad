#!/usr/bin/env python3

import json
import random
import sys
import time

import paho.mqtt.client as mqtt


BROKER = "192.168.50.10"
PORT = 1883

RATE = float(sys.argv[1]) if len(sys.argv) > 1 else 20.0
SENSORS = 4

TOPIC_RAW = "iot/raw"
TOPIC_CONTROL = "iot/control"
TOPIC_STATUS = "iot/status"
TOPIC_METRICS = "iot/generator_metrics"

METRIC_INTERVAL = 2.0


if RATE <= 0:
    print("RATE mora biti veći od 0.")
    sys.exit(1)


paused = False

sent_total = 0
sent_interval = 0

metric_start = time.perf_counter()
next_send = time.perf_counter()


client = mqtt.Client(client_id="simulator")

client.will_set(
    TOPIC_STATUS,
    "offline",
    retain=True
)


def publish_status():
    status = "paused" if paused else "running"

    client.publish(
        TOPIC_STATUS,
        status,
        retain=True
    )


def reset_metrics():
    global sent_total, sent_interval
    global metric_start

    sent_total = 0
    sent_interval = 0
    metric_start = time.perf_counter()


def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC_CONTROL)
    publish_status()


def on_message(client, userdata, msg):
    global paused
    global next_send
    global metric_start
    global sent_interval

    command = msg.payload.decode().strip()

    if command == "pause":
        paused = True
        publish_status()

    elif command == "resume":
        paused = False

        now = time.perf_counter()

        next_send = now
        metric_start = now
        sent_interval = 0

        publish_status()

    elif command == "reset":
        reset_metrics()


client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT)
client.loop_start()

interval = 1.0 / RATE

print(
    f"Simulator: cilj={RATE} poruka/s"
)


try:
    n = 0

    while True:

        if paused:
            time.sleep(0.05)
            continue

        now = time.perf_counter()

        if now < next_send:
            time.sleep(next_send - now)

        n += 1

        data = {
            "sensor": f"sensor-{(n - 1) % SENSORS + 1}",
            "temperature": round(
                random.uniform(20, 28),
                2
            ),
            "humidity": round(
                random.uniform(40, 60),
                2
            ),
            "ts": time.time()
        }

        result = client.publish(
            TOPIC_RAW,
            json.dumps(data)
        )

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            sent_total += 1
            sent_interval += 1

        next_send += interval

        now = time.perf_counter()

        if next_send < now:
            next_send = now

        elapsed = now - metric_start

        if elapsed >= METRIC_INTERVAL:

            actual_rate = (
                sent_interval / elapsed
            )

            metrics = {
                "target_rate_msg_s": RATE,
                "actual_rate_msg_s": round(
                    actual_rate,
                    2
                ),
                "sent_total": sent_total
            }

            client.publish(
                TOPIC_METRICS,
                json.dumps(metrics)
            )

            sent_interval = 0
            metric_start = now


except KeyboardInterrupt:
    pass


finally:
    client.publish(
        TOPIC_STATUS,
        "offline",
        retain=True
    )

    client.loop_stop()
    client.disconnect()
