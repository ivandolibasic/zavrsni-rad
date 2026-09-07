#!/usr/bin/env python3

import json
import random
import sys
import time

import paho.mqtt.client as mqtt


BROKER = "192.168.50.10"
RATE = float(sys.argv[1]) if len(sys.argv) > 1 else 20.0
SENSORS = 4

paused = False

client = mqtt.Client(client_id="simulator")
client.will_set("iot/status", "offline", retain=True)


def set_status():
    client.publish(
        "iot/status",
        "paused" if paused else "running",
        retain=True
    )


def on_connect(client, userdata, flags, rc):
    client.subscribe("iot/control")
    set_status()
    client.publish("edge/log", "[simulator] spojen")


def on_message(client, userdata, msg):
    global paused

    command = msg.payload.decode().strip().lower()

    if command == "pause":
        paused = True
        set_status()
        client.publish("edge/log", "[simulator] pauziran")

    elif command == "resume":
        paused = False
        set_status()
        client.publish("edge/log", "[simulator] nastavljen")


client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, 1883)
client.loop_start()

interval = 1 / RATE
next_send = time.perf_counter()

print(f"Simulator: {RATE} poruka/s")


try:
    n = 0

    while True:
        if paused:
            time.sleep(0.05)
            next_send = time.perf_counter()
            continue

        now = time.perf_counter()

        if now < next_send:
            time.sleep(next_send - now)

        n += 1

        data = {
            "sensor": f"sensor-{(n - 1) % SENSORS + 1}",
            "temperature": round(random.uniform(20, 28), 2),
            "humidity": round(random.uniform(40, 60), 2),
            "ts": time.time(),
        }

        client.publish("iot/raw", json.dumps(data))

        next_send += interval

        if next_send < time.perf_counter():
            next_send = time.perf_counter()

except KeyboardInterrupt:
    client.publish("iot/status", "offline", retain=True)
    client.publish("edge/log", "[simulator] zaustavljen")
    client.loop_stop()
    client.disconnect()
