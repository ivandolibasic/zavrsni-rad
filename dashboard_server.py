#!/usr/bin/env python3

import asyncio
import csv
import json
import os
import time

import paho.mqtt.client as mqtt
import websockets


BROKER = "192.168.50.10"
PORT = 1883

CSV_FILE = "metrics.csv"

iot = []
cluster = {}
logs = []
simulator_status = "unknown"


def save_row(row):
    new_file = not os.path.exists(CSV_FILE)

    fields = [
        "vrijeme_s",
        "izvor",
        "cvor",
        "ciljana_stopa_poruka_s",
        "stvarna_stopa_poruka_s",
        "cpu_postotak",
        "ram_postotak",
        "latencija_ms",
        "propusnost_poruka_s"
    ]

    with open(CSV_FILE, "a", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fields
        )

        if new_file:
            writer.writeheader()

        writer.writerow(row)


def on_connect(client, userdata, flags, rc):
    client.subscribe("edge/result")
    client.subscribe("edge/metrics/+")
    client.subscribe("edge/log")
    client.subscribe("iot/status")
    client.subscribe("iot/generator_metrics")


def on_message(client, userdata, msg):
    global simulator_status

    if msg.topic == "edge/result":

        try:
            data = json.loads(msg.payload.decode())
        except json.JSONDecodeError:
            return

        iot.append(data)

        # Zadnjih 120 rezultata za prikaz na nadzornoj ploči.
        del iot[:-120]


    elif msg.topic.startswith("edge/metrics/"):

        try:
            data = json.loads(msg.payload.decode())
        except json.JSONDecodeError:
            return

        node = msg.topic.split("/")[-1]

        cluster[node] = data

        save_row({
            "vrijeme_s": time.time(),
            "izvor": "radni_cvor",
            "cvor": node,
            "ciljana_stopa_poruka_s": "",
            "stvarna_stopa_poruka_s": "",
            "cpu_postotak": data["cpu_percent"],
            "ram_postotak": data["ram_percent"],
            "latencija_ms": data["latency_ms"],
            "propusnost_poruka_s": data["throughput_msg_s"]
        })


    elif msg.topic == "iot/generator_metrics":

        try:
            data = json.loads(msg.payload.decode())
        except json.JSONDecodeError:
            return

        save_row({
            "vrijeme_s": time.time(),
            "izvor": "simulator",
            "cvor": "",
            "ciljana_stopa_poruka_s":
                data["target_rate_msg_s"],
            "stvarna_stopa_poruka_s":
                data["actual_rate_msg_s"],
            "cpu_postotak": "",
            "ram_postotak": "",
            "latencija_ms": "",
            "propusnost_poruka_s": ""
        })


    elif msg.topic == "edge/log":

        logs.append(
            msg.payload.decode()
        )

        del logs[:-50]


    elif msg.topic == "iot/status":

        simulator_status = (
            msg.payload.decode()
        )


mqtt_client = mqtt.Client(
    client_id="dashboard"
)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(
    BROKER,
    PORT
)

mqtt_client.loop_start()


async def browser(ws):

    try:

        while True:

            try:

                message = await asyncio.wait_for(
                    ws.recv(),
                    timeout=1
                )

                command = json.loads(
                    message
                ).get("command")

                if command in (
                    "pause",
                    "resume"
                ):

                    mqtt_client.publish(
                        "iot/control",
                        command
                    )

            except asyncio.TimeoutError:
                pass


            await ws.send(
                json.dumps({
                    "iot": iot,
                    "cluster": cluster,
                    "logs": logs,
                    "simulator_status":
                        simulator_status
                })
            )


    except websockets.exceptions.ConnectionClosed:
        pass


async def main():

    print(
        "Nadzorna ploča: "
        "ws://0.0.0.0:8765"
    )

    async with websockets.serve(
        browser,
        "0.0.0.0",
        8765
    ):
        await asyncio.Future()


try:
    asyncio.run(main())

except KeyboardInterrupt:
    pass

finally:
    mqtt_client.loop_stop()
    mqtt_client.disconnect()
