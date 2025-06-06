from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt
import json
import threading
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
from fastapi import Query
from datetime import datetime

# 🔐 Datos de tu cuenta InfluxDB Cloud
INFLUX_URL = "https://us-east-1-1.aws.cloud2.influxdata.com"
INFLUX_TOKEN = "o_WFeAGE0ekUqHp91shE0EyT6_BihlPkYyyQkOBjU9jdkYREUtTgLHVd3RmYXWVlQDdNEw0ve3BVLrk0DHElzQ=="
INFLUX_ORG = "Student"
INFLUX_BUCKET = "finalproject"

client_influx = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)
query_api = client_influx.query_api()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BROKER = "broker.hivemq.com"
TOPICOS = {
    "bruto": "mantenimiento/vibraciones4",
    "fft": "mantenimiento/fft",
    "estadisticas": "mantenimiento/estadisticas",
    "modelo": "mantenimiento/mlmodel",
    "alarmas": "mantenimiento/alarmas"
}

# Variables para cada tópico
bruto_data = {}
fft_data = {}
estadisticas_data = {}
modelo_data = {}
alarmas_data = {}

# Callback para actualizar los datos
def on_message(client, userdata, msg):
    global bruto_data, fft_data, estadisticas_data, modelo_data, alarmas_data
    try:
        payload = json.loads(msg.payload.decode())
        if msg.topic == TOPICOS["bruto"]:
            bruto_data = payload
        elif msg.topic == TOPICOS["fft"]:
            fft_data = payload
        elif msg.topic == TOPICOS["estadisticas"]:
            estadisticas_data = payload
        elif msg.topic == TOPICOS["modelo"]:
            modelo_data = payload
        elif msg.topic == TOPICOS["alarmas"]:
            alarmas_data = payload
    except Exception as e:
        print(f"❌ Error al procesar mensaje en {msg.topic}:", e)

# Suscripción MQTT
def iniciar_mqtt():
    cliente = mqtt.Client()
    cliente.on_message = on_message
    cliente.connect(BROKER, 1883)
    for topico in TOPICOS.values():
        cliente.subscribe(topico)
    cliente.loop_forever()

threading.Thread(target=iniciar_mqtt, daemon=True).start()

# Rutas independientes
@app.get("/datos")
def get_bruto():
    return bruto_data

@app.get("/fft")
def get_fft():
    return fft_data

@app.get("/estadisticas")
def get_estadisticas():
    return estadisticas_data

@app.get("/modelo")
def get_modelo():
    return modelo_data

@app.get("/alarmas")
def get_alarmas():
    return alarmas_data


@app.get("/historico")
def obtener_historico_completo():
    rango = "-4m"

    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {rango})
      |> filter(fn: (r) => r["_measurement"] == "estadisticas")
      |> keep(columns: ["_time", "_field", "_value"])
    '''

    resultado = query_api.query(org=INFLUX_ORG, query=query)

    datos = {}

    for tabla in resultado:
        for r in tabla.records:
            variable = r.get_field()
            timestamp = r.get_time().isoformat()
            valor = r.get_value()

            if variable not in datos:
                datos[variable] = []
            datos[variable].append({
                "timestamp": timestamp,
                "valor": valor
            })

    return datos
   
