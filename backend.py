from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import paho.mqtt.client as mqtt
import json
import threading

app = FastAPI()

# CORS para permitir acceso desde frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción: usar ["http://localhost:5500"] u origen específico
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variable para guardar el último dato recibido
ultimo_dato = {}

# Callback MQTT
def on_message(client, userdata, msg):
    global ultimo_dato
    try:
        data = json.loads(msg.payload.decode())
        ultimo_dato = data
    except Exception as e:
        print("❌ Error al procesar mensaje:", e)

# Hilo MQTT
def iniciar_mqtt():
    cliente = mqtt.Client()
    cliente.on_message = on_message
    cliente.connect("broker.hivemq.com", 1883)
    cliente.subscribe("mantenimiento/vibraciones4")
    cliente.loop_forever()

threading.Thread(target=iniciar_mqtt, daemon=True).start()

# Ruta para que el frontend obtenga los datos
@app.get("/datos")
def obtener_datos():
    return ultimo_dato
