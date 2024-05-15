import paho.mqtt.client as mqtt
import json
from crate import client as clientDB
from datetime import datetime
import pytz  

def on_connect(client, userdata, flags, rc, properties):
    print("Conectado con mqtt " + str(rc))
    client.subscribe("sensor-topic")

def on_message(client, userdata, msg):
    print("Recibido: " + msg.topic + " " + str(msg.payload))
    try:
        m_decode = str(msg.payload.decode("utf-8", "ignore"))
        print("data Received", m_decode)
        print("Converting from Json to Object")
        m_in = json.loads(m_decode)  
        print(m_in)
        insert_into_cratedb(m_in)
    except KeyError as e:
        print("Error con el JSON: {e}")
    

# Función para insertar datos en CrateDB
def insert_into_cratedb(data):
    with clientDB.connect("localhost:4200") as connection:
        cursor = connection.cursor()
        now_utc = datetime.utcnow()
        tz = pytz.timezone('Europe/Madrid')
        now_with_tz = now_utc.replace(tzinfo=pytz.utc).astimezone(tz)
        fecha = now_with_tz.isoformat()
        query = "INSERT INTO ra_table (id_nodo, fecha, temperatura, humedad, co2, volatiles) VALUES (?, ?, ?, ?, ?, ?)"
        params = (data["id_nodo"], fecha, data["temperatura"], data["humedad"], data["co2"], data["volatiles"])
        cursor.execute(query, params)
        print('Datos enviados a la DB')

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)  # Specify MQTT version (e.g., MQTTv311)

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)

client.loop_forever()
