import os
import json
import time
import logging
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import RealDictCursor

# --- CONFIGURACIÓN ---
# Leemos las variables de entorno (definidas en docker-compose)
DB_HOST = os.getenv("DB_HOST", "ro-db")
DB_NAME = os.getenv("DB_NAME", "iot_db")
DB_USER = os.getenv("DB_USER", "usuario_iot")
DB_PASS = os.getenv("DB_PASS", "password_segura")
MQTT_HOST = os.getenv("MQTT_HOST", "172.17.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
TOPIC_ROOT = "plant/#"

# Configuración de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONEXIÓN BASE DE DATOS ---
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        logger.error(f"Error conectando a la DB: {e}")
        return None

# --- LÓGICA DE ALERTAS ---
def check_alerts(device_id, data, cursor):
    """
    Analiza los datos y genera alertas si se superan los umbrales.
    Aquí puedes agregar toda la lógica personalizada que quieras.
    """
    alerts = []
    
    # Ejemplo 1: Temperatura muy alta
    if "temperatura" in data and data["temperatura"] > 80:
        alerts.append(("ALTA_TEMPERATURA", f"Temperatura crítica detectada: {data['temperatura']}°C"))

    # Ejemplo 2: Presión baja (si la bomba está encendida)
    if "presion" in data and data["presion"] < 1.0:
        alerts.append(("BAJA_PRESION", f"Presión peligrosamente baja: {data['presion']} Bar"))

    # Insertar alertas en la DB
    if alerts:
        for alert_type, message in alerts:
            query = """
            INSERT INTO alerts (device_id, alert_type, message)
            VALUES (%s, %s, %s)
            """
            cursor.execute(query, (device_id, alert_type, message))
            logger.warning(f"⚠️ ALERTA GENERADA para {device_id}: {message}")

# --- PROCESAMIENTO DE MENSAJES MQTT ---
def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        logger.info(f"📩 Mensaje recibido en {msg.topic}")
        
        # 1. Intentar leer el JSON
        data = json.loads(payload)
        
        # Asumimos que el JSON trae un campo "device_id" o lo sacamos del topic
        # Opción A: Viene en el JSON
        device_id = data.get("device_id")
        
        # Opción B: Si no viene en el JSON, intenta sacarlo del topic (ej: plant/ESP-01/data)
        if not device_id:
            parts = msg.topic.split("/")
            if len(parts) > 1:
                device_id = parts[1] # Asume que el segundo elemento es el ID
        
        if not device_id:
            logger.warning("❌ Mensaje descartado: No se encontró device_id")
            return

        conn = get_db_connection()
        if not conn:
            return

        cur = conn.cursor()

        # 2. VALIDACIÓN: ¿Existe este equipo en nuestra empresa?
        cur.execute("SELECT tenant_id FROM devices WHERE device_id = %s", (device_id,))
        device = cur.fetchone()

        if device:
            # ¡El equipo es válido! Procedemos.
            logger.info(f"✅ Equipo Verificado: {device_id}")

            # 3. Guardar Mediciones
            insert_query = """
            INSERT INTO measurements (device_id, data)
            VALUES (%s, %s)
            """
            # Guardamos el JSON completo en la columna 'data'
            cur.execute(insert_query, (device_id, json.dumps(data)))

            # 4. Chequear Alertas
            check_alerts(device_id, data, cur)

            conn.commit()
            logger.info(f"💾 Datos guardados correctamente para {device_id}")

        else:
            logger.warning(f"🚫 Acceso denegado: El equipo {device_id} no está registrado en el sistema.")
            # Aquí podrías guardar un log de intentos fallidos si quisieras

        cur.close()
        conn.close()

    except json.JSONDecodeError:
        logger.error("Error al decodificar JSON")
    except Exception as e:
        logger.error(f"Error procesando mensaje: {e}")

# --- ARRANQUE MQTT ---
def start_mqtt():
    client = mqtt.Client()
    client.on_message = on_message

    logger.info(f"Conectando al Broker MQTT en {MQTT_HOST}...")
    
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, 60)
            client.subscribe(TOPIC_ROOT)
            logger.info(f"📡 Conectado y escuchando en '{TOPIC_ROOT}'")
            client.loop_forever()
        except Exception as e:
            logger.error(f"Fallo conexión MQTT: {e}. Reintentando en 5s...")
            time.sleep(5)

if __name__ == "__main__":
    # Esperar unos segundos para asegurar que la DB ya arrancó (útil en Docker)
    time.sleep(5)
    start_mqtt()