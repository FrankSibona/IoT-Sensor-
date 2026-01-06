import asyncio
import json
import logging
import threading
import time
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
import uvicorn
from fastapi import FastAPI
from sqlalchemy.orm import Session

# Importamos tus módulos existentes
from . import crud, models, web
from .db import SessionLocal, init_db
from .settings import settings

# Configuración de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("IOT_SERVER")

# --- LÓGICA MQTT (Ejecuta en segundo plano) ---
def process_mqtt_message(payload: str, topic: str):
    """Procesa el mensaje recibido y lo guarda en la DB usando SQLAlchemy"""
    try:
        data = json.loads(payload)
        
        # 1. Identificar Device ID
        device_id = data.get("device_id")
        if not device_id:
            # Intento sacar ID del topic: plant/CLIENTE/DEVICE_ID
            parts = topic.split("/")
            if len(parts) >= 3:
                device_id = parts[-1]
        
        if not device_id:
            logger.warning(f"⚠️ Mensaje sin device_id en {topic}")
            return

        # 2. Conexión a DB
        db: Session = SessionLocal()
        try:
            # 3. Validar Dispositivo
            device = crud.get_device(db, device_id)
            if not device:
                logger.warning(f"🚫 Dispositivo desconocido: {device_id}")
                return

            # 4. Actualizar "Última vez visto" y Status
            crud.touch_device_seen(db, device_id)

            # 5. Guardar en tabla flexible 'Measurement' (Para Grafana)
            # Esto usa la clase que agregamos a models.py
            meas = models.Measurement(device_id=device_id, data=data)
            db.add(meas)
            
            # 6. Lógica de ALERTAS (Tu código personalizado)
            check_and_create_alerts(db, device_id, data)

            db.commit()
            logger.info(f"💾 Datos guardados para {device_id}")

        except Exception as e:
            logger.error(f"Error guardando datos: {e}")
            db.rollback()
        finally:
            db.close()

    except json.JSONDecodeError:
        logger.error("JSON inválido recibido")

def check_and_create_alerts(db: Session, device_id: str, data: dict):
    """Analiza los datos y crea alertas en la tabla 'alarms'"""
    
    # Ejemplo: Temperatura Alta
    temp = data.get("temperatura_feed") or data.get("temp")
    if temp and isinstance(temp, (int, float)) and temp > 45:
        crud.create_alarm_simple(
            db, device_id, "TEMP_HIGH", "WARN", 
            f"Temperatura de entrada alta: {temp}°C"
        )
        logger.warning(f"🔥 Alerta de Temperatura para {device_id}")

    # Ejemplo: Presión Baja (si la bomba está activa)
    pres = data.get("pressure_feed")
    pump = data.get("pump_high")
    if pump == 1 and pres and pres < 1.0:
        crud.create_alarm_simple(
            db, device_id, "PRESS_LOW", "CRIT", 
            f"Presión baja ({pres} bar) con bomba encendida"
        )

def mqtt_loop():
    """Bucle infinito que corre en un hilo separado"""
    client = mqtt.Client()
    
    def on_connect(c, userdata, flags, rc):
        if rc == 0:
            logger.info(f"📡 MQTT Conectado a {settings.MQTT_HOST}")
            # Suscribirse a todo bajo el topic root
            c.subscribe(f"{settings.TOPIC_ROOT}/#")
        else:
            logger.error(f"Fallo conexión MQTT, código: {rc}")

    def on_message(c, userdata, msg):
        process_mqtt_message(msg.payload.decode(), msg.topic)

    client.on_connect = on_connect
    client.on_message = on_message

    # Reintentos de conexión
    while True:
        try:
            logger.info("Intentando conectar al Broker...")
            client.connect(settings.MQTT_HOST, settings.MQTT_PORT, 60)
            client.loop_forever()
        except Exception as e:
            logger.error(f"Error MQTT: {e}. Reintentando en 5s...")
            time.sleep(5)

# --- ARRANQUE WEB (FastAPI) ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Inicio: Crear tablas DB
    logger.info("🛠️ Inicializando Base de Datos...")
    init_db()
    
    # 2. Inicio: Arrancar MQTT en segundo plano
    logger.info("🚀 Arrancando hilo MQTT...")
    mqtt_thread = threading.Thread(target=mqtt_loop, daemon=True)
    mqtt_thread.start()
    
    yield
    # (Aquí iría código de limpieza al apagar si fuera necesario)

app = FastAPI(title="IOT Platform", lifespan=lifespan)

# Incluir las rutas de tu web.py
app.include_router(web.router)

if __name__ == "__main__":
    # Arranca el servidor web en el puerto 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)