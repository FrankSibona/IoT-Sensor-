# app/run_ingestor.py  (NUEVO ARCHIVO - PROCESO SEPARADO PARA MQTT)
import time
from .db import init_db
from .mqtt_ingestor import start_mqtt


def main():
    # asegura tablas (por si el ingestor arranca antes que la web)
    init_db()

    # arranca listener MQTT + watchdog OFFLINE + writer
    start_mqtt()

    # mantener vivo el proceso
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
