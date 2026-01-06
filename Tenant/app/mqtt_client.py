# app/mqtt_client.py  (COMPLETO - CON publish() CORRECTO)
import os
import json
import time
import threading
from typing import Dict, Any, Optional

import paho.mqtt.client as mqtt


class MqttPublisher:
    def __init__(self):
        # ===== Configuración desde variables de entorno =====
        self.host = os.getenv("MQTT_HOST", "127.0.0.1")
        self.port = int(os.getenv("MQTT_PORT", "1883"))

        # Usuario de la APP (no el del dispositivo)
        self.username = os.getenv("MQTT_ADMIN_USER", "")
        self.password = os.getenv("MQTT_ADMIN_PASS", "")

        # ===== Cliente MQTT =====
        self._client = mqtt.Client(
            client_id=f"app-pub-{os.getpid()}",
            clean_session=True,
        )

        if self.username:
            self._client.username_pw_set(self.username, self.password)

        # Flags de estado
        self._connected = threading.Event()

        # Callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

        # Conexión asincrónica (no bloquea FastAPI)
        self._client.connect_async(self.host, self.port, keepalive=30)
        self._client.loop_start()

        # Espera corta para levantar (evita publish sin conexión)
        self._connected.wait(timeout=5)

    # ================== Callbacks ==================

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected.set()
        else:
            print(f"[MQTT] Error de conexión, rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self._connected.clear()
        print("[MQTT] Desconectado")

    # ================== Publish ==================

    def publish_json(
        self,
        topic: str,
        payload: Dict[str, Any],
        qos: int = 1,
        retain: bool = False,
    ) -> bool:
        """
        Publica un payload JSON en el topic indicado.
        Retorna True si se envió correctamente.
        """
        if "ts" not in payload:
            payload["ts"] = int(time.time())

        data = json.dumps(payload, ensure_ascii=False)

        if not self._connected.is_set():
            print("[MQTT] No conectado, intentando publicar igual")

        info = self._client.publish(topic, data, qos=qos, retain=retain)
        info.wait_for_publish(timeout=3)
        return info.rc == mqtt.MQTT_ERR_SUCCESS

    def publish(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        """
        Publica string (por ejemplo JSON ya serializado).
        Devuelve MQTTMessageInfo.
        """
        if not self._connected.is_set():
            print("[MQTT] No conectado, intentando publicar igual")
        return self._client.publish(topic, payload, qos=qos, retain=retain)


# ================== Singleton ==================

_publisher: Optional[MqttPublisher] = None
_lock = threading.Lock()


def get_publisher() -> MqttPublisher:
    global _publisher
    with _lock:
        if _publisher is None:
            _publisher = MqttPublisher()
        return _publisher
