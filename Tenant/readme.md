## RO Diagnostic Admin (FastAPI + HTML)

### 1) Instalar
python -m venv .venv
source .venv/bin/activate  (Linux)  /  .venv\Scripts\activate (Windows)
pip install -r requirements.txt

### 2) Configurar (opcional)
Export env vars:
- DB_URL=sqlite:///./app.db
- WEB_HOST=0.0.0.0
- WEB_PORT=8000
- MQTT_HOST=localhost
- MQTT_PORT=1883
- MQTT_USER=...
- MQTT_PASS=...
- MQTT_TLS=0
- TOPIC_ROOT=plant

### 3) Ejecutar
python -m app.main

### 4) Usar
- http://localhost:8000/tenants (crear empresa)
- http://localhost:8000/devices/new (alta equipo RO-2025-0001)
- http://localhost:8000/devices (lista)
- editar ficha desde el link "Editar ficha"
