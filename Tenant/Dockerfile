# Usamos una imagen ligera de Python
FROM python:3.11-slim

# Evitamos que Python genere archivos .pyc y buffer de salida
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instalamos las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código
COPY . .

# Exponemos el puerto para la API (opcional, por si quieres ver estado)
EXPOSE 8000

# Ejecutamos el script principal
CMD ["python", "app/main.py"]