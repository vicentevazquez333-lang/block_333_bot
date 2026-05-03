FROM python:3.11-slim

# Instalar Tesseract OCR y dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr \
    tesseract-ocr-spa \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Copiar e instalar dependencias primero (mejor caché de Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del bot
COPY bot.py .

# Puerto que expone el servidor de webhooks
EXPOSE 8080

# Comando de inicio
CMD ["python", "bot.py"]
