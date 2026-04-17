FROM python:3.11-slim

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
