FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py digitel_sqlite.py gnb_sqlite.py cicpc_sqlite.py import_cicpc_sqlite.py pnb_sqlite.py import_pnb_sqlite.py .

EXPOSE 8080

CMD ["python", "bot.py"]
