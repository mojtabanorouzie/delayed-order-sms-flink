FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/
COPY scenarios/ scenarios/

ENV PYTHONPATH=/app/src

ENTRYPOINT ["python", "-m", "order_simulator.main"]
