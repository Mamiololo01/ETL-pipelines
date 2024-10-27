FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]
