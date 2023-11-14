FROM python:3.12

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

CMD ["python3","main.py"]