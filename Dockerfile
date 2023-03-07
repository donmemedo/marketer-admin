FROM registry.tech1a.co:81/repository/tech1a-docker-registry/python/python:3.9

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

RUN ls

ENV PYTHONPATH="$PYTHONPATH:/app"

EXPOSE 8000

CMD uvicorn src.main:app --host 0.0.0.0 --port 8000
