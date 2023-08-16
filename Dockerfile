FROM registry.tech1a.co:81/repository/tech1a-docker-registry/python/python:3.10

ENV TZ=Asia/Tehran

COPY . /app

WORKDIR /app

RUN pip install -U pip
RUN pip install -r requirements.txt

RUN ls

ENV PYTHONPATH="$PYTHONPATH:/app"

WORKDIR /app/src

EXPOSE 8000

CMD uvicorn main:app --host 0.0.0.0 --port 8000
