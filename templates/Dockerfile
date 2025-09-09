FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install git+https://github.com/benbaarber/squid-worker.git

COPY simulation/ simulation/

ENV PYTHONPATH='/app'

ENTRYPOINT [ "python", "simulation/main.py" ]
