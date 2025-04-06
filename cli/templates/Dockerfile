FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN --mount=type=ssh pip install git+ssh://git@gitlab.com/VivumComputing/scientific/robotics/dnfs/evolution/squid.git#egg=squid&subdirectory=worker/python

COPY simulation/ simulation/

ENV SQUID_BROKER_WK_SOCK_URL='tcp://172.17.0.1:5557'
ENV PYTHONPATH='/app'

ENTRYPOINT [ "python", "simulation/main.py" ]