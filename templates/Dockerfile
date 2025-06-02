FROM python:3.12

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
ENV GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=no"
RUN --mount=type=ssh pip install git+ssh://git@gitlab.com/VivumComputing/scientific/tools/pysquid.git

COPY simulation/ simulation/

ENV PYTHONPATH='/app'

ENTRYPOINT [ "python", "simulation/main.py" ]
