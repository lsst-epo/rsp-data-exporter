FROM python:3.11

RUN mkdir -p /app
WORKDIR /app
COPY . .

ENV PORT=8080
# RUN apt-get update && apt-get -qq install vim # For debug only

RUN pip3 install --upgrade pip setuptools
RUN python3 -m pip install --no-cache-dir -r requirements.txt
RUN python3 --version
WORKDIR /app/src/rsp_data_exporter

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
 