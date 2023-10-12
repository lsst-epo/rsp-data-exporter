FROM gcr.io/google_appengine/python

RUN mkdir -p /app
WORKDIR /app
COPY . .

RUN pip3 install --upgrade pip setuptools
RUN python3 -m pip install --no-cache-dir -r requirements.txt
WORKDIR /app/src/rsp_data_exporter

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
 