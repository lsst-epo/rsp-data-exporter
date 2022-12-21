# FROM lsstsqre/centos:7-stack-lsst_distrib-d_2021_11_13 as lsststack
FROM gcr.io/google_appengine/python

USER root
RUN useradd -ms /bin/bash lsst
# USER lsst


# Google Cloud's App Engine platform does not offer Python 3.8+ so grab v3.9 and build it
RUN apt-get update && apt-get install -y software-properties-common build-essential zlib1g-dev libncurses5-dev libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev libsqlite3-dev wget libbz2-dev vim
WORKDIR /tmp
RUN wget https://www.python.org/ftp/python/3.9.1/Python-3.9.1.tgz
RUN tar -xf Python-3.9.1.tgz
WORKDIR /tmp/Python-3.9.1

RUN ./configure --enable-optimizations
RUN make -j 12
RUN make altinstall
RUN python3.9 --version
# USER lsst
RUN update-alternatives --install /usr/bin/python python /usr/local/bin/python3.9 1
RUN python --version

RUN mkdir -p /opt/lsst/software/server
WORKDIR /opt/lsst/software/server
COPY . .

# Set up the DB auth, configs, and lsst user
USER lsst
RUN mkdir /home/lsst/.lsst/
COPY ./db-auth.yaml /home/lsst/.lsst/db-auth.yaml

WORKDIR /home/lsst/.lsst/
RUN ls -l
RUN cat ./db-auth.yaml
USER root
RUN chmod 755 /home/lsst/.lsst/
RUN chmod -R 600 /home/lsst/.lsst/db-auth.yaml

# USER lsst
RUN chown -R lsst:lsst /home/lsst/.lsst

# # Install production dependencies.
# RUN pip3 install --upgrade pip setuptools wheel pep517
WORKDIR /opt/lsst/software/server
RUN pip3 install --upgrade pip
RUN python3.9 -m venv /tmp/venv
# RUN source /tmp/venv/bin/activate
RUN python3.9 -m pip install --no-cache-dir -r requirements.txt
# RUN python3.9 -m pip install lsst-daf-butler

WORKDIR /opt/lsst/software/server/src/rsp_data_exporter

USER lsst

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
 