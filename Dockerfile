FROM lsstsqre/centos:7-stack-lsst_distrib-d_2021_11_13 as lsststack

USER root
RUN yum -y update
RUN yum -y install python3
RUN python3 -V
RUN yum -y install mlocate
RUN updatedb
RUN yum -y install glibc-static

RUN mkdir -p /opt/lsst/software/server
WORKDIR /opt/lsst/software/server
COPY . .

# # Install production dependencies.
# USER lsst
RUN pip3 install --upgrade pip setuptools wheel pep517
RUN pip3 install --no-cache-dir -r requirements.txt
# RUN chmod 777 -r /tmp/venv

# USER lsst
# RUN source /tmp/venv/bin/activate
# RUN pip install --upgrade 

# Set up env vars for the Butler
ENV S3_ENDPOINT_URL https://storage.googleapis.com
ENV AWS_ACCESS_KEY_ID GOOG1EGDWSUXUNX2RFSAGKQQEI7VOAGTY7D3LIXUZWHLKPGCZ4IPLHATUBV6Q
ENV AWS_SECRET_ACCESS_KEY DXsZJYzOo6exunWgrngbS0J9pmBEZyeCJxNuWZU4
ENV CLOUD_STORAGE_BUCKET butler-config

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
 