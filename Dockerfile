FROM lsstsqre/centos:7-stack-lsst_distrib-d_2021_11_13 as lsststack

USER root
RUN yum -y update
RUN yum -y install python3
RUN python3 -V
RUN yum -y install mlocate
RUN updatedb

RUN mkdir /opt/lsst/software/server
WORKDIR /opt/lsst/software/server
COPY . .

# # Install production dependencies.
RUN pip3 install --no-cache-dir -r requirements.txt

# Set up env vars for the Butler
ENV S3_ENDPOINT_URL <endpoint>
ENV AWS_ACCESS_KEY_ID <access key>
ENV AWS_SECRET_ACCESS_KEY <secret key>
ENV CLOUD_STORAGE_BUCKET <bucket>

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
 