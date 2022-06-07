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

# For local development:
# ENV PORT 8181
# ENV S3_ENDPOINT_URL https://storage.googleapis.com
# ENV AWS_ACCESS_KEY_ID GOOG1EGDWSUXUNX2RFSAGKQQEI7VOAGTY7D3LIXUZWHLKPGCZ4IPLHATUBV6Q
# ENV AWS_SECRET_ACCESS_KEY DXsZJYzOo6exunWgrngbS0J9pmBEZyeCJxNuWZU4
# ENV CLOUD_STORAGE_BUCKET butler-config
# ENV DB_USER astro_objects
# ENV DB_PASS champagne-supernova-2021
# ENV DB_NAME astro_artifacts
# ENV DB_HOST 10.109.176.5
# ENV DB_PORT 5432
# ENV CLOUD_STORAGE_BUCKET_HIPS2FITS citizen-science-data
# COPY ./key.json ~/key.json
# ENV GOOGLE_APPLICATION_CREDENTIALS ~/key.json
# End of local development

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
 