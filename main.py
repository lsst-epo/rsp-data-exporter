import os, fnmatch
import subprocess
import csv
from flask import Flask, request, Response
from google.cloud import storage
import panoptes_client
import sqlalchemy
# import lsst.daf.butler as dafButler

app = Flask(__name__)

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

@app.route("/")
def download_rsp_data_and_upload():
    email = request.args.get('email')
    collection = request.args.get('collection')
    sourceId = request.args.get("sourceId")
    output = subprocess.run(['sh', '/opt/lsst/software/server/run.sh', email, collection], stdout=subprocess.PIPE).stdout.decode('utf-8')

    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_BUCKET)

    filepath = locate('*.fits', "/tmp/data/" + collection)

    # Create a new blob and upload the file's content.
    blob = bucket.blob(filepath[1])

    blob.upload_from_filename(str(filepath[0]))

    with open('/tmp/manifest.csv', 'w', newline='') as csvfile:
        fieldnames = ['email', 'sourceId', 'uri', 'externalId']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerow({'email': email, 'sourceId': sourceId, 'uri': blob.public_url, 'externalId': 12312312})
        
        # csvRes = csvfile

    manifestBlob = bucket.blob("manifest.csv")

    manifestUrl = manifestBlob.upload_from_filename("/tmp/manifest.csv")

    insertMetaRecord(blob.public_url, sourceId, 'sourceId')

    return manifestBlob.public_url

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

#
def locate(pattern, root_path):
    for path, dirs, files in os.walk(os.path.abspath(root_path)):
        for filename in fnmatch.filter(files, pattern):
            return [os.path.join(path, filename), filename ]

def init_connection_engine():
    db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 5,
        # Temporarily exceeds the set pool_size if no connections are available.
        "max_overflow": 2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # [END cloud_sql_postgres_sqlalchemy_limit]

        # [START cloud_sql_postgres_sqlalchemy_backoff]
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # [END cloud_sql_postgres_sqlalchemy_backoff]

        # [START cloud_sql_postgres_sqlalchemy_timeout]
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        "pool_timeout": 30,  # 30 seconds
        # [END cloud_sql_postgres_sqlalchemy_timeout]

        # [START cloud_sql_postgres_sqlalchemy_lifetime]
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # reestablished
        "pool_recycle": 1800,  # 30 minutes
        # [END cloud_sql_postgres_sqlalchemy_lifetime]
    }


    return init_tcp_connection_engine(db_config)

def init_tcp_connection_engine(db_config):
    pool = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME))
    pool.dialect.description_encoding = None
    return pool

def insertMetaRecord(uri, sourceId, sourceIdType):
    # do stuff
    global db
    db = init_connection_engine()
    edcVerId = 11000222
    public = True
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_meta (edc_ver_id, source_id, source_id_type, uri, public)"
        " VALUES (:edcVerIdSQL, :sourceIdSQL, :sourceIdTypeSQL, :uriSQL, :publicSQL)"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            conn.execute(stmt, edcVerIdSQL=edcVerId, sourceIdSQL=sourceId, sourceIdTypeSQL=sourceIdType, uriSQL=uri, publicSQL=public)

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        return Response(
            status=500,
            response="An error occurred while adding citizen science meta table."
        )
    return

