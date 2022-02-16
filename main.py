import os, fnmatch
import subprocess
import csv
from flask import Flask, request, Response
from google.cloud import storage
import panoptes_client
import sqlalchemy
from pprint import pprint
# import lsst.daf.butler as dafButler

app = Flask(__name__)

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
db = None

@app.route("/citizen-science-ingest")
def downloadRspDataAndUpload():
    global db
    db = init_connection_engine()
    email = request.args.get('email')
    collection = request.args.get('collection')
    sourceId = request.args.get("sourceId")
    vendorProjectId = request.args.get("vendorProjectId")
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
        # fieldnames = ['email', 'sourceId', 'location:1', 'externalId']
        fieldnames = ['email', 'location:1', 'externalId']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        # writer.writerow({'email': email, 'sourceId': sourceId, 'uri': blob.public_url, 'externalId': 12312312})
        writer.writerow({'email': email, 'location:1': 'https://storage.googleapis.com/butler-config/astrocat.jpeg', 'externalId': 12312312})
        
    manifestBlob = bucket.blob("manifest.csv")

    manifestBlob.upload_from_filename("/tmp/manifest.csv")

    # Beginning of database CRUD operations
    newOwner = False
    ownerId = lookupOwnerRecord(email)

    # First lookup owner
    if (isinstance(ownerId, int)):
        print("Found owner record!")
    else:
        newOwner = True
        ownerId = createNewOwnerRecord(email)

    # Then, lookup project
    if(newOwner == True):
        projectId = lookupProjectRecord(vendorProjectId)
    else:
        projectId = createNewProjectRecord(ownerId, vendorProjectId)

    # Then, lookup batch info
    batchId = checkBatchStatus(projectId)

    if(batchId < 0):
        # Do not allow for the creation of an existing batch if there are 
        return Response(
            status=500,
            response="You cannot send a new batch of data to your citizen science project because you already have an active, uncompleted batch of data in-progress."
        )
    else:
        # Create new batch record
        batchId = createNewBatch(projectId)

        if(batchId > 0):
            # Finally, insert meta records
            insertMetaRecord(blob.public_url, sourceId, 'sourceId', projectId)
            db.dispose()
            return manifestBlob.public_url
        else:
            db.dispose()
            return Response(
            status=500,
                response="You cannot send a new batch of data to your citizen science project because you already have an active, uncompleted batch of data in-progress."
            )
        
def createNewBatch(projectId):
    print("rosas - inside createNewBatch")
    global db
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_batches (cit_sci_proj_id, batch_status)"
        "VALUES (:projectId, 'ACTIVE')"
    )
    try:
        batchId = -1;
        with db.connect() as conn:
            for row in conn.execute(stmt, projectId=projectId):
                batchId = row['cit_sci_batch_id']
            conn.close()
    except Exception as e:
        print(e)
        return Response(
            status=500,
            response="An error occurred while creating a citizen_sciences_batches record."
        )
    return batchId

def checkBatchStatus(projectId):
    global db
    stmt = sqlalchemy.text(
        "SELECT * FROM citizen_science_batches WHERE cit_sci_proj_id = :projectId AND batch_status = 'ACTIVE'"
    )
    try:
        batchId = -1
        with db.connect() as conn:
            records = conn.execute(stmt, projectId=projectId)
            if(records.rowcount > 0):
                batchId = records.first()['cit_sci_batch_id']
            conn.close()
    except Exception as e:
        print(e)

    return batchId

def createNewProjectRecord(ownerId, vendorProjectId):
    global db
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_projects (vendor_project_id, owner_id, project_status)"
        " VALUES (:vendorProjectId, :ownerId, :projectStatus) RETURNING cit_sci_proj_id"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, vendorProjectId=vendorProjectId, ownerId=ownerId, projectStatus='active'):
                projectId = row['cit_sci_proj_id']
                conn.close()

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        return Response(
            status=500,
            response="An error occurred while creating citizen_science_projects record(s)."
        )
    # return projectId['cit_sci_proj_id']
    return projectId

def lookupProjectRecord(vendorProjectId):
    global db
    stmt = sqlalchemy.text(
        "SELECT cit_sci_proj_id FROM citizen_science_projects WHERE vendor_project_id = :vendorProjectId"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, vendorProjectId=vendorProjectId):
                projectId = row['cit_sci_proj_id']
                conn.close()

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        return Response(
            status=500,
            response="An error occurred while reading the citizen_science_projects table."
        )
    return projectId

def createNewOwnerRecord(email):
    global db
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_owners (email, status)"
        " VALUES (:email, :status) RETURNING cit_sci_owner_id"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, email=email, status='active'):
                ownerId = row['cit_sci_owner_id']
                conn.close()

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        return Response(
            status=500,
            response="An error occurred while creating citizen_science_owners record(s)."
        )

    # return ownerId['cit_sci_owner_id']
    return ownerId

def lookupOwnerRecord(emailP):
    print("inside the lookup_owner_record() function")
    print(emailP)
    global db
    stmt = sqlalchemy.text(
        "SELECT cit_sci_owner_id FROM citizen_science_owners WHERE email=:email"
    )

    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, email=emailP):
                ownerId = row['cit_sci_owner_id']
                conn.close()

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        return Response(
            status=500,
            response="An error occurred while reading from the citizen_science_owners table."
        )
   
    # return ownerId.lastrowid
    return ownerId

def insertMetaRecord(uri, sourceId, sourceIdType, projectId):
    global db
    edcVerId = 11000222
    public = True
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_meta (edc_ver_id, source_id, source_id_type, uri, public)"
        " VALUES (:edcVerIdSQL, :sourceIdSQL, :sourceIdTypeSQL, :uriSQL, :publicSQL) RETURNING cit_sci_meta_id"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, edcVerIdSQL=edcVerId, sourceIdSQL=sourceId, sourceIdTypeSQL=sourceIdType, uriSQL=uri, publicSQL=public):
                metaRecordId = row['cit_sci_meta_id']
                insertLookupRecord(metaRecordId, projectId)
            conn.close()
            

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        return Response(
            status=500,
            response="An error occurred while creating a citizen_science_meta record."
        )
    return

def insertLookupRecord(metaRecordId, projectId):
    global db
    print("inside of the lookup record function")
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_proj_meta_lookup (cit_sci_proj_id, cit_sci_meta_id)"
        " VALUES (:projectId, :metaRecordId)"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            conn.execute(stmt, projectId=projectId, metaRecordId=metaRecordId)
            conn.close()
            
    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        return Response(
            status=500,
            response="An error occurred while creating a citizen_science_meta record."
        )
    return

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

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))