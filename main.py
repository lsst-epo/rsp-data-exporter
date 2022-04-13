import os, fnmatch
import glob # for debugging
import subprocess
import csv
from flask import Flask, request, Response
from google.cloud import storage
import panoptes_client
import sqlalchemy
from pprint import pprint
# Imports the Cloud Logging client library
from google.cloud import logging
import os
import shutil
import time
# import lsst.daf.butler as dafButler

app = Flask(__name__)

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
CLOUD_STORAGE_BUCKET_HIPS2FITS = os.environ['CLOUD_STORAGE_BUCKET_HIPS2FITS']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
db = None

# Instantiates the logging client
logging_client = logging.Client()
log_name = "rosas"
logger = logging_client.logger(log_name)

@app.route("/new-bucket-ingest")
def download_bucket_data_and_process():
    logger.log_text("rosas - inside of new-bucket-ingest!")
    logger.log_text("rosas - about to log guid!")
    guid = request.args.get("guid")
    email = request.args.get("email")
    vendor_project_id = request.args.get("vendor_project_id")
    # logger.log_text(rosas_test)
    logger.log_text("just logged guid!")
    logger.log_text(request.args.get("email"))

    valid_project_metadata = validate_project_metadata(email, vendor_project_id)

    if valid_project_metadata is True:
        cutouts = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS, guid + ".zip", guid)

        # Get the bucket that the file will be uploaded to.
        # Create a Cloud Storage client.
        gcs = storage.Client()
        bucket = gcs.bucket(CLOUD_STORAGE_BUCKET_HIPS2FITS)
        urls = []

        cutouts_count = 0
        for cutout in cutouts:
            if cutouts_count == 9999: # cutout max limit
                break
            destination_filename = cutout.replace("/tmp/" + guid + "/", "")
            blob = bucket.blob(destination_filename)
            blob.upload_from_filename(cutout)
            urls.append(blob.public_url)
            # Insert meta records
            logger.log_text("about to insert meta records")
            insertMetaRecord(blob.public_url, str(round(time.time() * 1000)) , 'sourceId', vendor_project_id)
            cutouts_count += 1

        manifest_url = build_and_upload_manifest(urls, email, "556677", CLOUD_STORAGE_BUCKET_HIPS2FITS)

        return manifest_url

# Accepts the bucket name and filename to download and returns the path of the downloaded file
def download_zip(bucket_name, filename, file = None):
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket_name)

    logger.log_text("rosas - about to download file from GCS")
    # Download the file to /tmp storage
    blob = bucket.blob("tmp/" + filename)
    zipped_cutouts = "/tmp/" + filename
    blob.download_to_filename(zipped_cutouts)

    logger.log_text("rosas - about to log the /tmp directory contents")
    rosas_test = str(glob.glob("/tmp/*"))
    logger.log_text(rosas_test)

    unzipped_cutouts_dir = "/tmp/" + file
    os.mkdir(unzipped_cutouts_dir)
    shutil.unpack_archive(zipped_cutouts, unzipped_cutouts_dir, "zip")

    # rosas_test2 = str(glob.glob("/tmp/" + file + "/*"))
    cutouts = glob.glob("/tmp/" + file + "/*")
    # logger.log_text(rosas_test2)
    return cutouts

def build_and_upload_manifest(urls, email, sourceId, bucket, destination_root = ""):
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    # loop over urls

    with open('/tmp/manifest.csv', 'w', newline='') as csvfile:
        fieldnames = ['email', 'location:1', 'external_id']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        offset = 1
        for url in urls:
            writer.writerow({'email': email, 'location:1': url, 'external_id': str(round(time.time() * 1000) + offset) })
            offset += 1
    
        
    manifestBlob = bucket.blob(destination_root + "manifest.csv")

    manifestBlob.upload_from_filename("/tmp/manifest.csv")
    return manifestBlob.public_url

def validate_project_metadata(email, vendor_project_id):
    global db
    db = init_connection_engine()
    newOwner = False
    ownerId = lookupOwnerRecord(email)

    # First lookup owner
    if (isinstance(ownerId, int)):
        logger.log_text("Found owner record")
    else:
        logger.log_text("NO owner record found")
        newOwner = True
        ownerId = createNewOwnerRecord(email)

    # Then, lookup project
    if(newOwner == True):
        logger.log_text("newOwner is true, so calling lookupProjectRecord()")
        projectId = lookupProjectRecord(vendor_project_id)
    else:
        logger.log_text("newOwner is false, so first looking up if a project exists")
        projectId = lookupProjectRecord(vendor_project_id)
        logger.log_text("projectId 1: " + str(projectId))
        if projectId is None:
            projectId = createNewProjectRecord(ownerId, vendor_project_id)
            logger.log_text("projectId 2: " + str(projectId))

    # Then, lookup batch info
    logger.log_text("about to check batch status")
    batchId = checkBatchStatus(projectId)
    logger.log_text("batch status: " + str(batchId))

    if(batchId > 0):
        # Do not allow for the creation of an existing batch if there are 
        logger.log_text("batchID is > 0!!!")
        return Response(
            status=500,
            response="You cannot send a new batch of data to your citizen science project because you already have an active, uncompleted batch of data in-progress."
        )
    else:
        logger.log_text("batchId is < 0, creating new batch record")
        # Create new batch record
        batchId = createNewBatch(projectId)
        logger.log_text("new batchId: " + str(batchId))

        if(batchId > 0):
            db.dispose()
            return True
        else:
            db.dispose()
            return False
        
@app.route("/citizen-science-ingest")
def butler_retrieve_data_and_upload():
    global db
    db = init_connection_engine()
    email = request.args.get('email')
    collection = request.args.get('collection')
    source_id = request.args.get("sourceId")
    vendor_project_id = request.args.get("vendorProjectId")
    output = subprocess.run(['sh', '/opt/lsst/software/server/run.sh', email, collection], stdout=subprocess.PIPE).stdout.decode('utf-8')
    # logger.log_text(output)

    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_BUCKET)

    filepath = locate('*.fits', "/tmp/data/" + collection)
    # # Create a new blob and upload the file's content.
    blob = bucket.blob(filepath[1])

    blob.upload_from_filename(str(filepath[0]))

    manifest_url = build_and_upload_manifest(['https://storage.googleapis.com/butler-config/astrocat.jpeg'], email, "000222444", CLOUD_STORAGE_BUCKET)
    valid_project_metadata = validate_project_metadata(email, vendor_project_id)

    if validate_project_metadata is True:
        # Finally, insert meta records
        logger.log_text("about to insert meta records")
        insertMetaRecord(manifest_url, source_id, 'sourceId', vendor_project_id)
    return manifest_url
        
def createNewBatch(projectId):
    logger.log_text("rosas - inside createNewBatch")
    global db
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_batches (cit_sci_proj_id, batch_status)"
        "VALUES (:projectId, 'ACTIVE') RETURNING cit_sci_batch_id"
    )
    try:
        batchId = -1;
        with db.connect() as conn:
            for row in conn.execute(stmt, projectId=projectId):
                logger.log_text("rosas - about to low row")
                logger.log_text(str(row))
                batchId = row['cit_sci_batch_id']
            conn.close()
    except Exception as e:
        logger.log_text(e)
        # return Response(
        #     status=500,
        #     response="An error occurred while creating a citizen_sciences_batches record."
        # )
        return e
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
        # return Response(
        #     status=500,
        #     response="An error occurred while creating citizen_science_projects record(s)."
        # )
        return e
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
        # return Response(
        #     status=500,
        #     response="An error occurred while reading the citizen_science_projects table."
        # )
        return e
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
                return ownerId

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
    global db
    stmt = sqlalchemy.text(
        "SELECT cit_sci_owner_id FROM citizen_science_owners WHERE email=:email"
    )
    ownerId = ""

    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, email=emailP):
                ownerId = row['cit_sci_owner_id']
                conn.close()
                return ownerId

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        # return Response(
        #     status=500,
        #     response="An error occurred while reading from the citizen_science_owners table."
        # )
        return e
   
    # return ownerId.lastrowid
    return ownerId

def lookupMetaRecord(sourceId, sourceIdType):
    global db
    stmt = sqlalchemy.text(
        "SELECT cit_sci_meta_id FROM citizen_science_meta WHERE source_id=:sourceId AND source_id_type=:sourceIdType"
    )
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, sourceId=sourceId, sourceIdType=sourceIdType):
                metaId = row['cit_sci_meta_id']
                conn.close()

    except Exception as e:
        # If something goes wrong, handle the error in this section. This might
        # involve retrying or adjusting parameters depending on the situation.

        # TO-DO: Add logger
        # logger.exception(e)
        print(e)
        # return Response(
        #     status=500,
        #     response="An error occurred while reading from the citizen_science_owners table."
        # )
        return e
   
    # return ownerId.lastrowid
    return metaId

def insertMetaRecord(uri, sourceId, sourceIdType, projectId):
    global db
    edcVerId = 11000222
    public = True
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_meta (edc_ver_id, source_id, source_id_type, uri, public)"
        " VALUES (:edcVerIdSQL, :sourceIdSQL, :sourceIdTypeSQL, :uriSQL, :publicSQL) RETURNING cit_sci_meta_id"
    )
    errorOccurred = False;
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            logger.log_text("inside with:")
            logger.log_text(repr(conn))
            for row in conn.execute(stmt, edcVerIdSQL=edcVerId, sourceIdSQL=sourceId, sourceIdTypeSQL=sourceIdType, uriSQL=uri, publicSQL=public):
                metaRecordId = row['cit_sci_meta_id']
                errorOccurred = True if insertLookupRecord(metaRecordId, projectId) else False
            conn.close()

    except Exception as e:
        # Is the exception because of a duplicate key werror? If so, lookup the ID of the meta record and perform the insert into the lookup table
        if "non_dup_records" in e.__str__():
            metaId = lookupMetaRecord(sourceId, sourceIdType)
            return insertLookupRecord(metaId, projectId)
        return False
    return errorOccurred

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
        logger.exception(e.__str__())
        return False
        # return Response(
        #     status=500,
        #     response="An error occurred while creating a citizen_science_meta record."
        # )
    return True

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