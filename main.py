import os, fnmatch, json, subprocess, csv, shutil, time
import glob # for debugging
from citizen_science_validator import CitizenScienceValidator
from data_exporter_response import DataExporterResponse
from flask import Flask, request, Response
from google.cloud import storage
import panoptes_client
from panoptes_client import Panoptes, Project, SubjectSet
import sqlalchemy
from pprint import pprint
# Imports the Cloud Logging client library
from google.cloud import logging
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
log_name = "rsp-data-exporter"
logger = logging_client.logger(log_name)
response = DataExporterResponse()
validator = CitizenScienceValidator()
debug = False

@app.route("/citizen-science-ingest-status")
def check_status_of_previously_executed_ingest():
    global response
    guid = request.args.get("guid")

    gcs = storage.Client()

    manifest_path = guid + "/manifest.csv"

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_BUCKET_HIPS2FITS)
    exists = storage.Blob(bucket=bucket, name=manifest_path).exists(gcs)

    response = DataExporterResponse()
    response.messages = []

    if exists:
        response.status = "success"
        response.manifest_url = "https://storage.googleapis.com/citizen-science-data/" + guid + "/manifest.csv"
    else:
        response.status = "error"
        response.messages.append("The job either failed or is still processing, please try again later.")

    res = json.dumps(response.__dict__)
    return res

@app.route("/citizen-science-bucket-ingest")
def download_bucket_data_and_process():
    global response, validator, debug
    guid = request.args.get("guid")
    email = request.args.get("email")
    vendor_project_id = request.args.get("vendor_project_id")
    vendor_batch_id = request.args.get("vendor_batch_id")
    debug = bool(request.args.get("debug"))
    # large_import = bool(request.args.get("large_import"))
    response = DataExporterResponse()
    response.messages = []
    validator = CitizenScienceValidator()

    time_mark(debug, __name__)
    
    validate_project_metadata(email, vendor_project_id, vendor_batch_id)

    if validator.error is False:
        cutouts = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS, guid + ".zip", guid)
    
        if validator.error is False:
            urls = upload_cutouts(cutouts, vendor_project_id)

            if validator.error is False:
            
                manifest_url = build_and_upload_manifest(urls, email, "556677", CLOUD_STORAGE_BUCKET_HIPS2FITS, guid + "/")
            
                response.status = "success"
                response.manifest_url = manifest_url
    else:
        response.status = "error"
        if response.messages == None or len(response.messages) == 0:
            response.messages.append("An error occurred while processing the data batch, please try again later.")

    res = json.dumps(response.__dict__)
    logger.log_text(res)
    time_mark(debug, "Done processing, return response to notebook aspect")
    return res

def upload_cutouts(cutouts, vendor_project_id):
    # Get the bucket that the file will be uploaded to.
    # Create a Cloud Storage client.
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_BUCKET_HIPS2FITS)
    urls = []

    cutouts_count = 0
    time_mark(debug, "Start of upload & inserting of metadata...")
    for cutout in cutouts:
        destination_filename = cutout.replace("/tmp/", "")
        blob = bucket.blob(destination_filename)
        
        blob.upload_from_filename(cutout)
        urls.append(blob.public_url)
        # Insert meta records
        insert_meta_record(blob.public_url, str(round(time.time() * 1000)) , 'sourceId', vendor_project_id)
        cutouts_count += 1
    time_mark(debug, "Upload and metadata insertion finished...")
    return urls

# Accepts the bucket name and filename to download and returns the path of the downloaded file
def download_zip(bucket_name, filename, file = None):
    global response, validator, db, debug
    time_mark(debug, "Start of download zip")
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket_name)

    # Download the file to /tmp storage
    blob = bucket.blob(filename)
    zipped_cutouts = "/tmp/" + filename
    time_mark(debug, "Start of download...")
    blob.download_to_filename(zipped_cutouts)
    time_mark(debug, "Download finished...")

    # logger.log_text("rosas - about to log the /tmp directory contents")
    # rosas_test = str(glob.glob("/tmp/*"))
    # logger.log_text(rosas_test)

    unzipped_cutouts_dir = "/tmp/" + file
    os.mkdir(unzipped_cutouts_dir)
    time_mark(debug, "Start of unzip....")
    shutil.unpack_archive(zipped_cutouts, unzipped_cutouts_dir, "zip")
    time_mark(debug, "Unzip finished...")

    # Count the number of objects and remove any files more than the allotted amount based on
    # the RSP user's data rights approval status
    time_mark(debug, "Start of dir count...")
    files = os.listdir(unzipped_cutouts_dir)
    time_mark(debug, "Dir count finished...")
    max_objects_count = 100
    logger.log_text("rosas - logging validator.data_rights_approved : " + str(validator.data_rights_approved)) 
    if validator.data_rights_approved == True:
        max_objects_count = 10000
    else:
        response.messages.append("Your project has not been approved by the data rights panel as of yet, as such you will not be able to send any additional data to Zooniverse until your project is approved.")

    if len(files) > max_objects_count:
        response.messages.append("Currently, a maximum of " + str(max_objects_count) + " objects is allowed per batch for your project - your batch has been has been truncated and anything in excess of " + str(max_objects_count) + " objects has been removed.")
        time_mark(debug, "Start of truncating excess files")
        for f_file in files[max_objects_count:]:
            # response.messages.append("Removing file : " + unzipped_cutouts_dir + "/" + f_file)
            os.remove(unzipped_cutouts_dir + "/" + f_file)
        time_mark(debug, "Truncating finished...")

    # logger.log_text("rosas - about to log the " + unzipped_cutouts_dir + "/* directory contents")
    # rosas_test = str(glob.glob(unzipped_cutouts_dir + "/*"))
    # logger.log_text(rosas_test)

    # Now, limit the files sent to image files
    time_mark(debug, "Start of grabbing all the cutouts for return...")
    pngs = glob.glob("/tmp/" + file + "/*.png")
    jpegs = glob.glob("/tmp/" + file + "/*.jpeg")
    jpgs = glob.glob("/tmp/" + file + "/*.jpg")
    cutouts = pngs + jpegs + jpgs
    time_mark(debug, "Grabbing cutouts finished...")
    return cutouts

def build_and_upload_manifest(urls, email, sourceId, bucket, destination_root = ""):
    global debug
    time_mark(debug, "In build and upload manifest")
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

def validate_project_metadata(email, vendor_project_id, vendor_batch_id = None):
    global db, validator, debug
    db = init_connection_engine()
    newOwner = False

    # Lookup if owner record exists, if so then return it
    ownerId = lookup_owner_record(email)

    # Create owner record
    if validator.error == False:
        if ownerId == None:
            newOwner = True
            ownerId = create_new_owner_record(email)
    else:
        return
        
    # Then, lookup project
    if validator.error == False:
        if(newOwner == True):
            project_id = create_new_project_record(vendor_project_id)
        else:
            project_id = lookup_project_record(vendor_project_id)
            if project_id is None:
                project_id = create_new_project_record(ownerId, vendor_project_id)
    else:
        return

    # Then, check batch status
    if validator.error == False:
        batch_id = check_batch_status(project_id, vendor_project_id) # To-do: Look into whether or not this would be a good time to check with Zoony on the status of the batches

        if batch_id < 0:
            # Create new batch record
            batchId = create_new_batch(project_id, vendor_batch_id)

            if(batchId > 0):
                # db.dispose()
                return True
            else:
                # db.dispose()
                return False
    else:
        return False

# To-do: Add vendor_batch_id to workflow of this function/route
@app.route("/citizen-science-butler-ingest")
def butler_retrieve_data_and_upload():
    global db
    db = init_connection_engine()
    email = request.args.get('email')
    collection = request.args.get('collection')
    source_id = request.args.get("sourceId")
    vendor_project_id = request.args.get("vendorProjectId")
    output = subprocess.run(['sh', '/opt/lsst/software/server/run.sh', email, collection], stdout=subprocess.PIPE).stdout.decode('utf-8')

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

    if valid_project_metadata is True:
        # Finally, insert meta records
        insert_meta_record(manifest_url, source_id, 'sourceId', vendor_project_id)
    return manifest_url
        
def create_new_batch(project_id, vendor_batch_id):
    global db, validator, reponse, debug
    time_mark(debug, "Start of create new batch")
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_batches (cit_sci_proj_id, batch_status, vendor_batch_id)"
        "VALUES (:projectId, 'ACTIVE', :vendorBatchId) RETURNING cit_sci_batch_id"
    )
    try:
        batchId = -1;
        with db.connect() as conn:
            for row in conn.execute(stmt, projectId=project_id, vendorBatchId=vendor_batch_id):
                batchId = row['cit_sci_batch_id']
            conn.close()
    except Exception as e:
        logger.log_text(e)
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new data batch record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return batchId

def check_batch_status(project_id, vendor_project_id):
    # First, look up batches in the database, which may 
    global db, validator, debug
    time_mark(debug, "Start of check batch status")
    batch_id = -1
    vendor_batch_id_db = 0
    stmt = sqlalchemy.text(
        "SELECT * FROM citizen_science_batches WHERE cit_sci_proj_id = :projectId AND batch_status = 'ACTIVE'"
    )
    try:
        with db.connect() as conn:
            records = conn.execute(stmt, projectId=project_id)
            if(records.rowcount > 0):
                record = records.first()
                batch_id = record['cit_sci_batch_id']
                vendor_batch_id_db = record['vendor_batch_id']
            conn.close()
    except Exception as e:
        print(e)

    if batch_id > 0: # An active batch record was found in the DB
        # Call the Zooniverse API to get all subject sets for the project
        project = Project.find(int(vendor_project_id))

        update_batch_record = False;
        for sub in list(project.links.subject_sets):
            if str(vendor_batch_id_db) == sub.id:
                for completeness_score in sub.completeness:
                    if sub.completeness[completeness_score] == 1.0:
                        update_batch_record = True
                    else:
                        update_batch_record = False
                        break
        if update_batch_record == True:
            updt_stmt = sqlalchemy.text(
                "UPDATE citizen_science_batches SET batch_status = 'COMPLETE' WHERE cit_sci_proj_id = :projectId AND cit_sci_batch_id = :batchId"
            )
            try:
                # batch_id = -1
                with db.connect() as conn:
                    conn.execute(updt_stmt, projectId=project_id, batchId=batch_id)
                    conn.close()
                
            except Exception as e:
                print(e)
            
            return -1 # no active batches
    
    return batch_id

def create_new_project_record(ownerId, vendorProjectId):
    global db, validator, response, debug
    time_mark(debug, "Start of create new project")
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_projects (vendor_project_id, owner_id, project_status)"
        " VALUES (:vendorProjectId, :ownerId, :projectStatus) RETURNING cit_sci_proj_id"
    )
    project_id = None
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, vendorProjectId=vendorProjectId, ownerId=ownerId, projectStatus='active'):
                project_id = row['cit_sci_proj_id']
                conn.close()

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return project_id

def lookup_project_record(vendorProjectId):
    global db, response, validator, debug
    time_mark(debug, "Start of lookup project record")
    project_id = None
    stmt = sqlalchemy.text(
        "SELECT cit_sci_proj_id, project_status, data_rights_approved FROM citizen_science_projects WHERE vendor_project_id = :vendorProjectId"
    )
    project_id = None
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, vendorProjectId=vendorProjectId):
                status = row['project_status']
                validator.data_rights_approved = row["data_rights_approved"]
                if status == "complete" or status == "cancelled" or status == "abandoned":
                    response.status = "error"
                    validator.error = True
                    response.messages.append("This project is in a status of " + status + " - either create a new project or contact Rubin to request for the project to be reopened.")
                    # return
                else:
                    project_id = row['cit_sci_proj_id']
        conn.close()

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to lookup your project record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return project_id

def create_new_owner_record(email):
    global db, validator, response, debug
    time_mark(debug, "Start of create new owner")
    stmt = sqlalchemy.text(
        "INSERT INTO citizen_science_owners (email, status)"
        " VALUES (:email, :status) RETURNING cit_sci_owner_id"
    )
    owner_id = None;
    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, email=email, status='active'):
                owner_id = row['cit_sci_owner_id']
                conn.close()
                return owner_id

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return owner_id

def lookup_owner_record(emailP):
    global db, validator, response, debug
    time_mark(debug, "Looking up owner record")
    stmt = sqlalchemy.text(
        "SELECT cit_sci_owner_id, status FROM citizen_science_owners WHERE email=:email"
    )
    ownerId = None
    status = ""

    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            for row in conn.execute(stmt, email=emailP):
                ownerId = row['cit_sci_owner_id']
                status = row['status']
                if status == "blocked" or status == "disabled":
                    validator.error = True
                    response.status = "error"
                    response.messages.append("You are not/no longer eligible to use the Rubin Science Platform to send data to Zooniverse.")
                conn.close()
                return ownerId

    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while looking up your projects owner record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
   
    return ownerId

def lookup_meta_record(sourceId, sourceIdType):
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

def insert_meta_record(uri, sourceId, sourceIdType, projectId):
    global db, debug
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
            for row in conn.execute(stmt, edcVerIdSQL=edcVerId, sourceIdSQL=sourceId, sourceIdTypeSQL=sourceIdType, uriSQL=uri, publicSQL=public):
                metaRecordId = row['cit_sci_meta_id']
                errorOccurred = True if insert_lookup_record(metaRecordId, projectId) else False
            conn.close()

    except Exception as e:
        # Is the exception because of a duplicate key error? If so, lookup the ID of the meta record and perform the insert into the lookup table
        if "non_dup_records" in e.__str__():
            metaId = lookup_meta_record(sourceId, sourceIdType)
            return insert_lookup_record(metaId, projectId)
        return False
    return errorOccurred

def insert_lookup_record(metaRecordId, projectId):
    global db
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

def time_mark(debug, milestone):
    if debug == True:
        print("Time mark - " + str(round(time.time() * 1000)) + " - in " + milestone);

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))