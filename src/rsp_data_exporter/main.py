import os, fnmatch, json, csv, shutil, time, threading, glob
from google.cloud import logging

TEST_ONLY = bool(os.environ.get('TEST_ONLY'))
logging_client = logging.Client()
log_name = "rsp-data-exporter.main"
if TEST_ONLY == True:
    log_name = "rsp-data-exporter-tests"
logger = logging_client.logger(log_name)

# Try to import the code relatively, which works fine when running live, but doesn't work for
# running unit tests with Pytest, so accommodate for both scenarios:
try:
    from .models.citizen_science.citizen_science_validator import CitizenScienceValidator
    from .models.data_exporter_response import DataExporterResponse
    from .models.edc_logger import EdcLogger
except:
    try:
        from models.citizen_science.citizen_science_validator import CitizenScienceValidator
        from models.data_exporter_response import DataExporterResponse
        from models.edc_logger import EdcLogger
    except Exception as e:
        if TEST_ONLY == True:
            logger.log_text("An error occurred while attempting to import subpackages!")
            logger.log_text(e.__str__())        

from flask import Flask, request
from google.cloud import storage
import numpy as np
import services.audit_report as AuditReportService
import services.manifest_file as ManifestFileService
import services.owner as OwnerService
import services.project as ProjectService
import services.batch as BatchService
import services.metadata as MetadataService
import services.lookup as LookupService
import services.tabular_data as TabularDataService

app = Flask(__name__)
response = DataExporterResponse()
validator = CitizenScienceValidator()
CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
CLOUD_STORAGE_BUCKET_HIPS2FITS = os.environ['CLOUD_STORAGE_BUCKET_HIPS2FITS']
CLOUD_STORAGE_CIT_SCI_URL_PREFIX = os.environ["CLOUD_STORAGE_CIT_SCI_URL_PREFIX"]
CLOUD_STORAGE_CIT_SCI_PUBLIC = os.environ["CLOUD_STORAGE_CIT_SCI_PUBLIC"]
debug = False

def check_test_only_var():
    return TEST_ONLY

@app.route("/active-batch-metadata")
def get_batch_metadata():
    logger.log_text("get_batch_metadata:: Inside of get_batch_metadata!!!!")

    vendor_project_id = request.args.get("vendor_project_id")
    logger.log_text("get_batch_metadata:: vendor_project_id=" + str(vendor_project_id))

    project_id = lookup_project_record(vendor_project_id)
    logger.log_text("get_batch_metadata:: project_id=" + str(project_id))

    # Clean up batches before fetching batch ID
    check_batch_status(project_id, vendor_project_id)

    # Fetch batch ID
    batch_id = BatchService.get_current_active_batch_id(project_id)
    logger.log_text("get_batch_metadata:: batche_id = " + str(batch_id))

    manifest_url = ManifestFileService.lookup_manifest_url(batch_id)

    return json.dumps({
        "metadata_url": manifest_url
    })

def query_lookup_records(project_id, batch_id):
    return LookupService.query_lookup_records(project_id, batch_id)

@app.route("/citizen-science-ingest-status")
def check_status_of_previously_executed_ingest():
    global response
    guid = request.args.get("guid")
    exists = ManifestFileService.check_if_manifest_file_exists(guid)
    response = DataExporterResponse()
    response.messages = []

    if exists:
        response.status = "success"
        response.manifest_url = CLOUD_STORAGE_CIT_SCI_URL_PREFIX + CLOUD_STORAGE_CIT_SCI_PUBLIC + "/" + guid + "/manifest.csv"
    else:
        response.status = "error"
        response.messages.append("The job either failed or is still processing, please try again later.")

    res = json.dumps(response.__dict__)
    return res

@app.route("/citizen-science-tabular-ingest")
def download_tabular_data_and_process():
    logger.log_text("/citizen-science-tabular-ingest hit!")
    global response, validator, debug, urls
    guid = request.args.get("guid")
    email = request.args.get("email")
    vendor_project_id = request.args.get("vendor_project_id")
    vendor_batch_id = request.args.get("vendor_batch_id")
    debug = bool(request.args.get("debug"))
    response = DataExporterResponse()
    response.messages = []
    validator = CitizenScienceValidator()
    urls = []

    time_mark(debug, __name__)
    validate_project_metadata(email, vendor_project_id, vendor_batch_id) 

    logger.log_text("just validated project metadata")
    if validator.error is False:
        logger.log_text("validator.error is False!")

        tabular_records = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS , "manifest.csv", guid, True)
        logger.log_text("about to call upload_manifest()")
        manifest_url = upload_manifest("/tmp/" + guid + "/manifest.csv")
        logger.log_text("done running upload_manifest()")
        # if data_format == "objects":
        #     logger.log_text("data_format == object, about to call create_dr_object_records")
        #     create_dr_objects_records(csv_path)
        # elif data_format == "diaobjects":
        #     logger.log_text("data_format == object, about to call create_dr_diaobject_records")
        #     create_dr_diaobject_records(csv_path)
        # elif data_format == "forcedsources":
        #     logger.log_text("data_format == object, about to call create_dr_forcedsource_records")
        #     create_dr_forcedsource_records(csv_path)
        # else:
        #     logger.log_text("data_format != object!!!")
        # manifest_url = transfer_tabular_manifest(urls, CLOUD_STORAGE_CIT_SCI_PUBLIC, guid)
        # updated_meta_records = update_meta_records_with_user_values(meta_records)
        tabular_meta_records = create_tabular_meta_records(tabular_records)
        meta_records_with_id = insert_meta_records(tabular_meta_records)
        lookup_status = insert_lookup_records(meta_records_with_id, validator.project_id, validator.batch_id)
        logger.log_text("lookup_status: " + str(lookup_status))
        response.status = "success"
        response.manifest_url = manifest_url
    else:
        logger.log_text("validator.error is True!")
        response.status = "error"
        if response.messages == None or len(response.messages) == 0:
            response.messages.append("An error occurred while processing the data batch, please try again later.")

    res = json.dumps(response.__dict__)
    time_mark(debug, "Done processing, return response to notebook aspect")
    return res

@app.route("/citizen-science-image-ingest")
def download_image_data_and_process():
    logger.log_text("/citizen-science-image-ingest hit!")
    global response, validator, debug, urls
    guid = request.args.get("guid")
    email = request.args.get("email")
    vendor_project_id = request.args.get("vendor_project_id")
    vendor_batch_id = request.args.get("vendor_batch_id")
    debug = bool(request.args.get("debug"))
    response = DataExporterResponse()
    response.messages = []
    validator = CitizenScienceValidator()
    urls = []

    time_mark(debug, __name__)

    validate_project_metadata(email, vendor_project_id, vendor_batch_id) 
    
    logger.log_text("just validated project metadata")
    if validator.error is False:
        logger.log_text("validator.error is False!")
    
        # astro cutouts data
        cutouts = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS, guid + ".zip", guid)

        if validator.error is False:
            urls, meta_records = upload_cutouts(cutouts, vendor_project_id)

            if validator.error is False:                
                manifest_url = build_and_upload_manifest(urls, CLOUD_STORAGE_CIT_SCI_PUBLIC, guid)
                updated_meta_records = update_meta_records_with_user_values(meta_records)
                meta_records_with_id = insert_meta_records(updated_meta_records)
                lookup_status = insert_lookup_records(meta_records_with_id, validator.project_id, validator.batch_id)
                logger.log_text("lookup_status: " + str(lookup_status))
                response.status = "success"
                response.manifest_url = manifest_url

                audit_records, audit_messages = insert_audit_records(vendor_project_id)

                if len(audit_records) < len(validator.mapped_manifest):
                    response.messages.append("Some audit records were not inserted!")

                if len(audit_messages) > 0:
                    response.messages = response.messages + audit_messages
    else:
        logger.log_text("validator.error is True!")
        response.status = "error"
        if response.messages == None or len(response.messages) == 0:
            response.messages.append("An error occurred while processing the data batch, please try again later.")
    

    res = json.dumps(response.__dict__)
    time_mark(debug, "Done processing, return response to notebook aspect")
    return res

@app.route("/citizen-science-audit-report")
def fetch_audit_records():
    vendor_project_id = request.args.get("vendor_project_id")
    try:
        return AuditReportService.fetch_audit_records(vendor_project_id)
    except Exception as e:
        logger.log_text("an exception occurred in fetch_audit_records!")
        logger.log_text(e.__str__())
        response = DataExporterResponse()
        response.status = "ERROR"
        response.messages.append("An error occurred while looking up the audit records associated with Zooniverse project ID: " + vendor_project_id)
        return json.dumps(response.__dict__)

def insert_audit_records(vendor_project_id):
    global validator
    vendor_project_id = request.args.get("vendor_project_id")
    try:
        return AuditReportService.insert_audit_records(vendor_project_id, validator.mapped_manifest, validator.owner_id)
    except Exception as e:
        logger.log_text("an exception occurred in insert_audit_records!")
        logger.log_text(e.__str__())
        response = DataExporterResponse()
        response.status = "ERROR"
        response.messages.append("An error occurred while looking up the audit records associated with Zooniverse project ID: " + vendor_project_id)
        return json.dumps(response.__dict__)

def update_batch_record_with_manifest_url(manifest_url_p):
    return ManifestFileService.update_batch_record_with_manifest_url(manifest_url_p, validator.batch_id)

def create_tabular_meta_records(tabular_records):
    return MetadataService.create_tabular_meta_records(tabular_records)

def update_meta_records_with_user_values(meta_records):
    user_defined_values, info_message = ManifestFileService.update_meta_records_with_user_values(meta_records, validator.mapped_manifest)
    if info_message != "":
        response.messages.append(info_message)
    return user_defined_values

def upload_manifest(csv_path):
    logger.log_text("inside of upload_manifest")
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    destination_filename = csv_path.replace("/tmp/", "")
    blob = bucket.blob(destination_filename)
    blob.upload_from_filename(csv_path)

    logger.log_text("logging blob.public_url:")
    logger.log_text(blob.public_url)
    return blob.public_url

def create_dr_forcedsource_records(csv_path):
    return TabularDataService.create_dr_forcedsource_records(csv_path)

def create_dr_diaobject_records(csv_path):
    return TabularDataService.create_dr_diaobject_records(csv_path)

def create_dr_objects_records(csv_path):
    return TabularDataService.create_dr_objects_records(csv_path)

def upload_cutouts(cutouts, vendor_project_id):
    global debug

    # Beginning of optimization code
    time_mark(debug, "Start of upload...")
    if len(cutouts) > 500: # Arbitrary threshold for threading
        subset_count = round(len(np.array(cutouts)) / 250)
        sub_cutouts_arr = np.split(np.array(cutouts), subset_count) # create sub arrays divided by 1k cutouts
        threads = []
        for i, sub_arr in enumerate(sub_cutouts_arr):
            t = threading.Thread(target=upload_cutout_arr, args=(sub_arr,str(i),))
            threads.append(t)
            threads[i].start()
        
        for thread in threads:
            logger.log_text("joining thread!")
            thread.join()

    else:
        urls = upload_cutout_arr(cutouts, str(1))
    time_mark(debug, "End of upload...")

    time_mark(debug, "Start of upload & inserting of metadata...")
    meta_records = create_meta_records(urls)
    time_mark(debug, "End of inserting of metadata records")
    return urls, meta_records
 
def upload_cutout_arr(cutouts, i):
    urls = []
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)

    already_logged = False

    for cutout in cutouts:
        if already_logged == False:
            logger.log_text(cutout)
            already_logged = True
        destination_filename = cutout.replace("/tmp/", "")
        blob = bucket.blob(destination_filename)
        
        blob.upload_from_filename(cutout)
        urls.append(blob.public_url)

    logger.log_text("finished uploading thread #" + i)

    return urls
        
def create_meta_records(urls):
    return MetadataService.create_meta_records(urls)

def insert_meta_records(meta_records):
    return MetadataService.insert_meta_records(meta_records)

def download_zip(bucket_name, filename, guid, is_tabular_dataset = False):
    global response, validator, db, debug
    time_mark(debug, "Start of download zip")
    # Create a Cloud Storage client.
    gcs = storage.Client()
    os.makedirs("/tmp/" + guid + "/", exist_ok=True)
    if is_tabular_dataset == True:
        filename = guid + "/" + filename

    logger.log_text("about to log bucket download name:")
    logger.log_text(bucket_name)

    bucket = gcs.bucket(bucket_name)

    # Download the file to /tmp storage
    logger.log_text("filename to download: " + filename)
    blob = bucket.blob(filename)
    zipped_cutouts = "/tmp/" + filename
    time_mark(debug, "Start of download...")
    blob.download_to_filename(zipped_cutouts)
    time_mark(debug, "Download finished...")

    unzipped_cutouts_dir = "/tmp/" + guid
    # os.mkdir(unzipped_cutouts_dir)

    # Deviate logic based on data type
    if is_tabular_dataset == True:
        files = os.listdir(unzipped_cutouts_dir)
        csv_path = unzipped_cutouts_dir + "/" + files[0]
        logger.log_text("inside of TABULAR_DATA code block")
        # Get CSV file
        csv_file = open(csv_path, "rU")
        reader = csv.reader(csv_file, delimiter=',')

        logger.log_text("about to log CSV file contents")
        tabular_records = []
        for row in reader:
            logger.log_text(str(row))
            tabular_records.append(row)
        logger.log_text("done logging CSV file contents")

        return tabular_records
    else:
        time_mark(debug, "Start of unzip....")
        # logger.log_text("rosas - about to log the /tmp directory contents")
        # rosas_test = str(glob.glob("/tmp/*"))
        # logger.log_text(rosas_test)

        shutil.unpack_archive(zipped_cutouts, unzipped_cutouts_dir, "zip")
        time_mark(debug, "Unzip finished...")

        # Count the number of objects and remove any files more than the allotted amount based on
        # the RSP user's data rights approval status
        time_mark(debug, "Start of dir count...")
        files = os.listdir(unzipped_cutouts_dir)
        time_mark(debug, "Dir count finished...")

        max_objects_count = 100
        if validator.data_rights_approved == True:
            max_objects_count = 10000
        else:
            response.messages.append("Your project has not been approved by the data rights panel as of yet, as such you will not be able to send any additional data to Zooniverse until your project is approved.")

        logger.log_text("Data is NOT in tabular format")
        if len(files) > max_objects_count:
            response.messages.append("Currently, a maximum of " + str(max_objects_count) + " objects is allowed per batch for your project - your batch of size " + str(len(files)) + " has been has been truncated and anything in excess of " + str(max_objects_count) + " objects has been removed.")
            time_mark(debug, "Start of truncating excess files")
            for f_file in files[(max_objects_count + 1):]:
                # response.messages.append("Removing file : " + unzipped_cutouts_dir + "/" + f_file)
                os.remove(unzipped_cutouts_dir + "/" + f_file)
            time_mark(debug, "Truncating finished...")

        # Now, limit the files sent to image files
        time_mark(debug, "Start of grabbing all the cutouts for return...")
        pngs = glob.glob("/tmp/" + guid + "/*.png")
        jpegs = glob.glob("/tmp/" + guid + "/*.jpeg")
        jpgs = glob.glob("/tmp/" + guid + "/*.jpg")
        cutouts = pngs + jpegs + jpgs
        time_mark(debug, "Grabbing cutouts finished...")
        return cutouts

def transfer_tabular_manifest(bucket, guid):
    global debug, validator
    time_mark(debug, "In transfer tabular manifest")

    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    manifestBlob = bucket.blob(guid + "/manifest.csv")
    logger.log_text("about to upload the new manifest to GCS")
    manifestBlob.upload_from_filename("/tmp/" + guid + "/manifest.csv")
    update_batch_record_with_manifest_url(manifestBlob.public_url)
    return manifestBlob.public_url

def build_and_upload_manifest(urls, bucket, guid = ""):
    time_mark(debug, "In build and upload manifest")
    manifest_url, mapped_manifest = ManifestFileService.build_and_upload_manifest(urls, bucket, validator.batch_id, guid)
    validator.mapped_manifest = mapped_manifest
    return manifest_url

def validate_project_metadata(email, vendor_project_id, vendor_batch_id = None):
    global validator, debug, response
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
            project_id = create_new_project_record(ownerId, vendor_project_id)
        else:
            project_id = lookup_project_record(vendor_project_id)
            if project_id is None:
                project_id = create_new_project_record(ownerId, vendor_project_id)
            
            validator.project_id = project_id
    else:
        return

    # Then, check batch status
    if validator.error == False:
        batch_ids = check_batch_status(project_id, vendor_project_id) 

        if batch_ids is not None and len(batch_ids) == 0:
            # Create new batch record
            batchId = create_new_batch(project_id, vendor_batch_id)

            if(batchId > 0):
                validator.batch_id = batchId
                return True
            else:
                return False
        else:
            # logger.log_text("about to check if create_edc_logger_record() needs to be called")
            # if validator.log_to_edc:
            #     logger.log_text("calling! create_edc_logger_record()")
            #     create_edc_logger_record()
            return False
    else:
        return False

# def create_edc_logger_record():
#     db = EdcLogger.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
#     notes_obj = json.loads(validator.edc_logger_notes)

#     edc_logger_record = EdcLogger(application_name="rsp-data-exporter", run_id=notes_obj.vendor_batch_id, notes=validator.edc_logger_notes, category=validator.edc_logger_category)
#     logger.log_text("about to commit edc-logger record")
#     db.add(edc_logger_record)
#     db.commit()
#     db.close()
#     logger.log_text("committed edc-logger record!")
#     return
        
def create_new_batch(project_id, vendor_batch_id):
    global validator, response, debug
    time_mark(debug, "Start of create new batch")
    batch_id, messages = BatchService.create_new_batch(project_id, vendor_batch_id)

    if len(messages) > 0:
        validator.error = True
        response.status = "error"
        response.messages.append(messages)
    return batch_id

def check_batch_status(project_id, vendor_project_id):
    global validator, debug
    time_mark(debug, "Start of check batch status!!!")

    batches_in_db, messages = BatchService.check_batch_status(project_id, vendor_project_id, TEST_ONLY, validator.data_rights_approved)
    if len(messages) > 0:
        validator.error = True
        response.status = "error"
        response.messages.append(messages)
    return batches_in_db

def create_new_project_record(owner_id, vendor_project_id):
    global validator, response, debug
    time_mark(debug, "Start of create new project")
    project_id, messages = ProjectService.create_new_project_record(owner_id, vendor_project_id)

    if len(messages) > 0:
        validator.error = True
        response.status = "error"
        response.messages.append(messages)
    return project_id

def lookup_project_record(vendor_project_id):
    global response, validator, debug
    time_mark(debug, "Start of lookup project record")
    project_id, data_rights_approved, messages =  ProjectService.lookup_project_record(vendor_project_id)

    validator.data_rights_approved = data_rights_approved
    if len(messages) > 0:
        validator.error = True
        response.status = "error"
        response.messages.append(messages)

    return project_id

def create_new_owner_record(email):
    global validator, response, debug
    time_mark(debug, "Start of create new owner")
    owner_id, messages = OwnerService.create_new_owner_record(email)
    if len(messages) > 0:
        validator.error = True
        response.status = "error"
        response.messages.append(messages)
    return owner_id

def lookup_owner_record(email):
    global validator, response, debug
    time_mark(debug, "Looking up owner record")
    owner_id, messages = OwnerService.lookup_owner_record(email)
    if len(messages) > 0:
        validator.error = True
        response.status = "error"
        response.messages.append(messages)
    return owner_id

def lookup_meta_record(object_id, object_id_type, meta_id = None):
    return MetadataService.lookup_meta_record(object_id, object_id_type, meta_id)

def insert_lookup_records(meta_records, project_id, batch_id):
    return LookupService.insert_lookup_records(meta_records, project_id, batch_id)

def locate(pattern, root_path):
    for path, files in os.walk(os.path.abspath(root_path)):
        for filename in fnmatch.filter(files, pattern):
            return [os.path.join(path, filename), filename ]

def time_mark(debug, milestone):
    if debug == True:
        logger.log_text("Time mark - " + str(round(time.time() * 1000)) + " - in " + milestone);

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))