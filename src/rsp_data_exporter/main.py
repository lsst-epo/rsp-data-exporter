import os, fnmatch, json, time
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
    from .services import audit_report as AuditReportService
    from .services import manifest_file as ManifestFileService
    from .services import owner as OwnerService
    from .services import project as ProjectService
    from .services import batch as BatchService
    from .services import metadata as MetadataService
    from .services import lookup as LookupService
    from .services import file as FileService
    # from .models.edc_logger import EdcLogger
except:
    try:
        from models.citizen_science.citizen_science_validator import CitizenScienceValidator
        from models.data_exporter_response import DataExporterResponse
        import services.audit_report as AuditReportService
        import services.manifest_file as ManifestFileService
        import services.owner as OwnerService
        import services.project as ProjectService
        import services.batch as BatchService
        import services.metadata as MetadataService
        import services.lookup as LookupService
        import services.file as FileService
        # from models.edc_logger import EdcLogger
    except Exception as e:
        if TEST_ONLY == True:
            logger.log_text("An error occurred while attempting to import subpackages!")
            logger.log_text(e.__str__())        

from flask import Flask, request
from google.cloud import storage

# import services.tabular_data as TabularDataService

app = Flask(__name__)
response = DataExporterResponse()
validator = CitizenScienceValidator()
CLOUD_STORAGE_BUCKET_HIPS2FITS = os.environ['CLOUD_STORAGE_BUCKET_HIPS2FITS']
CLOUD_STORAGE_CIT_SCI_URL_PREFIX = os.environ["CLOUD_STORAGE_CIT_SCI_URL_PREFIX"]
CLOUD_STORAGE_CIT_SCI_PUBLIC = os.environ["CLOUD_STORAGE_CIT_SCI_PUBLIC"]
debug = False

def check_test_only_var():
    return TEST_ONLY

@app.route("/active-batch-metadata")
def get_batch_metadata():
    vendor_project_id = request.args.get("vendor_project_id")
    project_id = lookup_project_record(vendor_project_id)

    # Clean up batches before fetching batch ID
    check_batch_status(project_id, vendor_project_id)

    # Fetch batch ID
    batch_id = BatchService.get_current_active_batch_id(project_id)
    manifest_url = ManifestFileService.lookup_manifest_url(batch_id)

    return json.dumps({
        "metadata_url": manifest_url
    })

@app.route("/citizen-science-ingest-status")
def check_status_of_previously_executed_ingest():
    global response
    guid = request.args.get("guid")
    exists = ManifestFileService.check_if_manifest_file_exists(guid)
    response = DataExporterResponse()
    response.messages = []

    if exists:
        response.status = "success"
        response.manifest_url = f"{CLOUD_STORAGE_CIT_SCI_URL_PREFIX}{CLOUD_STORAGE_CIT_SCI_PUBLIC}/{guid}/manifest.csv"
    else:
        response.status = "error"
        response.messages.append("The job either failed or is still processing, please try again later.")

    res = json.dumps(response.__dict__)
    return res

@app.route("/citizen-science-tabular-ingest")
def download_tabular_data_and_process():
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

    if validator.error is False:
        tabular_records = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS , "manifest.csv", guid, validator.data_rights_approved, True)
        manifest_url = ManifestFileService.upload_manifest(f"/tmp/{guid}/manifest.csv")
        # if data_format == "objects":
        #     TabularDatService.create_dr_objects_records(csv_path)
        # elif data_format == "diaobjects":
        #     TabularDatService.create_dr_diaobject_records(csv_path)
        # elif data_format == "forcedsources":
        #     TabularDatService.create_dr_forcedsource_records(csv_path)
        # else:
        #     logger.log_text("data_format != object!!!")
        # manifest_url = transfer_tabular_manifest(urls, CLOUD_STORAGE_CIT_SCI_PUBLIC, guid)
        # updated_meta_records = update_meta_records_with_user_values(meta_records)
        tabular_meta_records = MetadataService.create_tabular_meta_records(tabular_records)
        meta_records_with_id = MetadataService.insert_meta_records(tabular_meta_records)
        LookupService.insert_lookup_records(meta_records_with_id, validator.project_id, validator.batch_id)
        response.status = "success"
        response.manifest_url = manifest_url
    else:
        response.status = "error"
        if response.messages == None or len(response.messages) == 0:
            response.messages.append("An error occurred while processing the data batch, please try again later.")

    res = json.dumps(response.__dict__)
    time_mark(debug, "Done processing, return response to notebook aspect")
    return res

@app.route("/citizen-science-image-ingest")
def download_image_data_and_process():
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
    
    if validator.error is False:
        # astro cutouts data
        cutouts = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS, f"{guid}.zip", guid, validator.data_rights_approved)

        if validator.error is False:
            urls = upload_cutouts(cutouts)
            meta_records = MetadataService.create_meta_records(urls)

            if validator.error is False:                
                manifest_url = build_and_upload_manifest(urls, CLOUD_STORAGE_CIT_SCI_PUBLIC, guid)
                updated_meta_records = update_meta_records_with_user_values(meta_records)
                meta_records_with_id = insert_meta_records(updated_meta_records)
                LookupService.insert_lookup_records(meta_records_with_id, validator.project_id, validator.batch_id)
                response.status = "success"
                response.manifest_url = manifest_url

                audit_records, audit_messages = insert_audit_records(vendor_project_id)

                if len(audit_records) < len(validator.mapped_manifest):
                    response.messages.append("Some audit records were not inserted!")

                if len(audit_messages) > 0:
                    response.messages = response.messages + audit_messages
    else:
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
        logger.log_text("An exception occurred in fetch_audit_records!")
        logger.log_text(e.__str__())
        response = DataExporterResponse()
        response.status = "ERROR"
        response.messages.append(f"An error occurred while looking up the audit records associated with Zooniverse project ID: {vendor_project_id}")
        return json.dumps(response.__dict__)

def insert_audit_records(vendor_project_id):
    global validator
    vendor_project_id = request.args.get("vendor_project_id")
    try:
        return AuditReportService.insert_audit_records(vendor_project_id, validator.mapped_manifest, validator.owner_id)
    except Exception as e:
        logger.log_text("An exception occurred in insert_audit_records!")
        logger.log_text(e.__str__())
        response = DataExporterResponse()
        response.status = "ERROR"
        response.messages.append(f"An error occurred while looking up the audit records associated with Zooniverse project ID: {vendor_project_id}")
        return json.dumps(response.__dict__)

def update_meta_records_with_user_values(meta_records):
    user_defined_values, info_message = ManifestFileService.update_meta_records_with_user_values(meta_records, validator.mapped_manifest)
    if info_message != "":
        response.messages.append(info_message)
    return user_defined_values

def upload_cutouts(cutouts):
    global debug
    time_mark(debug, "Start of upload...")
    urls = FileService.upload_cutouts(cutouts)
    time_mark(debug, "End of upload...")
    return urls

def download_zip(bucket_name, filename, guid, data_rights_approved, is_tabular_dataset = False):
    global validator
    time_mark(debug, "Start of download zip")
    cutouts, messages = FileService.download_zip(bucket_name, filename, guid, data_rights_approved, is_tabular_dataset)
    time_mark(debug, "Grabbing cutouts finished...")
    if len(messages) > 0:
        validator.error = True
        response.status = "error"
        response.messages.append(messages)
    return cutouts

def transfer_tabular_manifest(bucket, guid):
    global debug, validator
    time_mark(debug, "In transfer tabular manifest")

    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    manifestBlob = bucket.blob(f"{guid}/manifest.csv")
    manifestBlob.upload_from_filename(f"/tmp/{guid}/manifest.csv")
    ManifestFileService.update_batch_record_with_manifest_url(manifestBlob.public_url)
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
#     db.add(edc_logger_record)
#     db.commit()
#     db.close()
#     return

def insert_meta_records(meta_records):
    global validator, response, debug
    time_mark(debug, "Start of insert new meta records")
    meta_records_with_id = MetadataService.insert_meta_records(meta_records)

    meta_enum = validator.RecordType.CITIZEN_SCIENCE_META
    for rec in meta_records:
        print(str(rec.cit_sci_meta_id))
        validator.appendRollback(meta_enum, rec.cit_sci_meta_id)


    return

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

def locate(pattern, root_path):
    for path, files in os.walk(os.path.abspath(root_path)):
        for filename in fnmatch.filter(files, pattern):
            return [os.path.join(path, filename), filename ]

def time_mark(debug, milestone):
    if debug == True:
        logger.log_text("Time mark - " + str(round(time.time() * 1000)) + " - in " + milestone);

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))