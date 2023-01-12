import os, fnmatch, json, subprocess, csv, shutil, time, logging as py_logging, threading, glob
from tokenize import tabsize
from unicodedata import category # for debugging
from google.cloud import logging

TEST_ONLY = bool(os.environ.get('TEST_ONLY'))
logging_client = logging.Client()
log_name = "rsp-data-exporter"
if TEST_ONLY == True:
    log_name = "rsp-data-exporter-tests"
logger = logging_client.logger(log_name)

# Try to import the code relatively, which works fine when running live, but doesn't work for
# running unit tests with Pytest, so accommodate for both scenarios:
try:
    from .models.citizen_science.citizen_science_validator import CitizenScienceValidator
    from .models.data_exporter_response import DataExporterResponse
    from .models.citizen_science.citizen_science_batches import CitizenScienceBatches
    from .models.citizen_science.citizen_science_projects import CitizenScienceProjects
    from .models.citizen_science.citizen_science_owners import CitizenScienceOwners
    from .models.citizen_science.citizen_science_meta import CitizenScienceMeta
    from .models.citizen_science.citizen_science_proj_meta_lookup import CitizenScienceProjMetaLookup
    from .models.data_release.data_release_diaobjects import DataReleaseDiaObjects
    from .models.data_release.data_release_objects import DataReleaseObjects
    from .models.data_release.data_release_forcedsources import DataReleaseForcedSources
    from .models.edc_logger import EdcLogger
except:
    try:
        from models.citizen_science.citizen_science_validator import CitizenScienceValidator
        from models.data_exporter_response import DataExporterResponse
        from models.citizen_science.citizen_science_batches import CitizenScienceBatches
        from models.citizen_science.citizen_science_projects import CitizenScienceProjects
        from models.citizen_science.citizen_science_owners import CitizenScienceOwners
        from models.citizen_science.citizen_science_meta import CitizenScienceMeta
        from models.citizen_science.citizen_science_proj_meta_lookup import CitizenScienceProjMetaLookup
        from models.data_release.data_release_diaobjects import DataReleaseDiaObjects
        from models.data_release.data_release_objects import DataReleaseObjects
        from models.data_release.data_release_forcedsources import DataReleaseForcedSources
        from models.edc_logger import EdcLogger
    except Exception as e:
        if TEST_ONLY == True:
            logger.log_text("An error occurred while attempting to import subpackages!")
            logger.log_text(e.__str__())        

from flask import Flask, request, Response
from google.cloud import storage
import panoptes_client
from panoptes_client import Panoptes, Project, SubjectSet
import sqlalchemy
from sqlalchemy import select, update
import numpy as np
# import lsst.daf.butler as dafButler

app = Flask(__name__)
response = DataExporterResponse()
validator = CitizenScienceValidator()
CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
CLOUD_STORAGE_BUCKET_HIPS2FITS = os.environ['CLOUD_STORAGE_BUCKET_HIPS2FITS']
CLOUD_STORAGE_CIT_SCI_URL_PREFIX = os.environ["CLOUD_STORAGE_CIT_SCI_URL_PREFIX"]
CLOUD_STORAGE_CIT_SCI_PUBLIC = os.environ["CLOUD_STORAGE_CIT_SCI_PUBLIC"]
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
db = None
CLOSED_PROJECT_STATUSES = ["COMPLETE", "CANCELLED", "ABANDONED"]
BAD_OWNER_STATUSES = ["BLOCKED", "DISABLED"]
debug = False
urls = []

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
    batch_id = get_current_active_batch_id(project_id)
    logger.log_text("get_batch_metadata:: batche_id = " + str(batch_id))

    manifest_url = lookup_manifest_url(batch_id)

    return json.dumps({
        "metadata_url": manifest_url
    })

def lookup_manifest_url(batch_id):
    db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
    stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_batch_id == batch_id)
    results = db.execute(stmt)
    record = results.scalars().first()
    return record.manifest_url

def get_current_active_batch_id(project_id):
    db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
    stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_proj_id == project_id).where(CitizenScienceBatches.batch_status == 'ACTIVE')
    results = db.execute(stmt)
    
    logger.log_text("logging dir of results in get_current_active_batch_id:")
    record = results.scalars().first()
    logger.log_text(str(dir(record)))
    logger.log_text("about to log results from within get_current_active_batch_id:")
    
    batch_id = record.cit_sci_batch_id
    logger.log_text(str(batch_id))
    return batch_id

def query_lookup_records(project_id, batch_id):
    db = CitizenScienceProjMetaLookup.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
    query = select(CitizenScienceProjMetaLookup).where(CitizenScienceProjMetaLookup.cit_sci_proj_id == project_id).where(CitizenScienceProjMetaLookup.cit_sci_batch_id == int(batch_id))
    lookup_records = db.execute(query)
    db.commit()
    meta_ids = []
    for row in lookup_records.scalars():
        logger.log_text("looping through lookup results")
        logger.log_text(str(row))
        meta_ids.append(row.cit_sci_meta_id)
    
    return meta_ids

@app.route("/citizen-science-butler-test")
def new_butler_test():
    # email = "erosas@lsst.org" # Add your primary email
    logger.log_text("got into /citizen-science-butler-test!")
    datasetId = "u/erosas@lsst.org/zooniverse-test" # Replace "change-this" with a unique name of your change, leave the leading slash '/'
    repo = 's3://butler-config' # Keep track of this URI for later use with: butler retrieve-artifacts...
    collection = "2.2i/runs/DP0.1"
    logger.log_text("about to set up the butler")
    butler = dafButler.Butler(repo, collections=collection, run=datasetId)
    logger.log_text("about to log butler object:")
    logger.log_text(str(butler.__dict__))
    registry = butler.registry
    logger.log_text("about to log registry object:")
    logger.log_text(str(registry.__dict__))
    logger.log_text("about to queryDatasets()")
    refs = registry.queryDatasets(datasetType="calexp", collections=collection)
    logger.log_text("about to loop through refs and call retrieveArtifacts for only the first ref")
    transferred = butler.retrieveArtifacts(refs, destination='gs://butler-config/data/')

    # for i, ref in enumerate(refs):
    #     logger.log_text("processing...:")
    #     logger.log_text(str(ref))
    #     transferred = butler.retrieveArtifacts(ref, destination='gs://butler-config/data/')
    #     break
    
    logger.log_text("done retrieving artifacts!")
    logger.log_text("about to log transferred object")
    logger.log_text(str(transferred.__dict))
    return json.dumps(transferred)

@app.route("/citizen-science-ingest-status")
def check_status_of_previously_executed_ingest():
    global response
    guid = request.args.get("guid")

    gcs = storage.Client()

    manifest_path = guid + "/manifest.csv"

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    exists = storage.Blob(bucket=bucket, name=manifest_path).exists(gcs)

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

@app.route("/citizen-science-bucket-ingest")
def download_bucket_data_and_process():
    logger.log_text("inside of the current code!!!!")
    global response, validator, debug, urls
    guid = request.args.get("guid")
    email = request.args.get("email")
    vendor_project_id = request.args.get("vendor_project_id")
    vendor_batch_id = request.args.get("vendor_batch_id")
    data_type = request.args.get("data_type")
    debug = bool(request.args.get("debug"))
    data_format = request.args.get("table")
    response = DataExporterResponse()
    response.messages = []
    validator = CitizenScienceValidator()
    urls = []

    time_mark(debug, __name__)
    
    if data_type != None:
        logger.log_text("logging data_type: " + data_type)
    else:
        logger.log_text("data_type is None")
        # Only validate data as part of the the citizen science data pipeline
        validate_project_metadata(email, vendor_project_id, vendor_batch_id) 
    
    logger.log_text("just validated project metadata")
    if validator.error is False:
        logger.log_text("validator.error is False!")
        if data_type != None and data_type == "TABULAR_DATA":
            csv_path = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS , guid + ".zip", guid, data_type)[0]
            # to-do: Upload CSV?
            logger.log_text("about to call upload_csv()")
            url = upload_csv(csv_path)
            logger.log_text("done running upload_csv()")
            if data_format == "objects":
                logger.log_text("data_format == object, about to call create_dr_object_records")
                create_dr_objects_records(csv_path, url)
            elif data_format == "diaobjects":
                logger.log_text("data_format == object, about to call create_dr_diaobject_records")
                create_dr_diaobject_records(csv_path, url)
            elif data_format == "forcedsources":
                logger.log_text("data_format == object, about to call create_dr_forcedsource_records")
                create_dr_forcedsource_records(csv_path, url)
            else:
                logger.log_text("data_format != object!!!")

        else: 
            # astro cutouts data
            cutouts = download_zip(CLOUD_STORAGE_BUCKET_HIPS2FITS, guid + ".zip", guid)
    
            if validator.error is False:
                urls = upload_cutouts(cutouts, vendor_project_id)
                # manifest_url = upload_cutouts(cutouts, vendor_project_id)

                if validator.error is False:
                
                    manifest_url = build_and_upload_manifest(urls, email, "556677", CLOUD_STORAGE_CIT_SCI_PUBLIC, guid)
                    update_meta_record_with_user_values()
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

def update_batch_record_with_manifest_url(manifest_url_p):
    global validator
    try:
        logger.log_text("about to update the manifest URL of the new batch")
        logger.log_text("updating batch ID #" + str(validator.batch_id) + " with manifest URL: " + manifest_url_p)
        db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        db.expire_on_commit = False
        db.execute(update(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_batch_id == validator.batch_id).values(manifest_url=manifest_url_p))
        
        db.commit()
        # db.expunge_all()
        db.close()
        logger.log_text("done updating the batch record with the manifest URL")
        # batchId = citizen_science_batch_record.cit_sci_batch_id
        # logger.log_text("new batch id: " + str(batchId))
    except Exception as e:
        logger.log_text("An exception occurred while attempting to update the batch record with the manifest URL!")
        logger.log_text(e.__str__())
    return

def update_meta_record_with_user_values():
    global validator

    logger.log_text("validator.mapped_manifest: ")
    logger.log_text(str(validator.mapped_manifest))

    for filename in validator.mapped_manifest:
        logger.log_text("looping over validator.mapped_manifest values!!!")
        logger.log_text("validator.mapped_manifest.value: " + str(validator.mapped_manifest[filename]))
        edc_ver_id = validator.mapped_manifest[filename]["external_id"]
        object_id = validator.mapped_manifest[filename]["objectId"]
        user_defined_data = validator.mapped_manifest[filename]

        # Remove fields that are already added to bespoke fields in the table
        del user_defined_data["filename"]
        del user_defined_data["external_id"]
        del user_defined_data["objectId"]
        logger.log_text("row after scrubbing fields: " + str(user_defined_data))

        try:
            logger.log_text("about to attempt to update meta record with user defined values!!!")
            db = CitizenScienceMeta.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
            db.expire_on_commit = False
            db.synchronize_session = False
            stmt = select(CitizenScienceMeta).where(CitizenScienceMeta.uri.like("%" + filename + "%"))
            results = db.execute(stmt)
            for row in results.scalars():                
                row.edc_ver_id = edc_ver_id
                row.source_id = object_id
                row.source_id_type = "object_id"
                row.user_defined_values = str(user_defined_data)
                db.commit()
            
            db.close()
            logger.log_text("done updating meta record with user defined values")
        except Exception as e:
            logger.log_text("An exception occurred while attempting to update the meta record with the user defined values!")
            logger.log_text("logging exception")
            logger.log_text(e.__str__())
            logger.log_text("done logging exception")
            
    return

def upload_csv(csv_path):
    logger.log_text("inside of upload_csv")
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    destination_filename = csv_path.replace("/tmp/", "")
    blob = bucket.blob(destination_filename)
    blob.upload_from_filename(csv_path)

    logger.log_text("logging blob.public_url:")
    logger.log_text(blob.public_url)
    return blob.public_url

def create_dr_forcedsource_records(csv_path, csv_url):
    csv_file = open(csv_path, "rU")
    reader = csv.DictReader(csv_file)
    logger.log_text("about to loop CSV file contents in create_dr_forcedsource_records")
    for row in reader:
        try:
            db = DataReleaseForcedSources.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
            data_release_forcedsource_record = DataReleaseForcedSources(forcedsourceid=row["forcedSourceId"],
                                                                        objectid=row["objectId"],
                                                                        parentobjectid=row["parentObjectId"],
                                                                        coord_ra=row["coord_ra"],
                                                                        coord_dec=row["coord_dec"],
                                                                        skymap=row["skymap"],
                                                                        tract=row["tract"],
                                                                        patch=row["patch"],
                                                                        band=row["band"],
                                                                        ccdvisitid=row["ccdVisitId"],
                                                                        detect_ispatchinner=bool(row["detect_isPatchInner"]),
                                                                        detect_isprimary=bool(row["detect_isPrimary"]),
                                                                        detect_istractinner=bool(row["detect_isTractInner"]),
                                                                        localbackground_instfluxerr=row["localBackground_instFluxErr"],
                                                                        localbackground_instflux=row["localBackground_instFlux"],
                                                                        localphotocaliberr=row["localPhotoCalibErr"],
                                                                        localphotocalib_flag=bool(row["localPhotoCalib_flag"]),
                                                                        localphotocalib=row["localPhotoCalib"],
                                                                        localwcs_cdmatrix_1_1=row["localWcs_CDMatrix_1_1"],
                                                                        localwcs_cdmatrix_1_2=row["localWcs_CDMatrix_1_2"],
                                                                        localwcs_cdmatrix_2_1=row["localWcs_CDMatrix_2_1"],
                                                                        localwcs_cdmatrix_2_2=row["localWcs_CDMatrix_2_2"],
                                                                        localwcs_flag=bool(row["localWcs_flag"]),
                                                                        pixelflags_bad=bool(row["pixelFlags_bad"]),
                                                                        pixelflags_crcenter=bool(row["pixelFlags_crCenter"]),
                                                                        pixelflags_cr=bool(row["pixelFlags_cr"]),
                                                                        pixelflags_edge=bool(row["pixelFlags_edge"]),
                                                                        pixelflags_interpolatedcenter=bool(row["pixelFlags_interpolatedCenter"]),
                                                                        pixelflags_interpolated=bool(row["pixelFlags_interpolated"]),
                                                                        pixelflags_saturatedcenter=bool(row["pixelFlags_saturatedCenter"]),
                                                                        pixelflags_saturated=bool(row["pixelFlags_saturated"]),
                                                                        pixelflags_suspectcenter=bool(row["pixelFlags_suspectCenter"]),
                                                                        pixelflags_suspect=bool(row["pixelFlags_suspect"]),
                                                                        psfdifffluxerr=row["psfDiffFluxErr"],
                                                                        psfdiffflux_flag=bool(row["psfDiffFlux_flag"]),
                                                                        psfdiffflux=row["psfDiffFlux"],
                                                                        psffluxerr=row["psfFluxErr"],
                                                                        psfflux_flag=bool(row["psfFlux_flag"]),
                                                                        psfflux=row["psfFlux"])
            db.add(data_release_forcedsource_record)
            db.commit()
            db.close()
        except Exception as e:
            logger.log_text("an exception occurred in create_dr_forcedsource_records!")
            logger.log_text(e.__str__())
    return

def create_dr_diaobject_records(csv_path, csv_url):
    csv_file = open(csv_path, "rU")
    reader = csv.DictReader(csv_file)
    logger.log_text("about to loop CSV file contents in create_dr_diaobject_records")
    
    for row in reader:
        try:
            db = DataReleaseDiaObjects.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
            data_release_diaobject_record = DataReleaseDiaObjects(ra=row["ra"],
                                                                  decl=row["decl"],
                                                                  rpsfluxchi2=row["rPSFluxChi2"],
                                                                  ipsfluxchi2=row["iPSFluxChi2"],
                                                                  gpsfluxchi2=row["gPSFluxChi2"],
                                                                  upsfluxchi2=row["uPSFluxChi2"],
                                                                  ypsfluxchi2=row["yPSFluxChi2"],
                                                                  zpsfluxchi2=row["zPSFluxChi2"],
                                                                  gpsfluxmax=row["gPSFluxMax"],
                                                                  ipsfluxmax=row["iPSFluxMax"],
                                                                  rpsfluxmax=row["rPSFluxMax"],
                                                                  upsfluxmax=row["uPSFluxMax"],
                                                                  ypsfluxmax=row["yPSFluxMax"],
                                                                  zpsfluxmax=row["zPSFluxMax"],
                                                                  gpsfluxmin=row["gPSFluxMin"],
                                                                  ipsfluxmin=row["iPSFluxMin"],
                                                                  rpsfluxmin=row["rPSFluxMin"],
                                                                  upsfluxmin=row["uPSFluxMin"],
                                                                  ypsfluxmin=row["yPSFluxMin"],
                                                                  zpsfluxmin=row["zPSFluxMin"],
                                                                  gpsfluxmean=row["gPSFluxMean"],
                                                                  ipsfluxmean=row["iPSFluxMean"],
                                                                  rpsfluxmean=row["rPSFluxMean"],
                                                                  upsfluxmean=row["uPSFluxMean"],
                                                                  ypsfluxmean=row["yPSFluxMean"],
                                                                  zpsfluxmean=row["zPSFluxMean"],
                                                                  gpsfluxndata=row["gPSFluxNdata"],
                                                                  ipsfluxndata=row["iPSFluxNdata"],
                                                                  rpsfluxndata=row["rPSFluxNdata"],
                                                                  upsfluxndata=row["uPSFluxNdata"],
                                                                  ypsfluxndata=row["yPSFluxNdata"],
                                                                  zpsfluxndata=row["zPSFluxNdata"])  
            db.add(data_release_diaobject_record)
            db.commit()
            db.close()
        except Exception as e:
            logger.log_text("an exception occurred in create_dr_diaobject_records!")
            logger.log_text(e.__str__())
    return

def create_dr_objects_records(csv_path, csv_url):
    logger.log_text("inside of create_dr_objects_records()")
    csv_file = open(csv_path, "rU")
    reader = csv.DictReader(csv_file)
    logger.log_text("about to loop CSV file contents in create_dr_objects_records")
    for row in reader:
        try:
            db = DataReleaseObjects.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
            data_release_object_record = DataReleaseObjects(objectid=row["objectId"],
                                                            coord_dec=row["coord_dec"],
                                                            coord_ra=row["Coord_ra"],
                                                            g_ra=row["g_ra"],
                                                            i_ra=row["i_ra"],
                                                            r_ra=row["r_ra"],
                                                            u_ra=row["u_ra"],
                                                            y_ra=row["y_ra"],
                                                            z_ra=row["z_ra"],
                                                            g_decl=row["g_decl"],
                                                            i_decl=row["i_decl"],
                                                            r_decl=row["r_decl"],
                                                            u_decl=row["u_decl"],
                                                            y_decl=row["y_decl"],
                                                            z_decl=row["z_decl"],
                                                            g_bdFluxB=row["g_bdFluxB"],
                                                            i_bdFluxB=row["i_bdFluxB"],
                                                            r_bdFluxB=row["r_bdFluxB"],
                                                            u_bdFluxB=row["u_bdFluxB"],
                                                            y_bdFluxB=row["y_bdFluxB"],
                                                            z_bdFluxB=row["z_bdFluxB"],
                                                            g_bdFluxD=row["g_bdFluxD"],
                                                            i_bdFluxD=row["i_bdFluxD"],
                                                            r_bdFluxD=row["r_bdFluxD"],
                                                            u_bdFluxD=row["u_bdFluxD"],
                                                            y_bdFluxD=row["y_bdFluxD"],
                                                            z_bdFluxD=row["z_bdFluxD"],
                                                            g_bdReB=row["g_bdReB"],
                                                            i_bdReB=row["i_bdReB"],
                                                            r_bdReB=row["r_bdReB"],
                                                            u_bdReB=row["u_bdReB"],
                                                            y_bdReB=row["y_bdReB"],
                                                            z_bdReB=row["z_bdReB"],
                                                            g_bdReD=row["g_bdReD"],
                                                            i_bdReD=row["i_bdReD"],
                                                            r_bdReD=row["r_bdReD"],
                                                            u_bdReD=row["u_bdReD"],
                                                            y_bdReD=row["y_bdReD"],
                                                            z_bdReD=row["z_bdReD"])    
            db.add(data_release_object_record)
            db.commit()
            db.close()
        except Exception as e:
            logger.log_text("an exception occurred in create_dr_object_records!")
            logger.log_text(e.__str__())
    return

# Note: plural
def upload_cutouts(cutouts, vendor_project_id):
    global debug, urls

    # Beginning of optimization code
    time_mark(debug, "Start of upload...")
    if len(cutouts) > 500: # Arbitrary threshold for threading
        logger.log_text("inside of the upload_cutouts len(cutouts) IF code block")
        subset_count = round(len(np.array(cutouts)) / 250)
        logger.log_text("subset_count: " + str(subset_count))
        sub_cutouts_arr = np.split(np.array(cutouts), subset_count) # create sub arrays divided by 1k cutouts
        threads = []
        for i, sub_arr in enumerate(sub_cutouts_arr):
            logger.log_text("i : " + str(i));
            t = threading.Thread(target=upload_cutout_arr, args=(sub_arr,str(i),))
            threads.append(t)
            logger.log_text("starting thread #" + str(i))
            threads[i].start()
        
        for thread in threads:
            logger.log_text("joining thread!")
            thread.join()

    else:
        upload_cutout_arr(cutouts, str(1))
    time_mark(debug, "End of upload...")

    time_mark(debug, "Start of upload & inserting of metadata...")
    insert_meta_records(urls, vendor_project_id)
    time_mark(debug, "End of inserting of metadata records")
    return urls

# Note: singular 
def upload_cutout_arr(cutouts, i):
    global urls
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

    return
        
def insert_meta_records(urls, vendor_project_id):
    time_mark(debug, "Start of metadata record insertion")
    meta_ids = []
    for url in urls:
        meta_id = insert_meta_record(url, str(round(time.time() * 1000)) , 'sourceId', vendor_project_id)
        if meta_id > 0:
            meta_ids.append(meta_id)
    time_mark(debug, "End of metadata record insertion")
    return meta_ids

# Accepts the bucket name and filename to download and returns the path of the downloaded file
def download_zip(bucket_name, filename, file = None, data_type = None):
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



    unzipped_cutouts_dir = "/tmp/" + file
    os.mkdir(unzipped_cutouts_dir)
    time_mark(debug, "Start of unzip....")
    logger.log_text("rosas - about to log the /tmp directory contents")
    rosas_test = str(glob.glob("/tmp/*"))
    logger.log_text(rosas_test)


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

    # Deviate logic based on data type
    if data_type != None and data_type == "TABULAR_DATA":
        csv_path = unzipped_cutouts_dir + "/" + files[0]
        logger.log_text("inside of TABULAR_DATA code block")
        # Get CSV file
        csv_file = open(csv_path, "rU")
        reader = csv.reader(csv_file, delimiter=',')
        logger.log_text("about to log CSV file contents")
        for row in reader:
            logger.log_text(str(row))
        logger.log_text("done logging CSV file contents")
        return [csv_path]
    else:
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
        pngs = glob.glob("/tmp/" + file + "/*.png")
        jpegs = glob.glob("/tmp/" + file + "/*.jpeg")
        jpgs = glob.glob("/tmp/" + file + "/*.jpg")
        cutouts = pngs + jpegs + jpgs
        time_mark(debug, "Grabbing cutouts finished...")
        return cutouts

def build_and_upload_manifest(urls, email, sourceId, bucket, guid = ""):
    global debug, validator
    time_mark(debug, "In build and upload manifest")
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    # list to store the names of columns
    column_names = None
    cutout_metadata = {}
    filename_idx = None

    # Read the manifest that came from the RSP and store it in a dict with 
    # the filename as the key
    logger.log_text("about to read the RSP manifest")
    with open('/tmp/' + guid + '/metadata.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
    
        first = True
        # loop to iterate through the rows of csv
        for row in csv_reader:
            
            # adding the first row
            if first == True:
                logger.log_text("about to log headers")
                logger.log_text(str(row))
                column_names = row
                filename_idx = column_names.index("filename")
                column_names[column_names.index("edc_ver_id")] = "external_id"
                first = False
            else:
                # Set new key for row
                filename = row[filename_idx]

                metadata = {}
                c_idx = 0
                for col in row:
                    metadata[column_names[c_idx]] = col
                    c_idx = c_idx + 1
                
                cutout_metadata[filename] = metadata
    validator.mapped_manifest = cutout_metadata

    # loop over urls
    logger.log_text("about to write new manifest file")
    with open('/tmp/' + guid + '/manifest.csv', 'w', newline='') as csvfile:
        column_names.append("location:1")
        writer = csv.DictWriter(csvfile, fieldnames=column_names)

        writer.writeheader()

        for url in urls:
            url_list = url.split("/")
            filename = url_list[len(url_list)-1]

            if filename in cutout_metadata:
                csv_row = cutout_metadata[filename]
                csv_row["location:1"] = url
                writer.writerow(csv_row)

    logger.log_text("done writing new manifest")
    
    manifestBlob = bucket.blob(guid + "/manifest.csv")
    

    logger.log_text("about to upload the new manifest to GCS")
    manifestBlob.upload_from_filename("/tmp/" + guid + "/manifest.csv")
    update_batch_record_with_manifest_url(manifestBlob.public_url)
    return manifestBlob.public_url

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
        batch_ids = check_batch_status(project_id, vendor_project_id) # To-do: Look into whether or not this would be a good time to check with Zoony on the status of the batches

        if len(batch_ids) == 0:
            # Create new batch record
            batchId = create_new_batch(project_id, vendor_batch_id)

            if(batchId > 0):
                validator.batch_id = batchId
                return True
            else:
                return False
        else:
            logger.log_text("about to check if create_edc_logger_record() needs to be called")
            if validator.log_to_edc:
                logger.log_text("calling! create_edc_logger_record()")
                create_edc_logger_record()
            validator.error = True
            response.messages.append("You currently have an active batch of data on the Zooniverse platform and cannot create a new batch until the current batch has been completed.")
            return False
    else:
        return False

def create_edc_logger_record():
    db = EdcLogger.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
    notes_obj = json.loads(validator.edc_logger_notes)

    edc_logger_record = EdcLogger(application_name="rsp-data-exporter", run_id=notes_obj.vendor_batch_id, notes=validator.edc_logger_notes, category=validator.edc_logger_category)
    logger.log_text("about to commit edc-logger record")
    db.add(edc_logger_record)
    db.commit()
    db.close()
    logger.log_text("committed edc-logger record!")
    return

# To-do: Add vendor_batch_id to workflow of this function/route
@app.route("/citizen-science-butler-ingest")
def butler_retrieve_data_and_upload():
    global db
    db = init_connection_engine()
    email = request.args.get('email')
    collection = request.args.get('collection')
    source_id = request.args.get("sourceId")
    vendor_project_id = request.args.get("vendorProjectId")
    # output = subprocess.run(['sh', '/opt/lsst/software/server/run.sh', email, collection], stdout=subprocess.PIPE).stdout.decode('utf-8')

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
    global validator, response, debug
    time_mark(debug, "Start of create new batch")
    batchId = -1;
    try:
        db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        db.expire_on_commit = False
        citizen_science_batch_record = CitizenScienceBatches(cit_sci_proj_id=project_id, vendor_batch_id=vendor_batch_id, batch_status='ACTIVE')    
        db.add(citizen_science_batch_record)
        
        db.commit()
        db.expunge_all()
        db.close()
        batchId = citizen_science_batch_record.cit_sci_batch_id
        logger.log_text("new batch id: " + str(batchId))
    except Exception as e:
        logger.log_text("An exception occurred while trying to create a new batch!:")
        logger.log_text("Exception text: " + e.__str__())
        logger.log_text("Exception text: " + str(e))
        logger.log_text("End of exception logging")
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new data batch record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return batchId

def check_batch_status(project_id, vendor_project_id):
    # First, look up batches in the database, which may 
    global validator, debug
    time_mark(debug, "Start of check batch status!!!")
    logger.log_text("inside of check_batch_status, logging project_id : " + str(project_id))
    batches_still_active = []
    batches_not_found_in_zooniverse = []

    try:
        db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_proj_id == project_id).where(CitizenScienceBatches.batch_status == 'ACTIVE')
        results = db.execute(stmt)
        
        # batch_record = None
        batches_in_db = []
        for row in results.scalars():
            batches_in_db.append({
                "batch_record" : row,
                "batch_id" : row.cit_sci_batch_id,
                "vendor_batch_id_db" : row.vendor_batch_id
            })

        db.commit()

        logger.log_text("about to evaluate batch_id after checking the DB")
        if len(batches_in_db) > 0 and TEST_ONLY == False:
            logger.log_text("# of active batches found in DB: " + str(len(batches_in_db)))
            # Call the Zooniverse API to get all subject sets for the project
            project = Project.find(int(vendor_project_id))

            logger.log_text("about to log project:")
            logger.log_text(str(project.raw))

            subject_set_list = list(project.links.subject_sets)
            
            for batch_in_db in batches_in_db:
                update_batch_record = False
                logger.log_text("about to process batch_id")
                logger.log_text(str(batch_in_db["batch_id"]))
                logger.log_text("about to process vendor_batch_id_db")
                logger.log_text(str(batch_in_db["vendor_batch_id_db"]))

                if len(subject_set_list) == 0:
                    logger.log_text("the length of project.links.subject_sets is 0!")
                    update_batch_record = True
                else:
                    logger.log_text("the length of project.links.subject_sets is > 0! : " + str(len(list(project.links.subject_sets))))
                    found_subject_set = False
                    for sub in subject_set_list:
                        try:
                            logger.log_text("looping through project.links.subject_sets")
                            if str(batch_in_db["vendor_batch_id_db"]) == sub.id:
                                logger.log_text("Found the subject set in question!")
                                found_subject_set = True
                                # Zooniverse has a weird way of tracking subject sets, subject sets that have not been
                                # added to a workflow have a completeness of: {}
                                if len(sub.completeness) == 0:
                                    logger.log_text("The subject set hasn't been assigned to a workflow/hasn't been worked on!!!")
                                    update_batch_record = False
                                    batches_still_active.append(sub.id)
                                    break
                                else:
                                    # The subject set HAS been started, worked on, so evaluate if it is complete
                                    for completeness_key in sub.completeness:
                                        if sub.completeness[completeness_key] == 1.0:
                                            logger.log_text("subject set IS COMPLETE!!")
                                            update_batch_record = True
                                            break
                                        else:
                                            # Found the batch, but it's not complete, check if it contains subjects or not
                                            try:
                                                logger.log_text("subject set is NOT complete!!")
                                                first = next(subject_set_list[0].subjects)
                                                if first is not None:
                                                    # Active batch with subjects, return
                                                    logger.log_text("first is NOT None!!!")
                                                    batches_still_active.append(sub.id)
                                                    update_batch_record = False
                                                    break
                                                else:
                                                    logger.log_text("Erroneous caching of Zooniverse client, updating EDC database");
                                                    update_batch_record = True
                                                    break
                                            except StopIteration:
                                                logger.log_text("setting validator.error to True!")
                                                validator.log_to_edc = True
                                                validator.edc_logger_category = "BATCH_LOOKUP"
                                                logger_notes = {
                                                    "project_id" : project_id,
                                                    "batch_id" : batch_in_db["batch_id"],
                                                    "vendor_project_id" : vendor_project_id,
                                                    "vendor_batch_id" : batch_in_db["vendor_batch_id_db"]
                                                }
                                                validator.edc_logger_notes = json.dumps(logger_notes)
                                                response.status = "error"
                                                response.messages.append("You have an active, but empty subject set on the zooniverse platform with an ID of " + str(batch_in_db["vendor_batch_id_db"]) + ". Please delete this subject set on the Zoonivese platform and try again.")
                                                continue
                        except Exception as e:
                            logger.log_text("An error occurred while looping through the subject sets, this usually occurs because of stale data that has been cached by Zooniverse. ")
                            validator.log_to_edc = True
                            validator.edc_logger_category = "BATCH_LOOKUP"
                            validator.edc_logger_notes = str(e)
                            response.status = "error"
                            continue

                    if found_subject_set == False:
                        logger.log_text("The subject set in question was NEVER found!")
                        batches_not_found_in_zooniverse.append(str(batch_in_db["vendor_batch_id_db"]))
                        update_batch_record = True

                if update_batch_record == True:
                    logger.log_text("about to update EDC batch ID " + str(batch_in_db["batch_id"]) + ", vendor batch ID: " + str(batch_in_db["vendor_batch_id_db"]) + " in the DB!")
                    batch_in_db["batch_record"].batch_status = "COMPLETE"
                    db.commit()
        elif TEST_ONLY == True:
            for batch in batches_in_db:
                batches_still_active.append({
                    "batch_record" : batch})

    except Exception as e:
        logger.log_text("about to log exception in check_batch_status!")
        logger.log_text(e.__str__())
        logger.log_text(py_logging.exception("message"))
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to lookup your batch records - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    db.close()

    logger.log_text("batches found in the DB, but not in Zooniverse:")
    logger.log_text(str(batches_not_found_in_zooniverse))
    return batches_still_active

def create_new_project_record(ownerId, vendorProjectId):
    global validator, response, debug
    time_mark(debug, "Start of create new project")
    project_id = None
    try:
        logger.log_text("about to create new project record!")
        db = CitizenScienceProjects.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        citizen_science_project_record = CitizenScienceProjects(vendor_project_id=vendorProjectId, owner_id=ownerId, project_status='ACTIVE', excess_data_exception=False, data_rights_approved=False)
        db.add(citizen_science_project_record)
        db.commit()
        project_id = citizen_science_project_record.cit_sci_proj_id


    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text("An exception occurred while creating a new project record")
        logger.log_text(e.__str__())

    logger.log_text("about to return from create_new_project_record")
    return project_id

def lookup_project_record(vendorProjectId):
    global response, validator, debug
    time_mark(debug, "Start of lookup project record")
    project_id = None
    status = None

    logger.log_text("logging vendorProjectId:")
    logger.log_text(vendorProjectId)

    try:
        db = CitizenScienceProjects.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceProjects).where(CitizenScienceProjects.vendor_project_id == int(vendorProjectId))

        logger.log_text("about to execute query in lookup_project_record")
        results = db.execute(stmt)

        logger.log_text("about to loop through results")
        for row in results.scalars():
            logger.log_text("in a result in the loop!")
            status = row.project_status
            validator.data_rights_approved = row.data_rights_approved

            logger.log_text("about to check project status")
            if status in CLOSED_PROJECT_STATUSES:
                logger.log_text("project status in bad status!!!")
                response.status = "error"
                validator.error = True
                response.messages.append("This project is in a status of " + status + " - either create a new project or contact Rubin to request for the project to be reopened.")
            else:
                logger.log_text("project status is in a good place")
                project_id = row.cit_sci_proj_id
        db.close()
    except Exception as e:
        validator.error = True
        response.status = "error"
        logger.log_text("an exception occurred in lookup_project_record")
        response.messages.append("An error occurred while attempting to lookup your project record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text(e.__str__())

    logger.log_text("about to return project_id in lookup_project_record")
    return project_id

def create_new_owner_record(email):
    global validator, response, debug
    time_mark(debug, "Start of create new owner")

    owner_id = None;
    try:
        logger.log_text("about to insert new owner record")
        db = CitizenScienceOwners.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        citizen_science_owner_record = CitizenScienceOwners(email=email, status='ACTIVE')
        db.add(citizen_science_owner_record)
        db.commit()
        owner_id = citizen_science_owner_record.cit_sci_owner_id
    except Exception as e:
        validator.error = True
        response.status = "error"
        logger.log_text(e.__str__())
        response.messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return owner_id

def lookup_owner_record(emailP):
    global validator, response, debug
    time_mark(debug, "Looking up owner record")
    ownerId = None
    status = ""

    try:
        db = CitizenScienceOwners.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceOwners).where(CitizenScienceOwners.email == emailP)

        results = db.execute(stmt)
        for row in results.scalars():
            ownerId = row.cit_sci_owner_id
            status = row.status
            break

        if status in BAD_OWNER_STATUSES:
            validator.error = True
            response.status = "error"
            response.messages.append("You are not/no longer eligible to use the Rubin Science Platform to send data to Zooniverse.")

        db.close()
    except Exception as e:
        validator.error = True
        response.status = "error"
        response.messages.append("An error occurred while looking up your projects owner record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text(e.__str__())
   
    return ownerId

def lookup_meta_record(sourceId, sourceIdType, meta_id = None):
    meta_records = []
    metaId = None
    try:
        if meta_id == None:
            db = CitizenScienceMeta.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
            stmt = select(CitizenScienceMeta).where(CitizenScienceMeta.source_id == sourceId).where(CitizenScienceMeta.source_id_type == sourceIdType)
            results = db.execute(stmt)
            for row in results.scalars():
                metaId = row.cit_sci_meta_id

            db.close()

            logger.log_text("about to log metaId (queried via sourceId/sourceIdType) in lookup_meta_record()")
            logger.log_text(metaId)
        else:
            db = CitizenScienceMeta.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
            stmt = select(CitizenScienceMeta).where(CitizenScienceMeta.cit_sci_meta_id == meta_id)
            results = db.execute(stmt)
            for row in results.scalars():
                meta_records.append({
                    "edc_ver_id": row.edc_ver_id,
                    "source_id": row.source_id,
                    "source_id_type": row.source_id_type,
                    "cutout_url": row.uri,
                    "date_transferred": str(row.date_created)
                })

            db.close()

            logger.log_text("about to log meta record count (queried by batch_id) in lookup_meta_record()")
            logger.log_text(str(len(meta_records)))
            return meta_records
    except Exception as e:
        print("an exception occurred in lookup_meta_record!")
        print(e.__str__())
        logger.log_text(e.__str__())
        return e
   
    return metaId

def insert_meta_record(uri, sourceId, sourceIdType, projectId):
    global debug, validator

    # Temp hardcoded variables
    edcVerId = round(time.time() * 1000)
    public = True

    metaRecordId = None;
    try:
        db = CitizenScienceMeta.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        db.expire_on_commit = False
        citizen_science_meta_record = CitizenScienceMeta(edc_ver_id=edcVerId, source_id=sourceId, source_id_type=sourceIdType, uri=uri, public=public)
        db.add(citizen_science_meta_record)
        db.commit()
        
        metaRecordId = citizen_science_meta_record.cit_sci_meta_id 
        logger.log_text("About to call insert_lookup_record() the usual way in the try block")
        errorOccurred = insert_lookup_record(metaRecordId, validator.project_id, validator.batch_id)
        logger.log_text("errorOccurred:")
        logger.log_text(str(errorOccurred))
        logger.log_text("about to log metaRecordId")
        logger.log_text(str(metaRecordId))
        db.expunge_all()
        db.close()
        print("about to print citizen_science_meta_record:")
        print(str(citizen_science_meta_record.__dict__))
        print("1. metaRecordId: " + str(metaRecordId))
        print(type(metaRecordId))

    except Exception as e:
        logger.log_text("An exception occurred in insert_meta_record()")
        print("about to log validator.project_id: " + str(validator.project_id))
        print("about to log validator.batch_id: " + str(validator.batch_id))
        # Is the exception because of a duplicate key error? If so, lookup the ID of the meta record and perform the insert into the lookup table
        # if "non_dup_records" in e.__str__():
        #     logger.log_text("non_dup_record!")
        #     metaId = lookup_meta_record(sourceId, sourceIdType)
        #     return insert_lookup_record(metaId, validator.project_id, validator.batch_id)
        # else:
        #     logger.log_text(e.__str__())
        #     logger.log_text("NOT! non_dup_record!")
        metaRecordId = -1
    
    print("2. metaRecordId: " + str(metaRecordId))
    return metaRecordId

def insert_lookup_record(metaRecordId, projectId, batchId):
    logger.log_text("About to insert lookup record")
    try:
        db_l = CitizenScienceProjMetaLookup.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        db_l.expire_on_commit = False
        citizen_science_proj_meta_lookup_record = CitizenScienceProjMetaLookup(cit_sci_proj_id=projectId, cit_sci_meta_id=metaRecordId, cit_sci_batch_id=batchId)
        db_l.add(citizen_science_proj_meta_lookup_record)
        db_l.commit()
        db_l.close()
    except Exception as e:
        print("exception in insert_lookup_record()!")
        print(e.__str__())
        print("validator.batch_id: " + str(validator.batch_id))
        print("validator.project_id: " + str(validator.project_id))
        logger.log_text("An exception occurred while trying to insert lookup record!!")
        logger.log_text(e.__str__())
        return False
        
    return True

def locate(pattern, root_path):
    for path, dirs, files in os.walk(os.path.abspath(root_path)):
        for filename in fnmatch.filter(files, pattern):
            return [os.path.join(path, filename), filename ]

def init_connection_engine():
    db_config = {
        # [START cloud_sql_postgres_sqlalchemy_limit]
        # Pool size is the maximum number of permanent connections to keep.
        "pool_size": 10,
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
        logger.log_text("Time mark - " + str(round(time.time() * 1000)) + " - in " + milestone);

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))