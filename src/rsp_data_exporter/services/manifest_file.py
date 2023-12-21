import os, csv, time
from google.cloud import logging
from google.cloud import storage
from sqlalchemy import select, update

try:
    from ..models.citizen_science.citizen_science_batches import CitizenScienceBatches
except:
    try:
        from models.citizen_science.citizen_science_batches import CitizenScienceBatches
    except:
        pass

logging_client = logging.Client()
log_name = "rsp-data-exporter.audit_service"
logger = logging_client.logger(log_name)

VALID_OBJECT_ID_TYPES = ["DIRECT", "INDIRECT"]
CLOUD_STORAGE_CIT_SCI_PUBLIC = os.environ["CLOUD_STORAGE_CIT_SCI_PUBLIC"]

DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

def check_if_manifest_file_exists(guid):
    gcs = storage.Client()
    manifest_path = guid + "/manifest.csv"

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    return storage.Blob(bucket=bucket, name=manifest_path).exists(gcs)

def lookup_manifest_url(batch_id):
    db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
    stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_batch_id == batch_id)
    results = db.execute(stmt)
    record = results.scalars().first()
    return record.manifest_url

def update_batch_record_with_manifest_url(manifest_url_p, batch_id):
    try:
        logger.log_text("about to update the manifest URL of the new batch")
        logger.log_text("updating batch ID #" + str(batch_id) + " with manifest URL: " + manifest_url_p)
        db = CitizenScienceBatches.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        db.expire_on_commit = False
        db.execute(update(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_batch_id == batch_id).values(manifest_url=manifest_url_p))
        
        db.commit()
        db.close()
        logger.log_text("done updating the batch record with the manifest URL")
    except Exception as e:
        logger.log_text("An` exception occurred while attempting to update the batch record with the manifest URL!")
        logger.log_text(e.__str__())
    return

def update_meta_records_with_user_values(meta_records, mapped_manifest):

    logger.log_text("mapped_manifest: ")
    logger.log_text(str(mapped_manifest))
    info_message = ""

    logged_obj_type_msg = False
    for record in meta_records:
        filename = record.uri[record.uri.rfind("/") + 1:]
        try:
            user_defined_data = mapped_manifest[filename]
            edc_ver_id = mapped_manifest[filename]["external_id"]

            object_id = None
            if "objectId" in mapped_manifest[filename]:
                object_id = mapped_manifest[filename]["objectId"]

            object_id_type = None
            if "objectIdType" in mapped_manifest[filename]:
                object_id_type = mapped_manifest[filename]["objectIdType"]
                object_id_type = object_id_type.upper()
                del user_defined_data["objectIdType"]

            ra = None
            if "coord_ra" in mapped_manifest[filename]:
                ra = mapped_manifest[filename]["coord_ra"]
                del user_defined_data["coord_ra"]

            dec = None
            if "coord_dec" in mapped_manifest[filename]:
                dec = mapped_manifest[filename]["coord_dec"]
                del user_defined_data["coord_dec"]
                
            del user_defined_data["filename"]
            del user_defined_data["external_id"]

            # The only valid values for objectIdType are DIRECT and INDIRECT, so set all
            # values to INDIRECT if the come in the request as neither
            if object_id_type is not None and object_id_type.upper() not in VALID_OBJECT_ID_TYPES and logged_obj_type_msg == False:
                object_id_type = "INDIRECT"
                info_message = "You sent a manifest file with at least one objectIdType value that was neither 'DIRECT' or 'INDIRECT' (the only values allowed for object ID type). The value was automatically replaced with a value of 'INDIRECT'."
                logged_obj_type_msg = True

            record.set_fields(edc_ver_id=edc_ver_id, object_id=object_id, object_id_type=object_id_type, user_defined_values=str(user_defined_data), ra=ra, dec=dec)
        except Exception as e:
            logger.log_text(e.__str__())
            logger.log_text(f"SKIPPING: {filename} in update_meta_records_with_user_values()") 
    return meta_records, info_message

def build_and_upload_manifest(urls, bucket, batch_id, guid = ""):
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    # list to store the names of columns
    column_names = []
    cutout_metadata = {}
    filename_idx = None

    # Read the manifest that came from the RSP and store it in a dict with 
    # the filename as the key
    logger.log_text("about to read the RSP manifest")
    with open('/tmp/' + guid + '/manifest.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        has_flipbook_columns = False # by default
    
        first = True
        # loop to iterate through the rows of csv
        for row in csv_reader:
            
            # adding the first row
            if first == True:
                column_names += row
                
                # Add edc_ver_id as external_id column header
                column_names.append("external_id")

                filename_idx = column_names.index("filename")

                if "location:image_0" in row and "location:image_1" in row: # has two images at a minimum
                    has_flipbook_columns = True
                else:
                    # Add URL column header
                    column_names.append("location:1")
                first = False
            else:
                # Set new key for row
                filename = row[filename_idx]

                metadata = {}
                c_idx = 0
                for col in row:
                    metadata[column_names[c_idx]] = col
                    c_idx = c_idx + 1
                
                # Add the edc_ver_id
                logger.log_text(f"logging edc_ver_id for filename: {filename}")
                edc_ver_id = round(time.time() * 1000) + 1
                logger.log_text(f"edc_ver_id: {edc_ver_id}")
                metadata["edc_ver_id"] = edc_ver_id

                # Map metadata row to filename key
                cutout_metadata[filename] = metadata

    # loop over urls
    logger.log_text("about to write new manifest file")
    if has_flipbook_columns == True:
        column_names.remove("filename")

    with open('/tmp/' + guid + '/manifest.csv', 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()

        for url in urls:
            
            url_list = url.split("/")
            filename = url_list.pop()

            if filename in cutout_metadata:
                csv_row = cutout_metadata[filename]
                csv_row["external_id"] = cutout_metadata[filename]["edc_ver_id"]
                csv_row.pop("edc_ver_id")
                
                if has_flipbook_columns == True:
                    for col in column_names:
                        if "location:image_" in col:
                            csv_row[col] = '/'.join(url_list) + "/" + csv_row[col]
                    del csv_row["filename"]
                else:
                    csv_row["location:1"] = url

                writer.writerow(csv_row)
    
    manifestBlob = bucket.blob(guid + "/manifest.csv")
    logger.log_text("about to upload the new manifest to GCS")
    manifestBlob.upload_from_filename("/tmp/" + guid + "/manifest.csv")
    update_batch_record_with_manifest_url(manifestBlob.public_url, batch_id)
    return manifestBlob.public_url, cutout_metadata