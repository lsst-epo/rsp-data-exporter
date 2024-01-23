import os, csv, time
from google.cloud import logging
from google.cloud import storage
from sqlalchemy import select, update
from . import db as DatabaseService

try:
    from ..models.citizen_science.citizen_science_batches import CitizenScienceBatches
except:
    try:
        from models.citizen_science.citizen_science_batches import CitizenScienceBatches
    except:
        pass

logging_client = logging.Client()
log_name = "rsp-data-exporter.manifest_service"
logger = logging_client.logger(log_name)

VALID_OBJECT_ID_TYPES = ["DIRECT", "INDIRECT"]
CLOUD_STORAGE_CIT_SCI_PUBLIC = os.environ["CLOUD_STORAGE_CIT_SCI_PUBLIC"]

def check_if_manifest_file_exists(guid):
    gcs = storage.Client()
    manifest_path = f"{guid}/manifest.csv"

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    return storage.Blob(bucket=bucket, name=manifest_path).exists(gcs)

def lookup_manifest_url(batch_id):
    db = DatabaseService.get_db_connection()
    stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_batch_id == batch_id)
    results = db.execute(stmt)
    record = results.scalars().first()
    return record.manifest_url

def update_batch_record_with_manifest_url(manifest_url_p, batch_id):
    try:
        db = DatabaseService.get_db_connection()
        db.expire_on_commit = False
        db.execute(update(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_batch_id == batch_id).values(manifest_url=manifest_url_p))
        
        db.commit()
        db.close()
    except Exception as e:
        logger.log_text("An` exception occurred while attempting to update the batch record with the manifest URL!")
        logger.log_text(e.__str__())
    return

def update_meta_records_with_user_values(meta_records, mapped_manifest):
    info_message = ""
    logged_obj_type_msg = False
    for record in meta_records:
        filename = record.uri[record.uri.rfind("/") + 1:]
        try:
            if filename in mapped_manifest:
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
                    
                if "filename" in user_defined_data:
                    del user_defined_data["filename"]
                del user_defined_data["external_id"]

                # The only valid values for objectIdType are DIRECT and INDIRECT, so set all
                # values to INDIRECT if the come in the request as neither
                if object_id_type is not None and object_id_type.upper() not in VALID_OBJECT_ID_TYPES and logged_obj_type_msg == False:
                    object_id_type = "INDIRECT"
                    info_message = "You sent a manifest file with at least one objectIdType value that was neither 'DIRECT' or 'INDIRECT' (the only values allowed for object ID type). The value was automatically replaced with a value of 'INDIRECT'."
                    logged_obj_type_msg = True

                record.set_fields(edc_ver_id=edc_ver_id, object_id=object_id, object_id_type=object_id_type, user_defined_values=str(user_defined_data), ra=ra, dec=dec)
            else:
                logger.log_text(f"SKIPPING: {filename} in update_meta_records_with_user_values() due to not being a key in the mapped_manifest!!")
        except Exception as e:
            logger.log_text(str(e))
            logger.log_text(f"SKIPPING: {filename} in update_meta_records_with_user_values() due to exception!")

    return meta_records, info_message

def build_and_upload_manifest(urls, bucket, batch_id, guid = ""):
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    # list to store the names of columns
    column_names = []
    cutout_metadata = {}
    upload_manifest = {}
    filename_idx = None
    location_cols = []

    # Read the manifest that came from the RSP and store it in a dict with 
    # the filename as the key
    with open(f"/tmp/{guid}/manifest.csv", 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')
        has_flipbook_columns = False # by default
    
        # loop to iterate through the rows of csv
        for idx, row in enumerate(csv_reader):
            if idx == 0:
                # adding the first row
                column_names += row
                
                # Add edc_ver_id as external_id column header
                column_names.append("external_id")

                if "location:image_0" in row and "location:image_1" in row: # has two images at a minimum
                    has_flipbook_columns = True
                    location_cols = filter(lambda col: ("location:" in col), column_names)

                else:
                    # Add URL column header
                    column_names.append("location:1")
                    filename_idx = column_names.index("filename")
            else:
                if has_flipbook_columns == True:
                    metadata = {}
                    for loc in location_cols:
                        # filename
                        filename_idx = column_names.index(loc)
                        filename = row[filename_idx].split("/").pop()
                        cutout_metadata[filename] = {}
                        cutout_metadata[filename]["location:1"] = row[filename_idx]
                        # object ID
                        obj_id_col_num = loc.replace("location:image_", "")
                        obj_id_idx = column_names.index(f"objectId:image_{obj_id_col_num}")
                        cutout_metadata[filename]["objectId"] = row[obj_id_idx]
                        cutout_metadata[filename]["objectIdType"] = "DIRECT"
                        # EDC ver ID
                        edc_ver_id = round(time.time() * 1000) + 1
                        cutout_metadata[filename]["external_id"] = edc_ver_id

                    for c_idx, col in enumerate(row):
                        if "location:image_" not in column_names[c_idx] and "objectId:image_" not in column_names[c_idx]:
                            cutout_metadata[filename][column_names[c_idx]] = col

                filename_idx = column_names.index("filename")
                filename = row[filename_idx]

                metadata = {}
                for c_idx, col in enumerate(row):
                    metadata[column_names[c_idx]] = col
                
                # Add the edc_ver_id
                edc_ver_id = round(time.time() * 1000) + 1
                metadata["edc_ver_id"] = edc_ver_id

                # Map metadata row to filename key
                upload_manifest[filename] = metadata

    # loop over urls
    if has_flipbook_columns == True:
        column_names.remove("filename")
        if "objectId" not in column_names:
            column_names.append("objectId")
        if "objectIdType" not in column_names:
            column_names.append("objectIdType")

    with open(f"/tmp/{guid}/manifest.csv", 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()

        for url in urls:
            url_list = url.split("/")
            filename = url_list.pop()

            if filename in upload_manifest:
                csv_row = upload_manifest[filename]
                csv_row["external_id"] = upload_manifest[filename]["edc_ver_id"]
                csv_row.pop("edc_ver_id")
                
                if has_flipbook_columns == True:
                    for col in column_names:
                        if "location:image_" in col:
                            csv_row[col] = f"{'/'.join(url_list)}/{csv_row[col]}"
                            obj_id_col_num = col.replace("location:image_", "")
                            csv_row["objectId"] = upload_manifest[filename][f"objectId:image_{obj_id_col_num}"]
                            csv_row["objectIdType"] = "DIRECT"
                    del csv_row["filename"]
                else:
                    csv_row["location:1"] = url

                writer.writerow(csv_row)

    manifestBlob = bucket.blob(f"{guid}/manifest.csv")
    manifestBlob.upload_from_filename(f"/tmp/{guid}/manifest.csv")
    update_batch_record_with_manifest_url(manifestBlob.public_url, batch_id)
    return manifestBlob.public_url, cutout_metadata

def upload_manifest(csv_path):
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    destination_filename = csv_path.replace("/tmp/", "")
    blob = bucket.blob(destination_filename)
    blob.upload_from_filename(csv_path)

    return blob.public_url