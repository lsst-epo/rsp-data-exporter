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
                user_defined_data = mapped_manifest[filename]["user_defined_values"]
                edc_ver_id = mapped_manifest[filename]["edc_ver_id"]

                source_id_type = "objectId"
                object_id = None
                source_id = None
                if any(key.startswith("diaObjectId") for key in mapped_manifest[filename]):
                    source_id_type = "diaObjectId"
                    source_id = mapped_manifest[filename]["diaObjectId"]
                elif any(key.startswith("objectId") for key in mapped_manifest[filename]):
                    object_id = mapped_manifest[filename]["objectId"]

                object_id_type = None
                if "objectIdType" in mapped_manifest[filename]:
                    object_id_type = mapped_manifest[filename]["objectIdType"]
                    object_id_type = object_id_type.upper()
                    if "objectIdType" in user_defined_data:
                        del user_defined_data["objectIdType"]

                ra = None
                if "coord_ra" in mapped_manifest[filename]:
                    ra = mapped_manifest[filename]["coord_ra"]
                    if "coord_ra" in user_defined_data:
                        del user_defined_data["coord_ra"]

                dec = None
                if "coord_dec" in mapped_manifest[filename]:
                    dec = mapped_manifest[filename]["coord_dec"]
                    if "coord_dec" in user_defined_data:
                        del user_defined_data["coord_dec"]
                    
                if "filename" in user_defined_data:
                    del user_defined_data["filename"]

                # The only valid values for objectIdType are DIRECT and INDIRECT, so set all
                # values to INDIRECT if the come in the request as neither
                if object_id_type is not None and object_id_type.upper() not in VALID_OBJECT_ID_TYPES and logged_obj_type_msg == False:
                    object_id_type = "INDIRECT"
                    info_message = "You sent a manifest file with at least one objectIdType value that was neither 'DIRECT' or 'INDIRECT' (the only values allowed for object ID type). The value was automatically replaced with a value of 'INDIRECT'."
                    logged_obj_type_msg = True

                record.set_fields(edc_ver_id=edc_ver_id, object_id=object_id, object_id_type=object_id_type, source_id=source_id, source_id_type=source_id_type, user_defined_values=str(user_defined_data), ra=ra, dec=dec)
            else:
                logger.log_text(f"SKIPPING: {filename} in update_meta_records_with_user_values() due to not being a key in the mapped_manifest!!")
        except Exception as e:
            logger.log_text(str(e))
            logger.log_text(f"SKIPPING: {filename} in update_meta_records_with_user_values() due to exception!")

    return meta_records, info_message

def build_and_upload_manifest(urls, bucket, batch_id, guid = "", flipbook = False):

    if flipbook == True:
        logger.log_text("about to process flipbook manifest file")
        return build_and_upload_manifest_for_flipbook(urls, bucket, batch_id, guid )
    else:
        return build_and_upload_manifest_for_static_cutouts(urls, bucket, batch_id, guid)

def build_and_upload_manifest_for_flipbook(urls, bucket, batch_id, guid):
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    # list to store the names of columns
    column_names = []
    mapped_manifest = {}
    upload_manifest = []
    filename_idx = None
    location_cols = []
    canonical_cols = ["objectId", "diaObjectId", "objectIdType", "diaObjectIdType", "edc_ver_id"]

    # Read the manifest that came from the RSP and store it in a dict with 
    # the filename as the key
    with open(f"/tmp/{guid}/manifest.csv", 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')

        # loop to iterate through the rows of csv
        edc_ver_id = round(time.time() * 1000) 
        csv_rows = enumerate(csv_reader)
        column_names = next(csv_rows)[1]
        column_names.append("external_id")
        upload_manifest.append(column_names)
        location_cols = filter(lambda col: ("location:" in col), column_names)

        for idx, row in csv_rows:
            
            updated_csv_row = row
            
            for idx_l, loc in enumerate(location_cols):
                filename_idx = column_names.index(loc)
                filename = row[filename_idx].split("/").pop()
                mapped_manifest[filename] = {}
                location_suffix = loc.removeprefix('location:image_')

                url_idx = [n for n, x in enumerate(urls) if filename in x]
                url = urls[url_idx[0]]
                mapped_manifest[filename].update(dict(zip(column_names, row)))
                mapped_manifest[filename][f"location:image_{str(idx_l)}"] = url
                mapped_manifest[filename]["edc_ver_id"] = edc_ver_id
                # mapped_manifest[filename].pop("external_id")

                # Clean up dict
                mapped_manifest[filename].pop('filename', None)
                mapped_manifest[filename].pop('location:1', None)

                # object ID / dia object ID
                if f"objectId:image_{location_suffix}" in column_names:
                    mapped_manifest[filename]["objectId"] = mapped_manifest[filename][f"objectId:image_{location_suffix}"]
                    mapped_manifest[filename]["objectIdType"] = "DIRECT"
                if f"diaObjectId:image_{location_suffix}" in column_names:
                    mapped_manifest[filename]["objectId"] = mapped_manifest[filename][f"diaObjectId:image_{location_suffix}"]
                    mapped_manifest[filename]["objectIdType"] = "INDIRECT"

                # User defined values
                user_defined_values = dict(filter(lambda e:(e[0] not in canonical_cols and "location:image_" not in e[0] and "objectId:image_" not in e[0] and "diaObjectId:image_" not in e[0]), mapped_manifest[filename].items())) 
                mapped_manifest[filename]["user_defined_values"] = user_defined_values

                # Now sort out the manifest to-be uploaded
                updated_csv_row[filename_idx] = url
            updated_csv_row.append(edc_ver_id)
            upload_manifest.append(updated_csv_row)
            edc_ver_id += 1


    with open(f"/tmp/{guid}/manifest.csv", 'w', newline='') as csvfile:
        for m_row in upload_manifest:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(m_row)

    manifestBlob = bucket.blob(f"{guid}/manifest.csv")
    manifestBlob.upload_from_filename(f"/tmp/{guid}/manifest.csv")
    update_batch_record_with_manifest_url(manifestBlob.public_url, batch_id)

    return manifestBlob.public_url, mapped_manifest

def build_and_upload_manifest_for_static_cutouts(urls, bucket, batch_id, guid):
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(bucket)

    # list to store the names of columns
    column_names = []
    mapped_manifest = {}
    upload_manifest = []
    filename_idx = None
    canonical_cols = ["objectId", "diaObjectId", "objectIdType", "diaObjectIdType", "edc_ver_id"]

    # Read the manifest that came from the RSP and store it in a dict with 
    # the filename as the key
    with open(f"/tmp/{guid}/manifest.csv", 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter = ',')

        # loop to iterate through the rows of csv
        edc_ver_id = round(time.time() * 1000)
        csv_rows = enumerate(csv_reader)
        column_names = next(csv_rows)[1]
        column_names.append("location:1")
        column_names.append("external_id")
        filename_idx = column_names.index("filename")
        upload_manifest.append(column_names)

        for idx, row in csv_rows:
            # Set new key for row
            filename = row[filename_idx]
            url_idx = [n for n, x in enumerate(urls) if filename in x]
            url = urls[url_idx[0]]
            
            mf_dict = dict(zip(column_names, row))
            mf_dict["edc_ver_id"] = edc_ver_id
            mf_dict["location:1"] = url
            
            mf_row = row
            mf_row.append(url)
            mf_row.append(edc_ver_id)
            mapped_manifest[filename] = mf_dict
            # mapped_manifest[filename].pop("external_id")
            upload_manifest.append(mf_row)
                        
            # User defined values
            user_defined_values = dict(filter(lambda e:(e[0] not in canonical_cols and "location:image_" not in e[0] and "objectId:image_" not in e[0] and "diaObjectId:image_" not in e[0]), mapped_manifest[filename].items())) 
            mapped_manifest[filename]["user_defined_values"] = user_defined_values

            edc_ver_id += 1

    with open(f"/tmp/{guid}/manifest.csv", 'w', newline='') as csvfile:
        for m_row in upload_manifest:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(m_row)

    manifestBlob = bucket.blob(f"{guid}/manifest.csv")
    manifestBlob.upload_from_filename(f"/tmp/{guid}/manifest.csv")
    update_batch_record_with_manifest_url(manifestBlob.public_url, batch_id)
    return manifestBlob.public_url, mapped_manifest

def upload_manifest(csv_path):
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)
    destination_filename = csv_path.replace("/tmp/", "")
    blob = bucket.blob(destination_filename)
    blob.upload_from_filename(csv_path)

    return blob.public_url