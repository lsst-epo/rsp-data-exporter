import time, json
from sqlalchemy import select
from google.cloud import logging
import db as DatabaseService

try:
    from ..models.citizen_science.citizen_science_meta import CitizenScienceMeta
except:
    try:
        from models.citizen_science.citizen_science_meta import CitizenScienceMeta
    except:
        pass

logging_client = logging.Client()
log_name = "rsp-data-exporter.metadata_service"
logger = logging_client.logger(log_name)

def create_meta_records(urls):
    meta_records = []
    for url in urls:
        edcVerId = round(time.time() * 1000)
        public = True
        meta_records.append(CitizenScienceMeta(edc_ver_id=edcVerId, uri=url, public=public))
        pass
    return meta_records

def insert_meta_records(meta_records):
    logger.log_text("about to bulk insert meta records in insert_meta_records()!!")

    try:
        db = DatabaseService.get_db_connection()
        db.expire_on_commit = False
        db.bulk_save_objects(meta_records, return_defaults=True)
        db.commit()
        db.flush()
    except Exception as e:
        logger.log_text("an exception occurred in insert_meta_records!")
        logger.log_text(e.__str__())

    logger.log_text("done bulk inserting meta records!")
    return meta_records

def lookup_meta_record(objectId, objectIdType, meta_id = None):
    meta_records = []
    metaId = None
    try:
        if meta_id == None:
            db = DatabaseService.get_db_connection()
            stmt = select(CitizenScienceMeta).where(CitizenScienceMeta.object_id == objectId).where(CitizenScienceMeta.object_id_type == objectIdType)
            results = db.execute(stmt)
            for row in results.scalars():
                metaId = row.cit_sci_meta_id
 
            db.close()

            logger.log_text("about to log metaId (queried via objectId/objectIdType) in lookup_meta_record()")
            logger.log_text(str(metaId))
        else:
            db = DatabaseService.get_db_connection()
            stmt = select(CitizenScienceMeta).where(CitizenScienceMeta.cit_sci_meta_id == meta_id)
            results = db.execute(stmt)
            for row in results.scalars():
                meta_records.append({
                    "edc_ver_id": row.edc_ver_id,
                    "object_id": row.object_id,
                    "object_id_type": row.object_id_type,
                    "cutout_url": row.uri,
                    "date_transferred": str(row.date_created)
                })

            db.close()

            logger.log_text("about to log meta record count (queried by batch_id) in lookup_meta_record()")
            logger.log_text(str(len(meta_records)))
            return meta_records
    except Exception as e:
        logger.log_text(e.__str__())
        return e
   
    return metaId

def create_tabular_meta_records(tabular_records):
    logger.log_text("Creating meta records for tabular dataset")
    
    meta_records = []
    column_names = tabular_records.pop(0)
    logger.log_text("logging column_names after pop:")
    logger.log_text(str(column_names))

    # Source ID will be extracted directly
    obj_id_idx = column_names.index("objectId")
    column_names.pop(obj_id_idx)

    for row in tabular_records:
        metadata = {}

        # Extract the canonical fields so that all that is left are the user-defined values which can be joined
        source_id = row.pop(obj_id_idx)
        for c_idx, col in enumerate(row):
            metadata[column_names[c_idx]] = col

        user_defined_values = json.dumps(metadata)

        logger.log_text("Logging user_defined_values for source_id: " + str(source_id))
        logger.log_text(user_defined_values)

        public = True
        edc_ver_id = round(time.time() * 1000) + 1
        meta_records.append(CitizenScienceMeta(edc_ver_id=edc_ver_id, source_id=source_id, source_id_type="objectId", public=public, user_defined_values=user_defined_values))
    
    return meta_records