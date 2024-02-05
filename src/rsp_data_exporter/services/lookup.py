from sqlalchemy import select, delete
from google.cloud import logging
from . import db as DatabaseService

try:
    from ..models.citizen_science.citizen_science_proj_meta_lookup import CitizenScienceProjMetaLookup
except:
    try:
        from models.citizen_science.citizen_science_proj_meta_lookup import CitizenScienceProjMetaLookup
    except:
        pass

logging_client = logging.Client()
log_name = "rsp-data-exporter.lookup_service"
logger = logging_client.logger(log_name)

def rollback_lookup_record(rollback):
    try:
        db = DatabaseService.get_db_connection()
        stmt = delete(CitizenScienceProjMetaLookup).where(CitizenScienceProjMetaLookup.cit_sci_lookup_id == rollback.primaryKey)
        db.execute(stmt)
        db.commit()
    except Exception as e:
        logger.log_text(e.__str__())
        return False
    
    db.close()
    return True

def query_lookup_records(project_id, batch_id):
    db = DatabaseService.get_db_connection()
    query = select(CitizenScienceProjMetaLookup).where(CitizenScienceProjMetaLookup.cit_sci_proj_id == project_id).where(CitizenScienceProjMetaLookup.cit_sci_batch_id == int(batch_id))
    lookup_records = db.execute(query)
    db.commit()
    meta_ids = []
    for row in lookup_records.scalars():
        meta_ids.append(row.cit_sci_meta_id)
    
    return meta_ids

def insert_lookup_records(meta_records, project_id, batch_id):
    lookup_records = []
    for record in meta_records:
        lookup_records.append(CitizenScienceProjMetaLookup(cit_sci_proj_id=project_id, cit_sci_meta_id=record.cit_sci_meta_id, cit_sci_batch_id=batch_id))

    try:
        db = DatabaseService.get_db_connection()
        db.bulk_save_objects(lookup_records, return_defaults=True)
        db.flush()
    except Exception as e:
        db.rollback()
        logger.log_text("An exception occurred while trying to insert lookup record!!")
        logger.log_text(e.__str__())
        return False
    
    db.commit()
    db.close()
    return lookup_records