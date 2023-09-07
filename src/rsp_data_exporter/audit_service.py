import os
import sqlalchemy
from sqlalchemy import select, func
from google.cloud import logging

logging_client = logging.Client()
log_name = "rsp-data-exporter.audit_service"
logger = logging_client.logger(log_name)

try:
    from .models.citizen_science.citizen_science_audit import CitizenScienceAudit
except:
    try:
        from models.citizen_science.citizen_science_audit import CitizenScienceAudit
    except:
        pass

DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']

def fetch_audit_records(vendor_project_id):
    try:
        db = CitizenScienceAudit.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select(CitizenScienceAudit.object_id).where(CitizenScienceAudit.vendor_project_id == int(vendor_project_id))
        results = db.execute(stmt)
        db.close()
    except Exception as e:
        logger.log_text("an exception occurred in lookup_project_record")
        logger.log_text(e.__str__())

    audit_response = []
    for row in results.scalars():
        audit_response.append(row)
        
    return str(audit_response)

def insert_audit_records(vendor_project_id, validator):
    audit_records = []

    logger.log_text("About to loop over validator.mapped_manifest in insert_audit_records()")
    object_ids = []
    for key in validator.mapped_manifest:
        logger.log_text("-- looking up key: " + key)
        logger.log_text(str(validator.mapped_manifest[key]))
        if "objectId" in validator.mapped_manifest[key]:
            logger.log_text("-- " + str(validator.mapped_manifest[key]))

            object_id = validator.mapped_manifest[key]["objectId"]
            object_ids.append(object_id)
            audit_records.append(CitizenScienceAudit(object_id=object_id, cit_sci_owner_id=validator.owner_id, vendor_project_id=vendor_project_id))

        if len(audit_records) > 0:
            try:
                db = CitizenScienceAudit.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
                db.expire_on_commit = False
                db.bulk_save_objects(audit_records, return_defaults=True)
                db.commit()
                db.flush()
            except Exception as e:
                logger.log_text("an exception occurred in insert_audit_records!")
                logger.log_text(e.__str__())
    try:
        audit_object_ids(object_ids, vendor_project_id)
    except:
        pass

    return audit_records

def audit_object_ids(object_ids, vendor_project_id):
    logger.log_text("about to audit object IDs!")
    try:
        db = CitizenScienceAudit.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select([CitizenScienceAudit.vendor_project_id, func.count(CitizenScienceAudit.object_id)]).where(CitizenScienceAudit.object_id.in_(object_ids)).group_by(CitizenScienceAudit.vendor_project_id).filter(CitizenScienceAudit.vendor_project_id != int(vendor_project_id))
        
        logger.log_text("about to execute query")
        results = db.execute(stmt).fetchall() 
        logger.log_text("done executing query!")
        # db.flush()
    except Exception as e:
        logger.log_text("an exception occurred in audit_object_ids")
        logger.log_text(e.__str__())

    logger.log_text("done with the query, now processing the results")
    audit_response = []
    # audit_dict = []
    for row in results:
        logger.log_text("looping over row!")
        audit_response.append(row)
        # audit_dict.append(row.__dict__)

    db.close()
    logger.log_text("about to log audit results!")
    logger.log_text(str(audit_response))

    logger.log_text("logging audit_response[0][0]")
    logger.log_text(str(audit_response[0][0]))

    logger.log_text("logging audit_response[0][1]")
    logger.log_text(str(audit_response[0][1]))

    return



# stmt = select(CitizenScienceAudit.object_id, CitizenScienceAudit.vendor_project_id).where(and_(CitizenScienceAudit.vendor_project_id.is_not(int(vendor_project_id))), CitizenScienceAudit.object_id.in_(object_ids)).group_by(CitizenScienceAudit.vendor_project_id)
