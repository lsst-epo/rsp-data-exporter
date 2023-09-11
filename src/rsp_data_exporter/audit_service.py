import os
import sqlalchemy
from sqlalchemy import select, func
from google.cloud import logging
from panoptes_client import Project

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
    
    audit_messages = []
    try:
        audit_messages = audit_object_ids(object_ids, vendor_project_id)
    except Exception as e:
        logger.log_text("an error occurred while trying to lookup object IDs!")
        logger.log_text(e.__str__())
        pass

    return audit_records, audit_messages

def audit_object_ids(object_ids, vendor_project_id):
    logger.log_text("about to audit object IDs!")
    try:
        db = CitizenScienceAudit.get_db_connection(DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS)
        stmt = select([CitizenScienceAudit.vendor_project_id, func.count(CitizenScienceAudit.object_id)]).where(CitizenScienceAudit.object_id.in_(object_ids)).group_by(CitizenScienceAudit.vendor_project_id).filter(CitizenScienceAudit.vendor_project_id != int(vendor_project_id))
        results = db.execute(stmt).fetchall() 
    except Exception as e:
        logger.log_text("an exception occurred in audit_object_ids")
        logger.log_text(e.__str__())
        return

    audit_aggr = []
    for row in results:
        audit_aggr.append([row[0], row[1]])

    db.close()

    logger.log_text("about to loop over audit records and get project name")
    audit_response = []
    for row in audit_aggr:
        logger.log_text("logging row:")
        logger.log_text(str(row))
        vendor_project_name = get_vendor_project_name(row[0])
        if vendor_project_name is not None:
            logger.log_text("vendor_project_name not null!")
            perc_match = float(row[1]) / len(object_ids)
            audit_response.append("The dataset you curated has a " + str(perc_match) + " object ID match on another Zooniverse project: " + vendor_project_name)

    return audit_response

def get_vendor_project_name(vendor_project_id):
    logger.log_text("about to lookup project name in get_vendor_project_name")
    try:
        project = Project.find(id=vendor_project_id)
        logger.log_text("about to log project.raw")
        logger.log_text(str(project.raw))
        project_name = project.raw["display_name"]
        logger.log_text("logging project_name:")
        logger.log_text(project_name)
        return project_name
    except Exception as e:
        if "Could not find project with id" in e.__str__():
            return "(Deleted Zooniverse project)"
        else:
            logger.log_text("an exception occurred in get_vendor_project_name")
            logger.log_text(e.__str__())
            return None