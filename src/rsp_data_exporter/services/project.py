import os
from sqlalchemy import select, delete
from google.cloud import logging
from . import db as DatabaseService

try:
    from ..models.citizen_science.citizen_science_projects import CitizenScienceProjects
except:
    try:
        from models.citizen_science.citizen_science_projects import CitizenScienceProjects
    except:
        pass

CLOSED_PROJECT_STATUSES = ["COMPLETE", "CANCELLED", "ABANDONED"]

logging_client = logging.Client()
log_name = "rsp-data-exporter.project_service"
logger = logging_client.logger(log_name)

def rollback_project_record(rollback):
    try:
        db = DatabaseService.get_db_connection()
        stmt = delete(CitizenScienceProjects).where(CitizenScienceProjects.cit_sci_proj_id == rollback.primaryKey)
        db.execute(stmt)
        db.commit()
    except Exception as e:
        logger.log_text(e.__str__())
        return False
    
    db.close()
    return True

def lookup_project_record(vendor_project_id):
    project_id = None
    status = None
    messages = []

    try:
        db = DatabaseService.get_db_connection()
        stmt = select(CitizenScienceProjects).where(CitizenScienceProjects.vendor_project_id == int(vendor_project_id))
        results = db.execute(stmt)
        for row in results.scalars():
            logger.log_text("in a result in the loop!")
            status = row.project_status
            data_rights_approved = row.data_rights_approved

            if status in CLOSED_PROJECT_STATUSES:
                messages.append(f"This project is in a status of {status} - either create a new project or contact Rubin to request for the project to be reopened.")
            else:
                project_id = row.cit_sci_proj_id
        db.close()
    except Exception as e:
        logger.log_text("An exception occurred in lookup_project_record")
        messages.append("An error occurred while attempting to lookup your project record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text(e.__str__())

    return project_id, data_rights_approved, messages

def create_new_project_record(owner_id, vendor_project_id):
    project_id = None
    messages = []
    try:
        db = DatabaseService.get_db_connection()
        citizen_science_project_record = CitizenScienceProjects(vendor_project_id=vendor_project_id, owner_id=owner_id, project_status='ACTIVE', excess_data_exception=False, data_rights_approved=False)
        db.add(citizen_science_project_record)
        db.flush()
        project_id = citizen_science_project_record.cit_sci_proj_id
    except Exception as e:
        db.rollback()
        messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text("An exception occurred while creating a new project record")
        logger.log_text(e.__str__())
    db.commit()
    db.close()
    return project_id, messages
