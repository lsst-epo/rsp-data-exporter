import os
from sqlalchemy import select
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

def lookup_project_record(vendor_project_id):
    project_id = None
    status = None
    messages = []

    logger.log_text("logging vendorProjectId:")
    logger.log_text(vendor_project_id)

    try:
        db = DatabaseService.get_db_connection()
        stmt = select(CitizenScienceProjects).where(CitizenScienceProjects.vendor_project_id == int(vendor_project_id))

        logger.log_text("about to execute query in lookup_project_record")
        results = db.execute(stmt)

        logger.log_text("about to loop through results")
        for row in results.scalars():
            logger.log_text("in a result in the loop!")
            status = row.project_status
            
            data_rights_approved = row.data_rights_approved

            logger.log_text("about to check project status")
            if status in CLOSED_PROJECT_STATUSES:
                messages.append("This project is in a status of " + status + " - either create a new project or contact Rubin to request for the project to be reopened.")
            else:
                logger.log_text("project status is in a good place")
                project_id = row.cit_sci_proj_id
        db.close()
    except Exception as e:
        logger.log_text("an exception occurred in lookup_project_record")
        messages.append("An error occurred while attempting to lookup your project record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text(e.__str__())

    logger.log_text("about to return project_id in lookup_project_record")
    return project_id, data_rights_approved, messages

def create_new_project_record(owner_id, vendor_project_id):
    project_id = None
    messages = []
    try:
        logger.log_text("about to create new project record!")
        db = DatabaseService.get_db_connection()
        citizen_science_project_record = CitizenScienceProjects(vendor_project_id=vendor_project_id, owner_id=owner_id, project_status='ACTIVE', excess_data_exception=False, data_rights_approved=False)
        db.add(citizen_science_project_record)
        db.commit()
        project_id = citizen_science_project_record.cit_sci_proj_id

    except Exception as e:
        messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text("An exception occurred while creating a new project record")
        logger.log_text(e.__str__())

    logger.log_text("about to return from create_new_project_record")
    return project_id
