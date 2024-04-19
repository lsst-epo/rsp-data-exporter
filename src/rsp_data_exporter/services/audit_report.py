from sqlalchemy import select, func, delete
from google.cloud import logging
from panoptes_client import Project
from . import db as DatabaseService

logging_client = logging.Client()
log_name = "rsp-data-exporter.audit_service"
logger = logging_client.logger(log_name)

try:
    from ..models.citizen_science.citizen_science_audit import CitizenScienceAudit
except:
    try:
        from models.citizen_science.citizen_science_audit import CitizenScienceAudit
    except:
        pass

def rollback_audit_record(rollback):
    try:
        db = DatabaseService.get_db_connection()
        stmt = delete(CitizenScienceAudit).where(CitizenScienceAudit.cit_sci_audit_id == rollback.primaryKey)
        db.execute(stmt)
        db.commit()
    except Exception as e:
        logger.log_text(e.__str__())
        return False
    
    db.close()
    return True

def fetch_audit_records(vendor_project_id):
    try:
        db = DatabaseService.get_db_connection()
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

def insert_audit_records(vendor_project_id, mapped_manifest, owner_id):
    audit_records = []
    object_ids = []
    for key in mapped_manifest:
        if "objectId" in mapped_manifest[key]:
            object_id = mapped_manifest[key]["objectId"]
            object_ids.append(object_id)
            audit_records.append(CitizenScienceAudit(object_id=object_id, cit_sci_owner_id=owner_id, vendor_project_id=vendor_project_id))
        elif "diaObjectID" in mapped_manifest[key]:
            object_id = mapped_manifest[key]["diaObjectID"]
            object_ids.append(object_id)
            audit_records.append(CitizenScienceAudit(object_id=object_id, cit_sci_owner_id=owner_id, vendor_project_id=vendor_project_id))

    if len(audit_records) > 0:
        try:
            db = DatabaseService.get_db_connection()
            db.bulk_save_objects(audit_records, return_defaults=True)
            db.flush()
        except Exception as e:
            db.rollback()
            logger.log_text("an exception occurred in insert_audit_records!")
            logger.log_text(e.__str__())
    
    audit_messages = []
    db.commit()
    db.close()
    try:
        audit_messages = audit_object_ids(object_ids, vendor_project_id)
    except Exception as e:
        logger.log_text("an error occurred while trying to lookup object IDs!")
        logger.log_text(e.__str__())
        pass

    return audit_records, audit_messages

def audit_object_ids(object_ids, vendor_project_id):
    try:
        db = DatabaseService.get_db_connection()
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
    audit_response = []
    for row in audit_aggr:
        vendor_project_name, vendor_project_start_date, workflow_info, subjects_count = get_vendor_project_details(row[0])
        if vendor_project_name is not None:
            max_obj_id_count = len(object_ids)
            if subjects_count > len(object_ids):
                max_obj_id_count = subjects_count
            perc_match = float(row[1]) / max_obj_id_count
            audit_response.append(f"The dataset you curated has a {str(perc_match)} object ID match on another Zooniverse project: {vendor_project_name}, which was started on {vendor_project_start_date}. {workflow_info}")
    return audit_response

def get_vendor_project_details(vendor_project_id):
    try:
        project = Project.find(id=vendor_project_id)
        project_name = project.raw["display_name"]
        project_start_date = project.created_at[:10]
        project_subjects_count = project.subjects_count

        workflow_info = []
        for idx, workflow in enumerate(project.links.workflows):
            workflow_num = idx + 1
            workflow_info.append(f"Workflow #{str(workflow_num)} : {str(workflow.subjects_count)} subjects; {str(workflow.completeness)} complete. ")

        workflow_output = "No workflows associated with this project."
        if len(workflow_info) > 0:
            workflow_output = "".join(workflow_info)
        return project_name, project_start_date, workflow_output, project_subjects_count
    except Exception as e:
        if "Could not find project with id" in e.__str__():
            return "(Deleted Zooniverse project)", "Unknown start date", "Unknown workflow information", 0
        else:
            logger.log_text("an exception occurred in get_vendor_project_details")
            logger.log_text(e.__str__())
            return None, "Unknown start date", "Unknown workflow information", 0