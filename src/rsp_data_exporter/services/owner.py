from sqlalchemy import select, delete
from google.cloud import logging
from . import db as DatabaseService

try:
    from ..models.citizen_science.citizen_science_owners import CitizenScienceOwners
except:
    try:
        from models.citizen_science.citizen_science_owners import CitizenScienceOwners
    except:
        pass

BAD_OWNER_STATUSES = ["BLOCKED", "DISABLED"]
logging_client = logging.Client()
log_name = "rsp-data-exporter.owner_service"
logger = logging_client.logger(log_name)

def rollback_owner_record(rollback):
    try:
        db = DatabaseService.get_db_connection()
        stmt = delete(CitizenScienceOwners).where(CitizenScienceOwners.cit_sci_owner_id == rollback.primaryKey)
        db.execute(stmt)
        db.commit()
    except Exception as e:
        logger.log_text(e.__str__())
        return False
    
    db.close()
    return True

def create_new_owner_record(email):
    owner_id = None;
    messages = []

    try:
        db = DatabaseService.get_db_connection()
        citizen_science_owner_record = CitizenScienceOwners(email=email, status='ACTIVE')
        db.add(citizen_science_owner_record)
        db.flush()
        owner_id = citizen_science_owner_record.cit_sci_owner_id
    except Exception as e:
        db.rollback()
        logger.log_text(e.__str__())
        messages.append("An error occurred while attempting to create a new project owner record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    db.commit()
    db.close()
    return owner_id, messages

def lookup_owner_record(email):
    owner_id = None
    status = ""
    messages = []

    try:
        db = DatabaseService.get_db_connection()
        stmt = select(CitizenScienceOwners).where(CitizenScienceOwners.email == email)

        results = db.execute(stmt)
        for row in results.scalars():
            owner_id = row.cit_sci_owner_id
            status = row.status
            break
        
        logger.log_text(f"Inside of lookup_owner_record, ownerId: {str(owner_id)}")
        if status in BAD_OWNER_STATUSES:
            messages.append("You are not/no longer eligible to use the Rubin Science Platform to send data to Zooniverse.")

        db.close()
    except Exception as e:
        messages.append("An error occurred while looking up your project's owner record - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")
        logger.log_text(e.__str__())
   
    return owner_id, messages