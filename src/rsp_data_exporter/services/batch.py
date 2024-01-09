from sqlalchemy import select
from google.cloud import logging
from panoptes_client import Project
from . import db as DatabaseService

try:
    from ..models.citizen_science.citizen_science_batches import CitizenScienceBatches
except:
    try:
        from models.citizen_science.citizen_science_batches import CitizenScienceBatches
    except:
        pass

logging_client = logging.Client()
log_name = "rsp-data-exporter.batch_service"
logger = logging_client.logger(log_name)

def get_current_active_batch_id(project_id):
    db = DatabaseService.get_db_connection()
    stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_proj_id == project_id).where(CitizenScienceBatches.batch_status == 'ACTIVE')
    results = db.execute(stmt)
    
    logger.log_text("logging dir of results in get_current_active_batch_id:")
    record = results.scalars().first()
    logger.log_text(str(dir(record)))
    logger.log_text("about to log results from within get_current_active_batch_id:")
    
    batch_id = record.cit_sci_batch_id
    logger.log_text(str(batch_id))
    return batch_id

def create_new_batch(project_id, vendor_batch_id):
    batch_id = -1;
    messages = []

    try:
        db = DatabaseService.get_db_connection()
        db.expire_on_commit = False
        citizen_science_batch_record = CitizenScienceBatches(cit_sci_proj_id=project_id, vendor_batch_id=vendor_batch_id, batch_status='ACTIVE')    
        db.add(citizen_science_batch_record)
        
        db.commit()
        db.expunge_all()
        db.close()
        batch_id = citizen_science_batch_record.cit_sci_batch_id
        logger.log_text("new batch id: " + str(batch_id))
    except Exception as e:
        logger.log_text("An exception occurred while trying to create a new batch!:")
        logger.log_text("Exception text: " + e.__str__())
        logger.log_text("Exception text: " + str(e))
        logger.log_text("End of exception logging")
        messages.append("An error occurred while attempting to create a new data batch record for you - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    return batch_id

def check_batch_status(project_id, vendor_project_id, test_only, data_rights_approved):
    logger.log_text("inside of check_batch_status, logging project_id : " + str(project_id))
    batches_still_active = []
    batches_not_found_in_zooniverse = []
    batches_in_db = []
    messages = []

    try:
        db = DatabaseService.get_db_connection()
        stmt = select(CitizenScienceBatches).where(CitizenScienceBatches.cit_sci_proj_id == project_id).where(CitizenScienceBatches.batch_status == 'ACTIVE')
        results = db.execute(stmt)
        
        for row in results.scalars():
            # if validator.data_rights_approved == False:
                # validator.error = True
                # response.status = "error"
                # response.messages.append("Your project has not yet been approved by the data rights panel. You can curate no more than one subject set before your project is approved.")
                # db.close()
                # return
            batches_in_db.append({
                "batch_record" : row,
                "batch_id" : row.cit_sci_batch_id,
                "vendor_batch_id_db" : row.vendor_batch_id
            })

        db.commit()

        logger.log_text("about to evaluate batch_id after checking the DB")
        if len(batches_in_db) > 0 and test_only == False:
            logger.log_text("# of active batches found in DB: " + str(len(batches_in_db)))
            # Call the Zooniverse API to get all subject sets for the project
            project = Project.find(int(vendor_project_id))

            logger.log_text("about to log project:")
            logger.log_text(str(project.raw))

            subject_set_list = list(project.links.subject_sets)
            
            for batch_in_db in batches_in_db:
                update_batch_record = False
                logger.log_text("about to process batch_id")
                logger.log_text(str(batch_in_db["batch_id"]))
                logger.log_text("about to process vendor_batch_id_db")
                logger.log_text(str(batch_in_db["vendor_batch_id_db"]))

                if len(subject_set_list) == 0:
                    logger.log_text("the length of project.links.subject_sets is 0!")
                    update_batch_record = True
                else:
                    logger.log_text("the length of project.links.subject_sets is > 0! : " + str(len(list(project.links.subject_sets))))

                    # Evaluate data rights
                    if data_rights_approved == False:
                        messages.append("Your project has not yet been approved by the data rights panel. You can curate no more than one subject set before your project is approved. If you have an existing subject set that you have already sent to your Zooniverse project and you need to correct the data before you present your project to the data rights panel then delete the subject set on the Zooniverse platform and try again.")
                        db.close()
                        return
                    
                    found_subject_set = False
                    for sub in subject_set_list:
                        try:
                            logger.log_text("looping through project.links.subject_sets")
                            if str(batch_in_db["vendor_batch_id_db"]) == sub.id:
                                logger.log_text("Found the subject set in question!")
                                found_subject_set = True
                                # Zooniverse has a weird way of tracking subject sets, subject sets that have not been
                                # added to a workflow have a completeness of: {}
                                if len(sub.completeness) == 0:
                                    logger.log_text("The subject set hasn't been assigned to a workflow/hasn't been worked on!!!")
                                    update_batch_record = False
                                    batches_still_active.append(sub.id)
                                    break
                                else:
                                    # The subject set HAS been started, worked on, so evaluate if it is complete
                                    for completeness_key in sub.completeness:
                                        if sub.completeness[completeness_key] == 1.0:
                                            logger.log_text("subject set IS COMPLETE!!")
                                            update_batch_record = True
                                            break
                                        else:
                                            # Found the batch, but it's not complete, check if it contains subjects or not
                                            try:
                                                logger.log_text("subject set is NOT complete!!")
                                                first = next(subject_set_list[0].subjects)
                                                if first is not None:
                                                    # Active batch with subjects, return
                                                    logger.log_text("first is NOT None!!!")
                                                    batches_still_active.append(sub.id)
                                                    update_batch_record = False
                                                    break
                                                else:
                                                    logger.log_text("Erroneous caching of Zooniverse client, updating EDC database");
                                                    update_batch_record = True
                                                    break
                                            except StopIteration:
                                                logger.log_text("setting validator.error to True!")
                                                messages.append("You have an active, but empty subject set on the zooniverse platform with an ID of " + str(batch_in_db["vendor_batch_id_db"]) + ". Please delete this subject set on the Zoonivese platform and try again.")
                                                continue
                        except Exception as e:
                            logger.log_text("An error occurred while looping through the subject sets, this usually occurs because of stale data that has been cached by Zooniverse. ")
                            continue

                    if found_subject_set == False:
                        logger.log_text("The subject set in question was NEVER found!")
                        batches_not_found_in_zooniverse.append(str(batch_in_db["vendor_batch_id_db"]))
                        update_batch_record = True

                if update_batch_record == True:
                    logger.log_text("about to update EDC batch ID " + str(batch_in_db["batch_id"]) + ", vendor batch ID: " + str(batch_in_db["vendor_batch_id_db"]) + " in the DB!")
                    batch_in_db["batch_record"].batch_status = "COMPLETE"
                    db.commit()
        elif test_only == True:
            for batch in batches_in_db:
                batches_still_active.append({"batch_record" : batch})

    except Exception as e:
        logger.log_text("about to log exception in check_batch_status!")
        logger.log_text(e.__str__())
        messages.append("An error occurred while attempting to lookup your batch records - this is usually due to an internal issue that we have been alerted to. Apologies about the downtime - please try again later.")

    db.close()

    return batches_in_db, messages
