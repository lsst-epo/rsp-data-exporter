import os, csv, shutil, glob, threading
from google.cloud import logging
from google.cloud import storage
import numpy as np

logging_client = logging.Client()
log_name = "rsp-data-exporter.file_service"
logger = logging_client.logger(log_name)

CLOUD_STORAGE_CIT_SCI_PUBLIC = os.environ["CLOUD_STORAGE_CIT_SCI_PUBLIC"]

def download_zip(bucket_name, filename, guid, data_rights_approved, is_tabular_dataset = False):
    messages = []
    gcs = storage.Client()
    os.makedirs(f"/tmp/{guid}/", exist_ok=True)
    if is_tabular_dataset == True:
        filename = f"{guid}/{filename}"

    logger.log_text("about to log bucket download name:")
    logger.log_text(bucket_name)

    bucket = gcs.bucket(bucket_name)

    # Download the file to /tmp storage
    logger.log_text("filename to download: " + filename)
    blob = bucket.blob(filename)
    zipped_cutouts = f"/tmp/{filename}"
    blob.download_to_filename(zipped_cutouts)

    unzipped_cutouts_dir = f"/tmp/{guid}"

    # Deviate logic based on data type
    if is_tabular_dataset == True:
        files = os.listdir(unzipped_cutouts_dir)
        csv_path = f"{unzipped_cutouts_dir}/{files[0]}"
        logger.log_text("inside of TABULAR_DATA code block")
        # Get CSV file
        csv_file = open(csv_path, "rU")
        reader = csv.reader(csv_file, delimiter=',')

        logger.log_text("about to log CSV file contents")
        tabular_records = []
        for row in reader:
            logger.log_text(str(row))
            tabular_records.append(row)
        logger.log_text("done logging CSV file contents")

        return tabular_records
    else:
        shutil.unpack_archive(zipped_cutouts, unzipped_cutouts_dir, "zip")

        # Count the number of objects and remove any files more than the allotted amount based on
        # the RSP user's data rights approval status
        files = os.listdir(unzipped_cutouts_dir)

        max_objects_count = 100
        if data_rights_approved == True:
            max_objects_count = 10000
        else:
            messages.append("Your project has not been approved by the data rights panel as of yet, as such you will not be able to send any additional data to Zooniverse until your project is approved.")

        logger.log_text("Data is NOT in tabular format")
        if len(files) > max_objects_count:
            messages.append(f"Currently, a maximum of {str(max_objects_count)} objects is allowed per batch for your project - your batch of size {str(len(files))} has been has been truncated and anything in excess of {str(max_objects_count)} objects has been removed.")
            for f_file in files[(max_objects_count + 1):]:
                os.remove(f"{unzipped_cutouts_dir}/{f_file}")

        # Now, limit the files sent to image files
        pngs = glob.glob(f"/tmp/{guid}/*.png")
        jpegs = glob.glob(f"/tmp/{guid}/*.jpeg")
        jpgs = glob.glob(f"/tmp/{guid}/*.jpg")
        cutouts = pngs + jpegs + jpgs
        return cutouts, messages

def upload_cutouts(cutouts):
    if len(cutouts) > 500: # Arbitrary threshold for threading
        subset_count = round(len(np.array(cutouts)) / 250)
        sub_cutouts_arr = np.split(np.array(cutouts), subset_count) # create sub arrays divided by 1k cutouts
        threads = []
        for i, sub_arr in enumerate(sub_cutouts_arr):
            t = threading.Thread(target=upload_cutout_arr, args=(sub_arr,str(i),))
            threads.append(t)
            threads[i].start()
        
        for thread in threads:
            logger.log_text("joining thread!")
            thread.join()

    else:
        urls = upload_cutout_arr(cutouts, str(1))
    
    return urls
 
def upload_cutout_arr(cutouts, i):
    urls = []
    gcs = storage.Client()
    bucket = gcs.bucket(CLOUD_STORAGE_CIT_SCI_PUBLIC)

    already_logged = False

    for cutout in cutouts:
        if already_logged == False:
            logger.log_text(cutout)
            already_logged = True
        destination_filename = cutout.replace("/tmp/", "")
        blob = bucket.blob(destination_filename)
        
        blob.upload_from_filename(cutout)
        urls.append(blob.public_url)

    logger.log_text("finished uploading thread #" + i)

    return urls