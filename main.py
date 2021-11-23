import os
import subprocess
import csv
from flask import Flask, request
from google.cloud import storage
# import lsst.daf.butler as dafButler

app = Flask(__name__)

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']

@app.route("/")


def get_rsp_data_and_create_manifest():
    email = request.args.get('email')
    collection = request.args.get('collection')
    sourceId = request.args.get("sourceId")
    output = subprocess.run(['sh', '/opt/lsst/software/server/run.sh', email, collection], stdout=subprocess.PIPE).stdout.decode('utf-8')
    
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    bucket = gcs.bucket(CLOUD_STORAGE_BUCKET)

    # Create a new blob and upload the file's content.
    blob = bucket.blob("test2.fits")

    blob.upload_from_filename("/tmp/data/u/erosas/zooniverse-test/calexp/20241104/g/g_sim_1.4/703697/calexp_LSSTCam-imSim_g_g_sim_1_4_703697_R20_S22_u_erosas_zooniverse-test.fits")

    with open('/tmp/manifest.csv', 'w', newline='') as csvfile:
        fieldnames = ['email', 'sourceId', 'uri', 'externalId']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerow({'email': email, 'sourceId': sourceId, 'uri': blob.public_url, 'externalId': 12312312})

    manifestBlob = bucket.blob("manifest.csv")

    manifestBlob.upload_from_filename("/tmp/manifest.csv")

    return manifestBlob.public_url

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))