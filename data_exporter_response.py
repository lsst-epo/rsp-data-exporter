# For creating robust responses from the RSP Data Exporter service

class DataExporterResponse():
    status = ""
    messages = []
    manifest_url = ""

    def __init__(self, status = None, message = None, manifest_url = None):
        self.status = status
        self.message = message
        self.manifest_url = manifest_url