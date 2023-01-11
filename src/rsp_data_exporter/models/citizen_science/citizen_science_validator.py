# For validating the data and project configurations for citizen science projects

class CitizenScienceValidator():
    error = False
    data_rights_approved = False
    excess_data_approved = False
    active_batch = False
    project_id = None # EDC project ID
    batch_id = None # EDC batch ID
    mapped_manifest = None
    log_to_edc = False
    edc_logger_notes = ""
    edc_logger_category = ""

    def __init__(self, error = False, data_rights_approved = False, excess_data_approved = False, active_batch = False):
        self.error = error
        self.data_rights_approved = data_rights_approved
        self.excess_data_approved = excess_data_approved
        self.active_batch = active_batch