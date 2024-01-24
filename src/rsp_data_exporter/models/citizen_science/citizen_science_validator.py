from enum import Enum
# For validating the data and project configurations for citizen science projects

class CitizenScienceValidator():
    error = False
    data_rights_approved = False
    excess_data_approved = False
    active_batch = False
    project_id = None # EDC project ID
    batch_id = None # EDC batch ID
    owner_id = None # EDC owner ID
    mapped_manifest = None
    log_to_edc = False
    edc_logger_notes = ""
    edc_logger_category = ""
    _rollbacks = []
    RecordType = Enum('RecordType', ['CITIZEN_SCIENCE_AUDIT', 'CITIZEN_SCIENCE_BATCHES', 'CITIZEN_SCIENCE_META', "CITIZEN_SCIENCE_OWNERS", "CITIZEN_SCIENCE_PROJ_META_LOOKUP", "CITIZEN_SCIENCE_PROJECTS"])

    def __init__(self, error = False, data_rights_approved = False, excess_data_approved = False, active_batch = False):
        self.error = error
        self.data_rights_approved = data_rights_approved
        self.excess_data_approved = excess_data_approved
        self.active_batch = active_batch

    def appendRollback(self, recordType: Enum, primaryKey: int):
        # Type checking
        if not isinstance(recordType, self.RecordType):
            raise TypeError('recordType must be an instance of RecordType Enum')
        if not isinstance(primaryKey, int):
            raise TypeError('primaryKey must be an instance of int')
            
        self._rollbacks.append(self.CitizenScienceRollback(recordType, primaryKey))
        return self._rollbacks

    class CitizenScienceRollback():
        recordType = None
        primaryKey = None

        def __init__(self, recordType, primaryKey):
            # Type checking
            if not isinstance(recordType.name, str):
                raise TypeError('recordType must be an instance of RecordType Enum')
            if not isinstance(primaryKey, int):
                raise TypeError('primaryKey must be an instance of int')
            self.recordType = recordType
            self.primaryKey = primaryKey