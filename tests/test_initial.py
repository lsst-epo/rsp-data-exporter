# import unittest
import pytest, os
from dotenv import load_dotenv
from rsp_data_exporter import main
from rsp_data_exporter.models.citizen_science.citizen_science_meta import CitizenScienceMeta
import rsp_data_exporter.services.metadata as MetadataService
import rsp_data_exporter.services.lookup as LookupService

load_dotenv()

# If pytest is working fine then this test will always execute successfully
def test_sanity_check():
    assert main.check_test_only_var() == True

# Owner record tests
def test_create_owner_record():
    _EMAIL = "fake@email.com"

    _OWNER_ID = main.create_new_owner_record(_EMAIL)
    assert _OWNER_ID == 1

def test_lookup_owner_record():
    _EMAIL = "fake@email.com"

    assert main.lookup_owner_record(_EMAIL) == 1

# Project record tests
def test_create_new_project():
    _EMAIL = "fake@email.org"
    _VENDOR_BATCH_ID = 99999

    _OWNER_ID = main.create_new_owner_record(_EMAIL)
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, _VENDOR_BATCH_ID)
    assert _PROJECT_ID == 1

def test_lookup_project_record():
    _VENDOR_BATCH_ID = "99999"

    assert main.lookup_project_record(_VENDOR_BATCH_ID) == 1

# Batch record tests
def test_create_new_batch():
    _EMAIL = "fake@email.net"
    _VENDOR_BATCH_ID = 77777

    _OWNER_ID = main.create_new_owner_record(_EMAIL)
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, _VENDOR_BATCH_ID)
    _BATCH_ID = main.create_new_batch(_PROJECT_ID, _VENDOR_BATCH_ID)
    assert _BATCH_ID == 1

def test_check_batch_status():
    _EMAIL = "fake@email.io"
    _VENDOR_BATCH_ID = 88888

    _OWNER_ID = main.create_new_owner_record(_EMAIL)
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, _VENDOR_BATCH_ID)
    main.create_new_batch(_PROJECT_ID, _VENDOR_BATCH_ID)
    assert len(main.check_batch_status(_PROJECT_ID, _VENDOR_BATCH_ID)) == 0

# Meta record tests
def test_create_meta_record():
    _URL = "http://some.fake.url/only/for/testing"
    _EDC_VER_ID = 123123123
    _PUBLIC = True
    _SOURCE_ID = 321321321
    _SOURCE_ID_TYPE = "objectId"
    _USER_DEFINED_VALUES = { "just_a" : "test" }

    meta_ids = MetadataService.insert_meta_records([CitizenScienceMeta(edc_ver_id=_EDC_VER_ID, uri=_URL, public=_PUBLIC, source_id=_SOURCE_ID, source_id_type=_SOURCE_ID_TYPE, user_defined_values=str(_USER_DEFINED_VALUES))])
    assert len(meta_ids) == 1

def test_check_meta_record_by_meta_id():
    _URL = "http://some.fake.url/only/for/testing"
    _EDC_VER_ID = 123123123
    _PUBLIC = True
    _SOURCE_ID = 321321321
    _SOURCE_ID_TYPE = "objectId"
    _USER_DEFINED_VALUES = { "just_a" : "test" }

    meta_ids = MetadataService.insert_meta_records([CitizenScienceMeta(edc_ver_id=_EDC_VER_ID, uri=_URL, public=_PUBLIC, source_id=_SOURCE_ID, source_id_type=_SOURCE_ID_TYPE, user_defined_values=str(_USER_DEFINED_VALUES))])
    meta_records = MetadataService.lookup_meta_record(None, None, meta_ids[0].cit_sci_meta_id)
    assert len(meta_records) == 1

def test_check_meta_record_by_object_id():
    _EMAIL = "fake@email.az"
    _VENDOR_BATCH_ID = 33333

    _OWNER_ID = main.create_new_owner_record(_EMAIL)
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, _VENDOR_BATCH_ID)
    _BATCH_ID = main.create_new_batch(_PROJECT_ID, _VENDOR_BATCH_ID)

    _URL = "http://some.fake.url/only/for/testing"
    _EDC_VER_ID = 123123123
    _PUBLIC = True
    _OBJECT_ID = 321321321
    _OBJECT_ID_TYPE = "DIRECT"
    _USER_DEFINED_VALUES = { "just_a" : "test" }

    main.validator.project_id = _PROJECT_ID
    main.validator.batch_id = _BATCH_ID
    MetadataService.insert_meta_records([CitizenScienceMeta(edc_ver_id=_EDC_VER_ID, uri=_URL, public=_PUBLIC, object_id=_OBJECT_ID, object_id_type=_OBJECT_ID_TYPE, user_defined_values=str(_USER_DEFINED_VALUES))])
    meta_record = MetadataService.lookup_meta_record(_OBJECT_ID, _OBJECT_ID_TYPE)
    assert meta_record > 1

# Lookup record tests
def test_create_lookup_record():
    _EMAIL = "fake@email.tv"
    _VENDOR_BATCH_ID = 22333
    
    _URL = "http://some.fake.url/only/for/testing"
    _EDC_VER_ID = 123123123
    _PUBLIC = True
    _SOURCE_ID = 321321321
    _SOURCE_ID_TYPE = "objectId"
    _USER_DEFINED_VALUES = { "just_a" : "test" }

    _OWNER_ID = main.create_new_owner_record(_EMAIL)
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, _VENDOR_BATCH_ID)
    _BATCH_ID = main.create_new_batch(_PROJECT_ID, _VENDOR_BATCH_ID)

    _META_RECORDS = MetadataService.insert_meta_records([CitizenScienceMeta(edc_ver_id=_EDC_VER_ID, uri=_URL, public=_PUBLIC, source_id=_SOURCE_ID, source_id_type=_SOURCE_ID_TYPE, user_defined_values=str(_USER_DEFINED_VALUES))])

    lookup_records = LookupService.insert_lookup_records(_META_RECORDS, _PROJECT_ID, _BATCH_ID)
    assert len(lookup_records) > 0

def test_lookup_lookup_records():
    _EMAIL = "fake@email.ca"
    _VENDOR_BATCH_ID = 11122
    
    _URL = "http://some.fake.url/only/for/testing"
    _EDC_VER_ID = 123123123
    _PUBLIC = True
    _SOURCE_ID = 321321321
    _SOURCE_ID_TYPE = "objectId"
    _USER_DEFINED_VALUES = { "just_a" : "test" }

    _OWNER_ID = main.create_new_owner_record(_EMAIL)
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, _VENDOR_BATCH_ID)
    _BATCH_ID = main.create_new_batch(_PROJECT_ID, _VENDOR_BATCH_ID)
    _META_RECORDS = MetadataService.insert_meta_records([CitizenScienceMeta(edc_ver_id=_EDC_VER_ID, uri=_URL, public=_PUBLIC, source_id=_SOURCE_ID, source_id_type=_SOURCE_ID_TYPE, user_defined_values=str(_USER_DEFINED_VALUES))])

    LookupService.insert_lookup_records(_META_RECORDS, _PROJECT_ID, _BATCH_ID)
    meta_ids = LookupService.query_lookup_records(_PROJECT_ID, _BATCH_ID)
    assert len(meta_ids) > 0