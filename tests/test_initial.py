# import unittest
import pytest, os
from dotenv import load_dotenv

from rsp_data_exporter import main
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
    assert main.check_batch_status(_PROJECT_ID, _VENDOR_BATCH_ID)[0]["batch_record"]["batch_id"] == 2

# Meta record tests
def test_create_meta_record():
    _URLS = ["http://some.fake.url/only/for/testing"]
    _VENDOR_BATCH_ID = 66666

    meta_ids = main.insert_meta_records(_URLS, _VENDOR_BATCH_ID)
    assert len(meta_ids) == 1

def test_check_meta_record_by_meta_id():
    _URLS = ["http://some.fake.url/only/for/testing"]
    _VENDOR_BATCH_ID = 55555

    meta_ids = main.insert_meta_records(_URLS, _VENDOR_BATCH_ID)
    meta_records = main.lookup_meta_record(None, None, meta_ids[0])
    assert len(meta_records) == 1

def test_check_meta_record_by_source_id():
    _URLS = ["http://some.fake.url/only/for/testing"]
    _VENDOR_BATCH_ID = 44444
    _SOURCE_ID_TYPE = "sourceId"

    main.insert_meta_records(_URLS, _VENDOR_BATCH_ID)
    meta_record = main.lookup_meta_record(_VENDOR_BATCH_ID, _SOURCE_ID_TYPE)
    assert meta_record > 1

# Lookup record tests
def test_create_lookup_record():
    _META_RECORD_ID = 100
    _PROJECT_ID = 1
    _BATCH_ID = 1

    successful = main.insert_lookup_record(_META_RECORD_ID, _PROJECT_ID, _BATCH_ID)
    assert successful == True

def test_lookup_lookup_records():
    _META_RECORD_ID = 100
    _PROJECT_ID = 1
    _BATCH_ID = 999

    main.insert_lookup_record(_META_RECORD_ID, _PROJECT_ID, _BATCH_ID)
    meta_ids = main.query_lookup_records(_PROJECT_ID, _BATCH_ID)
    assert len(meta_ids) > 0