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
    _EMAIL = "fake@email.com"
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