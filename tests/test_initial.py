# import unittest
import pytest, os
from dotenv import load_dotenv

from rsp_data_exporter import main
load_dotenv()

_VENDOR_PROJECT_ID = 99999
_TEST_EMAIL = "some@fake.email"
_OWNER_ID = 0
_PROJECT_ID = 0
_BATCH_ID = 0

# If pytest is working fine then this test will always execute successfully
def test_sanity_check():
    assert main.check_test_only_var() == True

# Owner record tests
def test_create_owner_record():
    _OWNER_ID = main.create_new_owner_record(_TEST_EMAIL)
    assert _OWNER_ID == 1

def test_lookup_owner_record():
    assert main.lookup_owner_record(_TEST_EMAIL) == 1

# Project record tests
def test_create_new_project():
    global _OWNER_ID, _VENDOR_PROJECT_ID
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, _VENDOR_PROJECT_ID)
    assert _PROJECT_ID == 1

def test_lookup_project_record():
    global _VENDOR_PROJECT_ID
    assert main.lookup_project_record(_VENDOR_PROJECT_ID) == 1

# Batch record tests
def test_create_new_batch():
    _BATCH_ID = main.create_new_batch(_PROJECT_ID, _VENDOR_PROJECT_ID)
    assert _BATCH_ID == 1

def test_check_batch_status():
    assert len(main.check_batch_status(_PROJECT_ID, _VENDOR_PROJECT_ID)) == 1