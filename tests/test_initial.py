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
    _OWNER_ID = main.create_new_owner_record("fake@email.com")
    assert _OWNER_ID == 1

def test_lookup_owner_record():
    assert main.lookup_owner_record("fake@email.com") == 1

# Project record tests
def test_create_new_project():
    _OWNER_ID = main.create_new_owner_record("fake@email.org")
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, 99999)
    assert _PROJECT_ID == 1

def test_lookup_project_record():
    assert main.lookup_project_record("99999") == 2

# Batch record tests
def test_create_new_batch():
    _OWNER_ID = main.create_new_owner_record("fake@email.net")
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, 77777)
    _BATCH_ID = main.create_new_batch(_PROJECT_ID, 77777)
    assert _BATCH_ID == 1

def test_check_batch_status():
    _OWNER_ID = main.create_new_owner_record("fake@email.io")
    _PROJECT_ID = main.create_new_project_record(_OWNER_ID, 88888)
    _BATCH_ID = main.create_new_batch(_PROJECT_ID, 88888)
    assert main.check_batch_status(_PROJECT_ID, 88888)[0] == 2