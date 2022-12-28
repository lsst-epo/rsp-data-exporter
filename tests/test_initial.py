# import unittest
import pytest
from rsp_data_exporter import main

# If pytest is working fine then this test will always execute successfully
def test_sanity_check():
    assert main.test_function() == True

def test_create_new_batch():
    assert main.create_new_batch(10000, 20000) > 0

def test_check_batch_status():
    assert len(main.check_batch_status(10000, 20000)) > 0