import os
from sqlalchemy import select
from google.cloud import logging

try:
    from ..models.citizen_science.citizen_science_projects import CitizenScienceProjects
except:
    try:
        from models.citizen_science.citizen_science_projects import CitizenScienceProjects
    except:
        pass

DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_NAME = os.environ['DB_NAME']
DB_HOST = os.environ['DB_HOST']
DB_PORT = os.environ['DB_PORT']
CLOSED_PROJECT_STATUSES = ["COMPLETE", "CANCELLED", "ABANDONED"]