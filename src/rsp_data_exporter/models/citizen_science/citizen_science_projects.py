from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.sql import func

Base = declarative_base()

class CitizenScienceProjects(Base):
    __tablename__ = 'citizen_science_projects'

    # Column defs
    cit_sci_proj_id = Column(Integer, primary_key=True)
    vendor_project_id = Column(Integer)
    owner_id = Column(Integer) # , ForeignKey('citizen_science_projects.cit_sci_owner_id')
    project_status = Column(String(50))
    excess_data_exception = Column(Boolean)
    date_created = Column(DateTime, server_default=func.now())
    date_completed = Column(DateTime)
    data_rights_approved = Column(Boolean)
