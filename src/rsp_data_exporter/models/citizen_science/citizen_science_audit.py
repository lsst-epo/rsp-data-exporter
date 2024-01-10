from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, ForeignKey, DateTime
from sqlalchemy.sql import func
from models.citizen_science.citizen_science_owners import CitizenScienceOwners

Base = declarative_base()

class CitizenScienceAudit(Base):
    __tablename__ = 'citizen_science_audit'

    # Column defs
    cit_sci_audit_id = Column(Integer, primary_key=True)
    object_id = Column(Integer)
    vendor_project_id = Column(Integer)
    cit_sci_owner_id = Column(Integer, ForeignKey(CitizenScienceOwners.cit_sci_owner_id))
    date_created = Column(DateTime, server_default=func.now())
