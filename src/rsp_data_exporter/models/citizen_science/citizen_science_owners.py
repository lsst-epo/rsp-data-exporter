from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func

Base = declarative_base()

class CitizenScienceOwners(Base):
    __tablename__ = 'citizen_science_owners'

    # Column defs
    cit_sci_owner_id = Column(Integer, primary_key=True)
    email = Column(String(50))
    status = Column(String(30))
    date_created = Column(DateTime, server_default=func.now())
