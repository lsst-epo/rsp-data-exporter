from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger, Numeric
from sqlalchemy.sql import func

Base = declarative_base()

class CitizenScienceMeta(Base):
    __tablename__ = 'citizen_science_meta'

    # Column defs
    cit_sci_meta_id = Column(Integer, primary_key=True)
    edc_ver_id = Column(Integer)
    source_id = Column(BigInteger)
    source_id_type = Column(String(30))
    uri = Column(String(255))
    public = Column(Boolean)
    date_created = Column(DateTime, server_default=func.now())
    user_defined_values = Column(String(500))
    object_id = Column(BigInteger)
    object_id_type = Column(String(50))
    ra = Column(Numeric(30))
    dec = Column(Numeric(30))
    
    def set_fields(self, **kwargs):
        if "edc_ver_id" in kwargs:
            self.edc_ver_id = kwargs["edc_ver_id"]
        if "source_id" in kwargs:
            self.source_id = kwargs["source_id"]
        if "source_id_type" in kwargs:
          self.source_id_type = kwargs["source_id_type"]
        if "user_defined_values" in kwargs:
            self.user_defined_values = kwargs["user_defined_values"]
        if "object_id" in kwargs:
            self.object_id = kwargs["object_id"]
        if "object_id_type" in kwargs:
            self.object_id_type = kwargs["object_id_type"]
        if "ra" in kwargs:
            self.ra = kwargs["ra"]
        if "dec" in kwargs:
            self.dec = kwargs["dec"]