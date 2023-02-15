from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger
from sqlalchemy.sql import func
import sqlalchemy

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

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
            engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
            engine.dialect.description_encoding = None
            Session = sessionmaker(bind=engine)
            return Session()
    
    def update_meta_fields(self, edc_ver_id, source_id, source_id_type, user_defined_data):
          self.edc_ver_id = edc_ver_id
          self.source_id = source_id
          self.source_id_type = source_id_type
          self.user_defined_values = user_defined_data