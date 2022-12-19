from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.sql import func
import sqlalchemy

Base = declarative_base()

class CitizenScienceOwners(Base):
    __tablename__ = 'citizen_science_owners'

    # Column defs
    cit_sci_owner_id = Column(Integer, primary_key=True)
    email = Column(String(50))
    status = Column(String(30))
    date_created = Column(DateTime, server_default=func.now())

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
            engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
            engine.dialect.description_encoding = None
            Session = sessionmaker(bind=engine)
            return Session()