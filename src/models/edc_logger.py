from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, String, DateTime, BigInteger
from sqlalchemy.sql import func
import sqlalchemy

Base = declarative_base()

class EdcLogger(Base):
    __tablename__ = 'edc_logger'

    # Column defs
    edc_logger_id = Column(BigInteger, primary_key=True)
    application_name = Column(String)
    run_id = Column(String)
    notes = Column(String)
    date_created = Column(DateTime, server_default=func.now())
    category = Column(String)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
        engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
        engine.dialect.description_encoding = None
        Session = sessionmaker(bind=engine)
        return Session()