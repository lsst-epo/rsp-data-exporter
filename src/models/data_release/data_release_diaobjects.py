from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import sqlalchemy

Base = declarative_base()

class DataReleaseDiaObjects(Base):
    __tablename__ = 'data_release_diaobjects'

    # Column defs
    edc_diaobj_ver_id = Column(BigInteger, primary_key=True)
    decl = Column(DOUBLE_PRECISION)
    ra = Column(DOUBLE_PRECISION)
    gpsfluxchi2 = Column(DOUBLE_PRECISION)
    ipsfluxchi2 = Column(DOUBLE_PRECISION)
    rpsfluxchi2 = Column(DOUBLE_PRECISION)
    upsfluxchi2 = Column(DOUBLE_PRECISION)
    ypsfluxchi2 = Column(DOUBLE_PRECISION)
    zpsfluxchi2 = Column(DOUBLE_PRECISION)
    gpsfluxmax = Column(DOUBLE_PRECISION)
    ipsfluxmax = Column(DOUBLE_PRECISION)
    rpsfluxmax = Column(DOUBLE_PRECISION)
    upsfluxmax = Column(DOUBLE_PRECISION)
    ypsfluxmax = Column(DOUBLE_PRECISION)
    zpsfluxmax = Column(DOUBLE_PRECISION)
    gpsfluxmin = Column(DOUBLE_PRECISION)
    ipsfluxmin = Column(DOUBLE_PRECISION)
    rpsfluxmin = Column(DOUBLE_PRECISION)
    upsfluxmin = Column(DOUBLE_PRECISION)
    ypsfluxmin = Column(DOUBLE_PRECISION)
    zpsfluxmin = Column(DOUBLE_PRECISION)
    gpsfluxmean = Column(DOUBLE_PRECISION)
    ipsfluxmean = Column(DOUBLE_PRECISION)
    rpsfluxmean = Column(DOUBLE_PRECISION)
    upsfluxmean = Column(DOUBLE_PRECISION)
    ypsfluxmean = Column(DOUBLE_PRECISION)
    zpsfluxmean = Column(DOUBLE_PRECISION)
    gpsfluxndata = Column(DOUBLE_PRECISION)
    ipsfluxndata = Column(DOUBLE_PRECISION)
    rpsfluxndata = Column(DOUBLE_PRECISION)
    upsfluxndata = Column(DOUBLE_PRECISION)
    ypsfluxndata = Column(DOUBLE_PRECISION)
    zpsfluxndata = Column(DOUBLE_PRECISION)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
        engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
        engine.dialect.description_encoding = None
        Session = sessionmaker(bind=engine)
        return Session()