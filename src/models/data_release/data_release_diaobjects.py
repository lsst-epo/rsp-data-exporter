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
    gPSFluxChi2 = Column(DOUBLE_PRECISION)
    iPSFluxChi2 = Column(DOUBLE_PRECISION)
    rPSFluxChi2 = Column(DOUBLE_PRECISION)
    uPSFluxChi2 = Column(DOUBLE_PRECISION)
    yPSFluxChi2 = Column(DOUBLE_PRECISION)
    zPSFluxChi2 = Column(DOUBLE_PRECISION)
    gPSFluxMax = Column(DOUBLE_PRECISION)
    iPSFluxMax = Column(DOUBLE_PRECISION)
    rPSFluxMax = Column(DOUBLE_PRECISION)
    uPSFluxMax = Column(DOUBLE_PRECISION)
    yPSFluxMax = Column(DOUBLE_PRECISION)
    zPSFluxMax = Column(DOUBLE_PRECISION)
    gPSFluxMin = Column(DOUBLE_PRECISION)
    iPSFluxMin = Column(DOUBLE_PRECISION)
    rPSFluxMin = Column(DOUBLE_PRECISION)
    uPSFluxMin = Column(DOUBLE_PRECISION)
    yPSFluxMin = Column(DOUBLE_PRECISION)
    zPSFluxMin = Column(DOUBLE_PRECISION)
    gPSFluxMean = Column(DOUBLE_PRECISION)
    iPSFluxMean = Column(DOUBLE_PRECISION)
    rPSFluxMean = Column(DOUBLE_PRECISION)
    uPSFluxMean = Column(DOUBLE_PRECISION)
    yPSFluxMean = Column(DOUBLE_PRECISION)
    zPSFluxMean = Column(DOUBLE_PRECISION)
    gPSFluxNdata = Column(DOUBLE_PRECISION)
    iPSFluxNdata = Column(DOUBLE_PRECISION)
    rPSFluxNdata = Column(DOUBLE_PRECISION)
    uPSFluxNdata = Column(DOUBLE_PRECISION)
    yPSFluxNdata = Column(DOUBLE_PRECISION)
    zPSFluxNdata = Column(DOUBLE_PRECISION)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
        engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
        engine.dialect.description_encoding = None
        Session = sessionmaker(bind=engine)
        return Session()