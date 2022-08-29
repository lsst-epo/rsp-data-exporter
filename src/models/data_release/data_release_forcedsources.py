from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import sqlalchemy

Base = declarative_base()

class DataReleaseForcedSources(Base):
    __tablename__ = 'data_release_forcedsources'

    # Column defs
    edc_forcedsource_ver_id = Column(BigInteger, primary_key=True)
    forcedSourceId = Column(BigInteger)
    objectId = Column(BigInteger)
    parentObjectId = Column(BigInteger)
    coord_ra = Column(DOUBLE_PRECISION)
    coord_dec = Column(DOUBLE_PRECISION)
    skymap = Column(String(255))
    tract = Column(BigInteger)
    patch = Column(Integer)
    band = Column(String(255))
    ccdVisitId = Column(BigInteger)
    detect_isPatchInner = Column(Boolean)
    detect_isPrimary = Column(Boolean)
    detect_isTractInner = Column(Boolean)
    localBackground_instFluxErr = Column(DOUBLE_PRECISION)
    localBackground_instFlux = Column(DOUBLE_PRECISION)
    localPhotoCalibErr = Column(DOUBLE_PRECISION)
    localPhotoCalib_flag = Column(Boolean)
    localPhotoCalib = Column(DOUBLE_PRECISION)
    localWcs_CDMatrix_1_1 = Column(DOUBLE_PRECISION)
    localWcs_CDMatrix_1_2 = Column(DOUBLE_PRECISION)
    localWcs_CDMatrix_2_1 = Column(DOUBLE_PRECISION)
    localWcs_CDMatrix_2_2 = Column(DOUBLE_PRECISION)
    localWcs_flag = Column(Boolean)
    pixelFlags_bad = Column(Boolean)
    pixelFlags_crCenter = Column(Boolean)
    pixelFlags_cr = Column(Boolean)
    pixelFlags_edge = Column(Boolean)
    pixelFlags_interpolatedCenter = Column(Boolean)
    pixelFlags_interpolated = Column(Boolean)
    pixelFlags_saturatedCenter = Column(Boolean)
    pixelFlags_saturated = Column(Boolean)
    pixelFlags_suspectCenter = Column(Boolean)
    pixelFlags_suspect = Column(Boolean)
    psfDiffFluxErr = Column(DOUBLE_PRECISION)
    psfDiffFlux_flag = Column(Boolean)
    psfDiffFlux = Column(DOUBLE_PRECISION)
    psfFluxErr = Column(DOUBLE_PRECISION)
    psfFlux_flag = Column(Boolean)
    psfFlux = Column(DOUBLE_PRECISION)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
        engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
        engine.dialect.description_encoding = None
        Session = sessionmaker(bind=engine)
        return Session()