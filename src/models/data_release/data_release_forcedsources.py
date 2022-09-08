from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, Boolean, BigInteger
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import sqlalchemy

Base = declarative_base()

class DataReleaseForcedSources(Base):
    __tablename__ = 'data_release_forcedsources'

    # Column defs
    edc_forcedsource_ver_id = Column(BigInteger, primary_key=True)
    forcedsourceid = Column(BigInteger)
    objectid = Column(BigInteger)
    parentobjectid = Column(BigInteger)
    coord_ra = Column(DOUBLE_PRECISION)
    coord_dec = Column(DOUBLE_PRECISION)
    skymap = Column(String(255))
    tract = Column(BigInteger)
    patch = Column(Integer)
    band = Column(String(255))
    ccdvisitid = Column(BigInteger)
    detect_ispatchinner = Column(Boolean)
    detect_isprimary = Column(Boolean)
    detect_istractinner = Column(Boolean)
    localbackground_instfluxerr = Column(DOUBLE_PRECISION)
    localbackground_instflux = Column(DOUBLE_PRECISION)
    localphotocaliberr = Column(DOUBLE_PRECISION)
    localphotocalib_flag = Column(Boolean)
    localphotocalib = Column(DOUBLE_PRECISION)
    localwcs_cdmatrix_1_1 = Column(DOUBLE_PRECISION)
    localwcs_cdmatrix_1_2 = Column(DOUBLE_PRECISION)
    localwcs_cdmatrix_2_1 = Column(DOUBLE_PRECISION)
    localwcs_cdmatrix_2_2 = Column(DOUBLE_PRECISION)
    localwcs_flag = Column(Boolean)
    pixelflags_bad = Column(Boolean)
    pixelflags_crcenter = Column(Boolean)
    pixelflags_cr = Column(Boolean)
    pixelflags_edge = Column(Boolean)
    pixelflags_interpolatedcenter = Column(Boolean)
    pixelflags_interpolated = Column(Boolean)
    pixelflags_saturatedcenter = Column(Boolean)
    pixelflags_saturated = Column(Boolean)
    pixelflags_suspectcenter = Column(Boolean)
    pixelflags_suspect = Column(Boolean)
    psfdifffluxerr = Column(DOUBLE_PRECISION)
    psfdiffflux_flag = Column(Boolean)
    psfdiffflux = Column(DOUBLE_PRECISION)
    psffluxerr = Column(DOUBLE_PRECISION)
    psfflux_flag = Column(Boolean)
    psfflux = Column(DOUBLE_PRECISION)

    def get_db_connection(db_host, db_port, db_name, db_user, db_pass):
        engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:{}/{}".format(db_user, db_pass, db_host, db_port, db_name))
        engine.dialect.description_encoding = None
        Session = sessionmaker(bind=engine)
        return Session()