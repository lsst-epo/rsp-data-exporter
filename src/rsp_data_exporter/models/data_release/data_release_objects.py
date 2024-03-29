from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, BigInteger
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import sqlalchemy

Base = declarative_base()

class DataReleaseObjects(Base):
    __tablename__ = 'data_release_objects'

    # Column defs
    edc_obj_ver_id = Column(BigInteger, primary_key=True)
    objectid = Column(BigInteger)
    coord_dec = Column(DOUBLE_PRECISION)
    coord_ra = Column(DOUBLE_PRECISION)
    g_ra = Column(DOUBLE_PRECISION)
    i_ra = Column(DOUBLE_PRECISION)
    r_ra = Column(DOUBLE_PRECISION)
    u_ra = Column(DOUBLE_PRECISION)
    y_ra = Column(DOUBLE_PRECISION)
    z_ra = Column(DOUBLE_PRECISION)
    g_decl = Column(DOUBLE_PRECISION)
    i_decl = Column(DOUBLE_PRECISION)
    r_decl = Column(DOUBLE_PRECISION)
    u_decl = Column(DOUBLE_PRECISION)
    y_decl = Column(DOUBLE_PRECISION)
    z_decl = Column(DOUBLE_PRECISION)
    g_bdFluxB = Column(DOUBLE_PRECISION)
    i_bdFluxB = Column(DOUBLE_PRECISION)
    r_bdFluxB = Column(DOUBLE_PRECISION)
    u_bdFluxB = Column(DOUBLE_PRECISION)
    y_bdFluxB = Column(DOUBLE_PRECISION)
    z_bdFluxB = Column(DOUBLE_PRECISION)
    g_bdFluxD = Column(DOUBLE_PRECISION)
    i_bdFluxD = Column(DOUBLE_PRECISION)
    r_bdFluxD = Column(DOUBLE_PRECISION)
    u_bdFluxD = Column(DOUBLE_PRECISION)
    y_bdFluxD = Column(DOUBLE_PRECISION)
    z_bdFluxD = Column(DOUBLE_PRECISION)
    g_bdReB = Column(DOUBLE_PRECISION)
    i_bdReB = Column(DOUBLE_PRECISION)
    r_bdReB = Column(DOUBLE_PRECISION)
    u_bdReB = Column(DOUBLE_PRECISION)
    y_bdReB = Column(DOUBLE_PRECISION)
    z_bdReB = Column(DOUBLE_PRECISION)
    g_bdReD = Column(DOUBLE_PRECISION)
    i_bdReD = Column(DOUBLE_PRECISION)
    r_bdReD = Column(DOUBLE_PRECISION)
    u_bdReD = Column(DOUBLE_PRECISION)
    y_bdReD = Column(DOUBLE_PRECISION)
    z_bdReD = Column(DOUBLE_PRECISION)