from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer

Base = declarative_base()

class CitizenScienceProjMetaLookup(Base):
    __tablename__ = 'citizen_science_proj_meta_lookup'

    # Column defs
    cit_sci_lookup_id = Column(Integer, primary_key=True)
    cit_sci_proj_id = Column(Integer) # , ForeignKey('citizen_science_projects.cit_sci_proj_id')
    cit_sci_meta_id = Column(Integer) # , ForeignKey('citizen_science_meta.cit_sci_meta_id')
    cit_sci_batch_id = Column(Integer) # , ForeignKey('citizen_science_batchs.cit_sci_batch_id')