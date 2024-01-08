import os, csv
# from sqlalchemy import select
from google.cloud import logging

try:
    from ..models.data_release.data_release_diaobjects import DataReleaseDiaObjects
    from ..models.data_release.data_release_objects import DataReleaseObjects
    from ..models.data_release.data_release_forcedsources import DataReleaseForcedSources
except:
    try:
        from models.data_release.data_release_diaobjects import DataReleaseDiaObjects
        from models.data_release.data_release_objects import DataReleaseObjects
        from models.data_release.data_release_forcedsources import DataReleaseForcedSources
    except:
        pass

import db as DatabaseService

logging_client = logging.Client()
log_name = "rsp-data-exporter.tabular_data_service"
logger = logging_client.logger(log_name)

def create_dr_forcedsource_records(csv_path):
    csv_file = open(csv_path, "rU")
    reader = csv.DictReader(csv_file)
    logger.log_text("about to loop CSV file contents in create_dr_forcedsource_records")
    for row in reader:
        try:
            db = DatabaseService.get_db_connection()
            data_release_forcedsource_record = DataReleaseForcedSources(forcedsourceid=row["forcedSourceId"],
                                                                        objectid=row["objectId"],
                                                                        parentobjectid=row["parentObjectId"],
                                                                        coord_ra=row["coord_ra"],
                                                                        coord_dec=row["coord_dec"],
                                                                        skymap=row["skymap"],
                                                                        tract=row["tract"],
                                                                        patch=row["patch"],
                                                                        band=row["band"],
                                                                        ccdvisitid=row["ccdVisitId"],
                                                                        detect_ispatchinner=bool(row["detect_isPatchInner"]),
                                                                        detect_isprimary=bool(row["detect_isPrimary"]),
                                                                        detect_istractinner=bool(row["detect_isTractInner"]),
                                                                        localbackground_instfluxerr=row["localBackground_instFluxErr"],
                                                                        localbackground_instflux=row["localBackground_instFlux"],
                                                                        localphotocaliberr=row["localPhotoCalibErr"],
                                                                        localphotocalib_flag=bool(row["localPhotoCalib_flag"]),
                                                                        localphotocalib=row["localPhotoCalib"],
                                                                        localwcs_cdmatrix_1_1=row["localWcs_CDMatrix_1_1"],
                                                                        localwcs_cdmatrix_1_2=row["localWcs_CDMatrix_1_2"],
                                                                        localwcs_cdmatrix_2_1=row["localWcs_CDMatrix_2_1"],
                                                                        localwcs_cdmatrix_2_2=row["localWcs_CDMatrix_2_2"],
                                                                        localwcs_flag=bool(row["localWcs_flag"]),
                                                                        pixelflags_bad=bool(row["pixelFlags_bad"]),
                                                                        pixelflags_crcenter=bool(row["pixelFlags_crCenter"]),
                                                                        pixelflags_cr=bool(row["pixelFlags_cr"]),
                                                                        pixelflags_edge=bool(row["pixelFlags_edge"]),
                                                                        pixelflags_interpolatedcenter=bool(row["pixelFlags_interpolatedCenter"]),
                                                                        pixelflags_interpolated=bool(row["pixelFlags_interpolated"]),
                                                                        pixelflags_saturatedcenter=bool(row["pixelFlags_saturatedCenter"]),
                                                                        pixelflags_saturated=bool(row["pixelFlags_saturated"]),
                                                                        pixelflags_suspectcenter=bool(row["pixelFlags_suspectCenter"]),
                                                                        pixelflags_suspect=bool(row["pixelFlags_suspect"]),
                                                                        psfdifffluxerr=row["psfDiffFluxErr"],
                                                                        psfdiffflux_flag=bool(row["psfDiffFlux_flag"]),
                                                                        psfdiffflux=row["psfDiffFlux"],
                                                                        psffluxerr=row["psfFluxErr"],
                                                                        psfflux_flag=bool(row["psfFlux_flag"]),
                                                                        psfflux=row["psfFlux"])
            db.add(data_release_forcedsource_record)
            db.commit()
            db.close()
        except Exception as e:
            logger.log_text("an exception occurred in create_dr_forcedsource_records!")
            logger.log_text(e.__str__())
    return

def create_dr_diaobject_records(csv_path):
    csv_file = open(csv_path, "rU")
    reader = csv.DictReader(csv_file)
    logger.log_text("about to loop CSV file contents in create_dr_diaobject_records")
    
    for row in reader:
        try:
            db = DatabaseService.get_db_connection()
            data_release_diaobject_record = DataReleaseDiaObjects(ra=row["ra"],
                                                                  decl=row["decl"],
                                                                  rpsfluxchi2=row["rPSFluxChi2"],
                                                                  ipsfluxchi2=row["iPSFluxChi2"],
                                                                  gpsfluxchi2=row["gPSFluxChi2"],
                                                                  upsfluxchi2=row["uPSFluxChi2"],
                                                                  ypsfluxchi2=row["yPSFluxChi2"],
                                                                  zpsfluxchi2=row["zPSFluxChi2"],
                                                                  gpsfluxmax=row["gPSFluxMax"],
                                                                  ipsfluxmax=row["iPSFluxMax"],
                                                                  rpsfluxmax=row["rPSFluxMax"],
                                                                  upsfluxmax=row["uPSFluxMax"],
                                                                  ypsfluxmax=row["yPSFluxMax"],
                                                                  zpsfluxmax=row["zPSFluxMax"],
                                                                  gpsfluxmin=row["gPSFluxMin"],
                                                                  ipsfluxmin=row["iPSFluxMin"],
                                                                  rpsfluxmin=row["rPSFluxMin"],
                                                                  upsfluxmin=row["uPSFluxMin"],
                                                                  ypsfluxmin=row["yPSFluxMin"],
                                                                  zpsfluxmin=row["zPSFluxMin"],
                                                                  gpsfluxmean=row["gPSFluxMean"],
                                                                  ipsfluxmean=row["iPSFluxMean"],
                                                                  rpsfluxmean=row["rPSFluxMean"],
                                                                  upsfluxmean=row["uPSFluxMean"],
                                                                  ypsfluxmean=row["yPSFluxMean"],
                                                                  zpsfluxmean=row["zPSFluxMean"],
                                                                  gpsfluxndata=row["gPSFluxNdata"],
                                                                  ipsfluxndata=row["iPSFluxNdata"],
                                                                  rpsfluxndata=row["rPSFluxNdata"],
                                                                  upsfluxndata=row["uPSFluxNdata"],
                                                                  ypsfluxndata=row["yPSFluxNdata"],
                                                                  zpsfluxndata=row["zPSFluxNdata"])  
            db.add(data_release_diaobject_record)
            db.commit()
            db.close()
        except Exception as e:
            logger.log_text("an exception occurred in create_dr_diaobject_records!")
            logger.log_text(e.__str__())
    return

def create_dr_objects_records(csv_path):
    logger.log_text("inside of create_dr_objects_records()")
    csv_file = open(csv_path, "rU")
    reader = csv.DictReader(csv_file)
    logger.log_text("about to loop CSV file contents in create_dr_objects_records")
    for row in reader:
        try:
            db = DatabaseService.get_db_connection()
            data_release_object_record = DataReleaseObjects(objectid=row["objectId"],
                                                            coord_dec=row["coord_dec"],
                                                            coord_ra=row["Coord_ra"],
                                                            g_ra=row["g_ra"],
                                                            i_ra=row["i_ra"],
                                                            r_ra=row["r_ra"],
                                                            u_ra=row["u_ra"],
                                                            y_ra=row["y_ra"],
                                                            z_ra=row["z_ra"],
                                                            g_decl=row["g_decl"],
                                                            i_decl=row["i_decl"],
                                                            r_decl=row["r_decl"],
                                                            u_decl=row["u_decl"],
                                                            y_decl=row["y_decl"],
                                                            z_decl=row["z_decl"],
                                                            g_bdFluxB=row["g_bdFluxB"],
                                                            i_bdFluxB=row["i_bdFluxB"],
                                                            r_bdFluxB=row["r_bdFluxB"],
                                                            u_bdFluxB=row["u_bdFluxB"],
                                                            y_bdFluxB=row["y_bdFluxB"],
                                                            z_bdFluxB=row["z_bdFluxB"],
                                                            g_bdFluxD=row["g_bdFluxD"],
                                                            i_bdFluxD=row["i_bdFluxD"],
                                                            r_bdFluxD=row["r_bdFluxD"],
                                                            u_bdFluxD=row["u_bdFluxD"],
                                                            y_bdFluxD=row["y_bdFluxD"],
                                                            z_bdFluxD=row["z_bdFluxD"],
                                                            g_bdReB=row["g_bdReB"],
                                                            i_bdReB=row["i_bdReB"],
                                                            r_bdReB=row["r_bdReB"],
                                                            u_bdReB=row["u_bdReB"],
                                                            y_bdReB=row["y_bdReB"],
                                                            z_bdReB=row["z_bdReB"],
                                                            g_bdReD=row["g_bdReD"],
                                                            i_bdReD=row["i_bdReD"],
                                                            r_bdReD=row["r_bdReD"],
                                                            u_bdReD=row["u_bdReD"],
                                                            y_bdReD=row["y_bdReD"],
                                                            z_bdReD=row["z_bdReD"])    
            db.add(data_release_object_record)
            db.commit()
            db.close()
        except Exception as e:
            logger.log_text("an exception occurred in create_dr_object_records!")
            logger.log_text(e.__str__())
    return

