""" 
Copyright 2021 Province of British Columbia
WINS data transform and wrangling
author: GEE, wburt
"""

import logging
import yaml
from logging.handlers import SMTPHandler
import os
import shutil
import smtplib
import socket
import string
import sys
import time
import tempfile
from configparser import ConfigParser

import arcgis
import arcpy
from arcgis.gis import GIS
from io import StringIO
from zipfile import ZipFile

config_file = sys.argv[1]
# optional ini for credentials
if len(sys.argv) ==3:
    assert os.path.exists(sys.argv[2])
    maphub_ini = sys.argv[2]
else:
    assert 'MAPHUB_USER' in os.environ, 'User credentials not established in os.environ["MAPHUB_USER"]'

with open(config_file,"r") as yml_file:
    config = yaml.safe_load(yml_file)

app_cfg = config['app']
SMTPHost = app_cfg["smtp_host"]
From = app_cfg["app_email"]

# parameters
log_folder = '../LogFiles'
REJECT_FOLDER = '../RejectedFeatures'
WORKING_FOLDER = '../WorkingDirectory'
TEMPLATE_PATH = "../WINSfgdbTemplates"
STAGING_PATH = app_cfg["staging"]
STAGING_GDB_NAME = config["geodatabases"]["upload"]
REJECT_GDB_NAME = config['geodatabases']['reject']
TEMPLATE_GDB_NAME = config['geodatabases']['template']
FEATURE_SERVICES = config["feature_services"]
SDE_TABLE = config['sde_table']
SDE = config['geodatabases']['sde']

upload_gdb = os.path.join(WORKING_FOLDER,STAGING_GDB_NAME)
localSDEConnectionFile = os.path.split(sys.argv[0])[0] + f"\\{SDE}"
BCGWwaterPODVW = localSDEConnectionFile + f"\\{SDE_TABLE}"
local_water_pod_fc = 'WATER_POD_TABLE'
localWaterPODTable = os.path.join(upload_gdb,local_water_pod_fc)

reject_gdb = os.path.join(REJECT_FOLDER,REJECT_GDB_NAME)
template_gdb = os.path.join(TEMPLATE_PATH, TEMPLATE_GDB_NAME)
download_dir = WORKING_FOLDER
arcpy.gp.logHistory = False
server = socket.gethostname()
subject = f"WINS message from {server}"
emailList = app_cfg['dist_emails'] # list

# accomidate dual environment
if len(sys.argv)==3 and 'MAPHUB_USER' not in os.environ:
    parser = ConfigParser()
    parser.read(maphub_ini)
    url = app_cfg['bcmaphub_url']
    user = parser.get('bcmaphub','user')
    auth = parser.get('bcmaphub','password')
else:
    url = app_cfg['bcmaphub_url']
    user = os.environ['MAPHUB_USER']
    auth = os.environ['MAPHUB_PASS']

# set logging
stream = StringIO('')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_logging = logging.StreamHandler()
stream_logging = logging.StreamHandler(stream=stream)
console_logging.setLevel(logging.DEBUG)
stream_logging.setLevel(logging.INFO)
fm = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s',"%Y-%m-%d %H:%M:%S")
console_logging.setFormatter(fm)
stream_logging.setFormatter(fm)
logger.addHandler(console_logging)
logger.addHandler(stream_logging)

def wins_staging():
    ''' Downloads application data, removes unapproved features, and transforms to staging '''
    try:
        # initialize app log
        logger.info('Starting WINS Staging')
        mh = GIS(username=user,password=auth)
        logger.debug(f'Logged in as: {mh.properties.user.username}')
        item_dict = FEATURE_SERVICES
        
        download_fgdb = 'wins_downloads.gdb'
        # clear download folder
        if os.path.exists(download_dir):
            shutil.rmtree(download_dir)
            time.sleep(5) # wait for completion
        if os.path.exists(download_dir):
            logger.error(f"{download_dir} did not completed", exc_info=..., stack_info=..., extra=...)
            assert os.path.exists(download_dir) is False, f"Download directory {download_dir} failed to delete"
        os.mkdir(download_dir)
        assert os.path.exists(download_dir), f"Download directory {download_dir} did not get created"
        logger.debug(f'Download directory established: {download_dir}')
        if not os.path.exists(os.path.join(download_dir,download_fgdb)):
            gdb = arcpy.CreateFileGDB_management(download_dir,download_fgdb)
            download_fgdb = gdb.getOutput(0)
            logger.debug(f'Download gdb established: {download_fgdb}')
        for item_name in item_dict.keys():
            logger.debug(f'Starting download: {item_name} {item_dict[item_name]}')
            item = mh.content.get(item_dict[item_name])
            assert item.type == 'Feature Service'
            file_format = 'File Geodatabase'
            result = item.export(title=item_name,export_format=file_format,wait=True,overwrite=True)
            with tempfile.TemporaryDirectory() as tmpdirname:
                zfile = result.download(tmpdirname)
                with ZipFile(zfile, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
                zipped_files = [f for f in os.listdir(tmpdirname) if '.gdb' in f]
                assert len(zipped_files)==1, "Data download from maphub ({item_name}) has more than one gdb"
                gdb = zipped_files[0]
                new_gdb = shutil.move(os.path.join(tmpdirname,gdb),f"{download_dir}/{item_name}.gdb")
                arcpy.CopyFeatures_management(f"{new_gdb}/{item_name}",f"{download_fgdb}/{item_name}")
                
            result.delete()
            logger.info(f'Downloaded {item_name}')

        if arcpy.Exists(upload_gdb):
            arcpy.Delete_management(upload_gdb)
        if arcpy.Exists(reject_gdb):
            arcpy.Delete_management(reject_gdb)

        # Create the new upload gdb from the template version
        logger.info('Starting Field Calculations')
        logger.debug(f'Establishing upload gdb from template: {upload_gdb}')
        arcpy.Copy_management(template_gdb, upload_gdb)
        # Create the new reject gdb from the template version
        template_reject = os.path.join(TEMPLATE_PATH,"Water_Licensing_BCRejectTemplate.gdb")
        arcpy.Copy_management(template_reject, reject_gdb)
        logger.debug(f'Establishing reject gdb from template: {reject_gdb}')

        for fc in item_dict.keys():
            input_fc = os.path.join(download_fgdb,fc)
            output_fc = os.path.join(upload_gdb, fc)
            logger.debug(f'Appending {input_fc} to {output_fc}')
            arcpy.Append_management(input_fc,output_fc,"NO_TEST")
            if fc == "POINTS_OF_DIVERSION":
                arcpy.AddIndex_management (output_fc, "TPOD_TAG", "TPOD_INX", "NON_UNIQUE", "NON_ASCENDING")
            if fc == "NON_TRIM_HYDROGRAPHY":
                arcpy.AddIndex_management (output_fc, "TNTH_TAG", "TNTH_INX", "NON_UNIQUE", "NON_ASCENDING")
            if fc == "RESERVES_AND_RESTRICTIONS":
                arcpy.AddIndex_management (output_fc, "TRRR_TAG", "TRRR_INX", "NON_UNIQUE", "NON_ASCENDING")

        rrr_fc = os.path.join(upload_gdb,'RESERVES_AND_RESTRICTIONS')
        logger.debug('Calculating Feature codes for RESERVES_AND_RESTRICTIONS')
        if arcpy.Exists("RRR_Layer"):
            arcpy.Delete_management("RRR_Layer")
        arcpy.MakeFeatureLayer_management(rrr_fc, "RRR_Layer")
        arcpy.SelectLayerByAttribute_management("RRR_Layer", "NEW_SELECTION", "TRRR_TAG LIKE 'RV%'")
        arcpy.CalculateField_management("RRR_Layer","FEATURE_CODE","\"EA83030000\"")
        arcpy.SelectLayerByAttribute_management("RRR_Layer", "NEW_SELECTION", "TRRR_TAG LIKE 'RS%'")
        arcpy.CalculateField_management("RRR_Layer","FEATURE_CODE","\"EA83040000\"")
        arcpy.SelectLayerByAttribute_management("RRR_Layer", "CLEAR_SELECTION")
        # download and index POD data
        arcpy.CopyRows_management(BCGWwaterPODVW,localWaterPODTable)

        logger.debug('Calculating POD PNTS_DESCR to RRR field (DESCRIPTION)')
        join = arcpy.AddJoin_management("RRR_Layer", "TRRR_TAG", localWaterPODTable, "PNTS_CODE", "KEEP_ALL")
        arcpy.CalculateField_management("RRR_Layer", "RESERVES_AND_RESTRICTIONS.DESCRIPTION", f"!{local_water_pod_fc}.PNTS_DESCR!")
        arcpy.RemoveJoin_management("RRR_Layer",local_water_pod_fc)

        if arcpy.Exists("RRR_Layer"):
            arcpy.Delete_management("RRR_Layer")

        logger.debug("Setting Null values for NON_TRIM_HYDROGRAPHY")
        update_fc = os.path.join(upload_gdb,"NON_TRIM_HYDROGRAPHY")
        # Set blank TNTH_TAGs to null
        if arcpy.Exists("NTH_Layer"):
            arcpy.Delete_management("NTH_Layer")
        arcpy.MakeFeatureLayer_management(update_fc, "NTH_Layer")
        arcpy.SelectLayerByAttribute_management("NTH_Layer", "NEW_SELECTION", "TNTH_TAG = ''")
        # calculate NULL TNTH_TAG
        arcpy.CalculateField_management("NTH_Layer","TNTH_TAG",'None')
        if arcpy.Exists("NTH_Layer"): 
            arcpy.Delete_management("NTH_Layer")
        logger.debug("Setting feature code values for NON_TRIM_HYDROGRAPHY")
        # Set FEATURE CODE for NON_TRIM_HYDROGRAPHY
        arcpy.CalculateField_management(update_fc,"FEATURE_CODE","\"GA24850000\"")
        if arcpy.Exists("NTH_Layer"):
            arcpy.Delete_management("NTH_Layer")
        logger.debug("Setting STREAM_NAME values for NON_TRIM_HYDROGRAPHY from POD table")
        arcpy.MakeFeatureLayer_management(update_fc, "NTH_Layer")
        arcpy.AddJoin_management("NTH_Layer", "TNTH_TAG", localWaterPODTable, "PNTS_CODE", "KEEP_ALL")
        arcpy.CalculateField_management("NTH_Layer", "NON_TRIM_HYDROGRAPHY.STREAM_NAME", f"!{local_water_pod_fc}.SRCE_GAZETTED!")
        arcpy.RemoveJoin_management("NTH_Layer",local_water_pod_fc)
        if arcpy.Exists("NTH_Layer"):
            arcpy.Delete_management("NTH_Layer")

        updateFC = os.path.join(upload_gdb, "FLOODED_AREA_LINES")
        arcpy.CalculateField_management(updateFC,"FEATURE_CODE","\"GB11350000\"")

        # Update attributes in WATER_LICENSED_WORKS_POINTS
        updateFC = os.path.join(upload_gdb, "WATER_LICENSED_WORKS_POINTS")
        if arcpy.Exists("WRK_Layer"):
            arcpy.Delete_management("WRK_Layer")
        arcpy.MakeFeatureLayer_management(updateFC, "WRK_Layer")
        arcpy.SelectLayerByAttribute_management("WRK_Layer", "NEW_SELECTION", "TWRK_TAG = ''")
        arcpy.CalculateField_management("WRK_Layer","TWRK_TAG",'None')

        arcpy.SelectLayerByAttribute_management("WRK_Layer", "NEW_SELECTION", "FEATURE_CODE = ''")
        arcpy.CalculateField_management("WRK_Layer","FEATURE_CODE",'None')
        if arcpy.Exists("WRK_Layer"):
            arcpy.Delete_management("WRK_Layer")
        updateFC = os.path.join(upload_gdb, "WATER_LICENSED_WORKS_LINES")

        if arcpy.Exists("WRK_Layer"):
            arcpy.Delete_management("WRK_Layer")
        arcpy.MakeFeatureLayer_management(updateFC, "WRK_Layer")
        arcpy.SelectLayerByAttribute_management("WRK_Layer", "NEW_SELECTION", "TWRK_TAG = ''")
        arcpy.CalculateField_management("WRK_Layer","TWRK_TAG",'None')

        arcpy.SelectLayerByAttribute_management("WRK_Layer", "NEW_SELECTION", "FEATURE_CODE = ''")
        arcpy.CalculateField_management("WRK_Layer","FEATURE_CODE",'None')
        if arcpy.Exists("WRK_Layer"):
            arcpy.Delete_management("WRK_Layer")

        # QA
        logger.info('Starting QA and rejections')
        updateFC = os.path.join(upload_gdb, "RESERVES_AND_RESTRICTIONS")
        rejectFC = os.path.join(reject_gdb,"RESERVES_AND_RESTRICTIONS")
        TRRR_frqTable = os.path.join(reject_gdb,"TRRR_FRQ")
        if arcpy.Exists(TRRR_frqTable):
            arcpy.Delete_management(TRRR_frqTable)
        arcpy.Frequency_analysis(updateFC,TRRR_frqTable, "TRRR_TAG")
        if arcpy.Exists("RRR_Layer"):
            arcpy.Delete_management("RRR_Layer")
        arcpy.MakeFeatureLayer_management(updateFC, "RRR_Layer")

        arcpy.AddJoin_management("RRR_Layer", "TRRR_TAG", TRRR_frqTable, "TRRR_TAG", "KEEP_ALL")
        arcpy.SelectLayerByAttribute_management("RRR_Layer", "NEW_SELECTION", "FREQUENCY > 1")
        arcpy.RemoveJoin_management("RRR_Layer","TRRR_FRQ")
        descRRR = arcpy.Describe("RRR_Layer")
        if len(descRRR.FIDSet) > 0:
            logger.warning(f'RESERVES_AND_RESTRICTIONS: rejecting {len(descRRR.FIDSet)} features due to duplicate TRRR_TAG values')
            arcpy.Append_management("RRR_Layer",rejectFC,"NO_TEST")
            arcpy.DeleteFeatures_management("RRR_Layer")
            arcpy.CalculateField_management(rejectFC,"REJECT_FLAG","\"Duplicate TRRR_TAG\"")
        arcpy.SelectLayerByAttribute_management("RRR_Layer", "CLEAR_SELECTION")
        arcpy.AddJoin_management("RRR_Layer", "TRRR_TAG", localWaterPODTable, "PNTS_CODE", "KEEP_ALL")
        arcpy.SelectLayerByAttribute_management("RRR_Layer", "NEW_SELECTION", f"{local_water_pod_fc}.PNTS_CODE is null")
        arcpy.RemoveJoin_management("RRR_Layer",local_water_pod_fc)
        descRRR = arcpy.Describe("RRR_Layer")
        if len(descRRR.FIDSet) > 0:
            logger.warning(f'RESERVES_AND_RESTRICTIONS: rejecting {len(descRRR.FIDSet)} features due to TRRR_TAG values not found in POD table')
            arcpy.Append_management("RRR_Layer",rejectFC,"NO_TEST")
            arcpy.DeleteFeatures_management("RRR_Layer")
        if arcpy.Exists("RRR_Reject"):
            arcpy.Delete_management("RRR_Reject")
        arcpy.MakeFeatureLayer_management(rejectFC, "RRR_Reject","REJECT_FLAG IS null")
        arcpy.CalculateField_management("RRR_Reject","REJECT_FLAG","\"TRRR_TAG not found in Water POD Table\"")
        if arcpy.Exists("RRR_Reject"):
            arcpy.Delete_management("RRR_Reject")
        if arcpy.Exists("RRR_Layer"):
            arcpy.Delete_management("RRR_Layer")

        # clean up
        logger.debug(f'QA comlete.. Clean up')
        if arcpy.Exists(localWaterPODTable):
            arcpy.Delete_management(localWaterPODTable)

        # stage
        logger.info("Staging data...")
        StagingAreaGDB = os.path.join(STAGING_PATH,STAGING_GDB_NAME)
        if arcpy.Exists(StagingAreaGDB):
            arcpy.Delete_management(StagingAreaGDB)
        arcpy.Copy_management(upload_gdb,StagingAreaGDB)
        logger.info("Staging successful")
        logger.info('WINS Completed')
        
    except Exception as e:
        logger.exception('WINS Staging Application Exception')
    finally:
        return stream.getvalue()

def email_log(text:string,reciepients:list,sender_address:string, subject:string,smtp:string):
    # email string to recipients 
    # Send email detailing the process
    SMTPHost = smtp
    From = sender_address
    To = ', '.join(reciepients)
    msg = f"Subject: {subject} \r\nTo: {To} \r\n\r\n\r\n{text}"
    # Make the connection to the SMTP server
    s = smtplib.SMTP(SMTPHost)
    # Send the message
    s.sendmail(From,reciepients,msg)
    # Disconnect from the SMTP server
    s.quit()

if __name__ == '__main__':
    try:
        app_log = wins_staging()
        email_heading = "Quick WINS STAGING SUCCESS\n This is an automated email from the Quick WINS Staging application\n"
    except:
        logging.error('WINS STAGING FAILED', exc_info=sys.exc_info())
        app_log = stream.getvalue()
        email_heading = "Quick WINS STAGING FAILURE\n This is an automated email from the Quick WINS Staging application\n"
    finally:
        app_log = email_heading + app_log
        email_log(text=app_log, reciepients=emailList, 
            sender_address=From, subject=subject, smtp=SMTPHost)
