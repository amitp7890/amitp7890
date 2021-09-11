import psycopg2
import psycopg2.extras
from datetime import datetime
import pandas as pd
import time
import json
import cx_Oracle
import sys
import logging
from logging.handlers import RotatingFileHandler

from configs import *

LOG_FILENAME = './aodlog.txt'
log_fmt = logging.Formatter('%(asctime)-15s :: %(name)s :: %(levelname)s :: %(message)s')
log_handler = RotatingFileHandler(LOG_FILENAME, mode='a', maxBytes=10000000,
                                  backupCount=1, encoding=None, delay=0)
log_handler.setFormatter(log_fmt)
log_handler.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)


class NDLDBConnection:

    def __init__(self):
        pass

    def get_ndl_connection(self):
        NDLconnection = None
        try:
            logger.info("Establish the connection to NDL Atoll db")
            username = credentials['NDL_PROD']['user']
            password = credentials['NDL_PROD']['password']

            dsn_tns = cx_Oracle.makedsn(credentials['NDL_PROD']['host'], credentials['NDL_PROD']['port'],
                                        credentials['NDL_PROD'][
                                            'env'])  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
            NDLconnection = cx_Oracle.connect(username, password, dsn=dsn_tns)
            logger.info("Connection Established to NDL Atoll db")
        except Exception as error:
            logger.error('Failed to connected to NDL Atoll db ::' + str(error))
            sys.exit()
        return NDLconnection

    def get_ndl_connection_stage(self):
        NDLconnection = None
        try:
            logger.info("Establish the connection to NDL Atoll db")
            username = credentials['NDL_STAGE']['user']
            password = credentials['NDL_STAGE']['password']

            dsn_tns = cx_Oracle.makedsn(credentials['NDL_STAGE']['host'],
                                        credentials['NDL_STAGE']['port'],
                                        credentials['NDL_STAGE'][
                                            'env'])  # if needed, place an 'r' before any parameter in order to address special characters such as '\'.
            NDLconnection = cx_Oracle.connect(username, password, dsn=dsn_tns)
            logger.info("Connection Established to NDL Atoll db")
        except Exception as error:
            logger.error('Failed to connected to NDL Atoll db ::' + str(error))
            sys.exit()
        return NDLconnection

    def get_tx_site(self, repeater, market, NDLconn):
        donorcellid = ''
        sitenames = ''
        try:
            print('Run queries')
            DONOR_CELLID_query = """SELECT DONOR_CELLID FROM {market}.XGREPEATERS x  WHERE TX_ID  = '{repeater}'""".format(
                market=market, repeater=repeater)
            donorcelliddf = pd.read_sql(DONOR_CELLID_query, con=NDLconn)
            print("select Query:{}".format(DONOR_CELLID_query), flush=True)
            donorcellid = donorcelliddf['DONOR_CELLID'][0]
            print("donorcellid: \n ", donorcellid)

            SITE_NAME_query = """SELECT SITE_NAME FROM {market}.XGTRANSMITTERS x2  WHERE TX_ID IN ('{repeater}', '{donorcellid}')""".format(
                market=market, repeater=repeater, donorcellid=donorcellid)
            sitenamesdf = pd.read_sql(SITE_NAME_query, con=NDLconn)
            sitenames = sitenamesdf['SITE_NAME'].tolist()
            print("sitenames: \n", sitenames)
            # sitenames = sitenamesdf['SITE_NAME']
        except Exception as error:
            logger.info("Error in get_tx_site")
            logger.error(error)
        return donorcellid, sitenames

    def getqueryresult(self, query, NDLconn, logvalue):
        df = pd.read_sql(query, con=NDLconn)
        logger.info(logvalue + ":" + query)

        if not df.empty:
            if 'MODIFIED_DATE' in df:
                df.MODIFIED_DATE = df.MODIFIED_DATE.astype(str)
                df.MODIFIED_DATE = df.MODIFIED_DATE.apply(lambda x: convert(x))
            if 'VZ_PROJECTED_ON_AIR_DATE' in df:
                df.VZ_PROJECTED_ON_AIR_DATE = df.VZ_PROJECTED_ON_AIR_DATE.astype(str)
                df.VZ_PROJECTED_ON_AIR_DATE = df.VZ_PROJECTED_ON_AIR_DATE.apply(lambda x: self.convert(x))
                # df.VZ_PROJECTED_ON_AIR_DATE = df.VZ_PROJECTED_ON_AIR_DATE.apply(lambda x: None if x in ("NaT","None") else datetime.strptime(x,'%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d'))
            if 'VZ_CELL_ON_AIR_DATE' in df:
                df.VZ_CELL_ON_AIR_DATE = df.VZ_CELL_ON_AIR_DATE.astype(str)
                df.VZ_CELL_ON_AIR_DATE = df.VZ_CELL_ON_AIR_DATE.apply(lambda x: self.convert(x))
            if 'VZ_SITE_ON_AIR_DATE' in df:
                df.VZ_SITE_ON_AIR_DATE = df.VZ_SITE_ON_AIR_DATE.astype(str)
                df.VZ_SITE_ON_AIR_DATE = df.VZ_SITE_ON_AIR_DATE.apply(lambda x: self.convert(x))
            if 'VZ_DSS_ACTIVATION_DATE' in df:
                df.VZ_DSS_ACTIVATION_DATE = df.VZ_DSS_ACTIVATION_DATE.astype(str)
                df.VZ_DSS_ACTIVATION_DATE = df.VZ_DSS_ACTIVATION_DATE.apply(lambda x: self.convert(x))

            df = df.astype(object).where(pd.notnull(df), None)

        return df

    def convert(self, x):
        date_convert = None
        if x in ("NaT", "None"):
            date_convert = None
        else:
            for fmt in ('%Y-%m-%d', '%Y-%m-%d %H:%M:%S.%f'):
                try:
                    date_convert = datetime.strptime(x, fmt).strftime('%Y-%m-%d')
                    return date_convert
                except ValueError:
                    pass
            raise ValueError('no valid date format found')
