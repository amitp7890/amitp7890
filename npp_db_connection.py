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


class NPPDBConnection:

    def __init__(self):
        pass

    def get_npp_connection(self):
        NPPconnection = None
        try:
            logger.info("Establish the connection to NPP db")
            NPPconnection = psycopg2.connect(
                database=credentials['NPP']['database'],
                user=credentials['NPP']['user'],
                password=credentials['NPP']['password'],
                host=credentials['NPP']['host'],
                port=credentials['NPP']['port'])
            logger.info("Connection Established to NPP db")
            return NPPconnection
        except Exception as error:
            logger.error('Failed to connected to NPP db ::' + str(error))
            sys.exit()
        # finally:
        #    NPPconnection.close()

    def coverage_bandrequest_response(self, data, connection):
        logger.info(" REQUEST LIST {} ".format(data))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        selectquery_resp = ''

        if 'band' and 'tx_id' in data:
            tx_id = data['tx_id']
            band = data['band'].lower()
            tx_record_id = None
            tx_rec_query_string = ''
            if 'tx_record_id' in data:
                tx_record_id = data['tx_record_id']
                tx_rec_query_string = ' or tx_record_id = ' + tx_record_id

            logger.info(band)
            try:

                select_query = """                                
                    SELECT json_agg(jsonb_build_object( 'type', 'Feature', 'id', ogc_fid, 'geometry', ST_AsGeoJSON(geom)::jsonb, 'properties', to_jsonb(row) - 'ogc_fid' - 'geom' )) 
                    FROM (SELECT ogc_fid , threshold,db_tx_name, geom FROM radio.rlopl_Coverage_{} where ( db_tx_name = '{}' {} ) and threshold = 133) row;
                    """.format(band, tx_id, tx_rec_query_string)
                cur.execute(select_query)
                selectquery_resp = cur.fetchone()
                logger.info("select Query:{}".format(select_query))
                logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))

            except(Exception, psycopg2.DatabaseError) as error:
                logger.error('Error while selecting in selectquery_resp ')
                logger.error(error)
        else:
            logger.error('Error band and tx_id not present ')

        connection.commit()
        cur.close()

        return selectquery_resp

    def reqthreshold_response(self, data, connection):
        logger.info(" REQUEST LIST {} ".format(data))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        selectquery_resp = ''

        if 'threshold' in data:
            request_id = data['request_id']
            threshold = data['threshold']
            try:
                logger.info("threshold != None")
                select_query1 = """
                    SELECT json_agg(jsonb_build_object( 'type', 'Feature', 'id', cov_geom_map_id, 'geometry', ST_AsGeoJSON(geom)::jsonb, 'properties', to_jsonb(row)  - 'cov_geom_map_id' - 'geom' ))
                    FROM (SELECT cov_geom_map_id , threshold, geom FROM aod.transmitter_cov_geom_map where request_id = %s  and threshold =  %s ) row;
                    """
                values = (request_id, threshold)
                cur.execute(select_query1, values)
                selectquery_resp = cur.fetchone()
                logger.info("select Query:{}".format(select_query1))
                logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))

            except(Exception, psycopg2.DatabaseError) as error:
                logger.error('Error while selecting in selectquery_resp1 ')
                logger.error(error)
        else:
            try:
                request_id = data['request_id']
                logger.info("threshold = None")
                select_query2 = """
                    SELECT json_agg(jsonb_build_object( 'type', 'Feature', 'id', cov_geom_map_id, 'geometry', ST_AsGeoJSON(geom)::jsonb, 'properties', to_jsonb(row) - 'cov_geom_map_id' - 'geom' ))
                    FROM (SELECT cov_geom_map_id , threshold, geom FROM aod.transmitter_cov_geom_map where request_id = %s  ) row;
                    """
                values = (request_id,)
                cur.execute(select_query2, values)
                selectquery_resp = cur.fetchall()
                logger.info("select Query:{}".format(select_query2))
                logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))

            except(Exception, psycopg2.DatabaseError) as error:
                logger.error('Error while selecting in selectquery_resp1 ')
                logger.error(error)
        connection.commit()
        cur.close()

        return selectquery_resp

    def get_status(self, request_id, connection):
        logger.info(" REQUEST LIST {} ".format(str(request_id)))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        status = "Failed"

        try:
            select_query = """SELECT status from aod.transmitter_coverage_request where request_id = {request_id}; """.format(
                request_id=request_id)
            # values = (tuple(request_id_list))
            cur.execute(select_query)
            selectquery_resp1 = cur.fetchone()
            logger.info("select Query:{}".format(select_query))
            print("select Query:{}".format(select_query), flush=True)
            print(selectquery_resp1)
            logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))
            if not selectquery_resp1:
                logger.info("Request pending", request_id)
                status = "Failed"
            else:
                status = selectquery_resp1["status"]


        except(Exception, psycopg2.DatabaseError) as error:
            logger.error('Error while selecting in transmitter_coverage_response ')
            logger.error(error)
        connection.commit()
        cur.close()
        return status

    def get_coverage_status(self, request_id, connection):
        logger.info(" REQUEST LIST {} ".format(str(request_id)))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        try:
            select_query = """SELECT status, comments from aod.transmitter_coverage_request where request_id = {request_id}; """.format(
                request_id=request_id)
            # values = (tuple(request_id_list))
            cur.execute(select_query)
            selectquery_resp1 = cur.fetchone()
            logger.info("select Query:{}".format(select_query))
            print("select Query:{}".format(select_query), flush=True)
            print(selectquery_resp1)
            logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))
            if not selectquery_resp1:
                logger.info("Request pending", request_id)
                status = {"status": "Unknown", "comments": "Check request id"}
            else:
                if selectquery_resp1["status"].lower() == "failure":
                    status = {"status": "Failed", "comments": selectquery_resp1["comments"]}
                else:
                    status = {"status": selectquery_resp1["status"], "comments": " "}


        except(Exception, psycopg2.DatabaseError) as error:
            logger.error('Error while selecting in get_coverage_status')
            logger.error(error)
            status = {"status": "Unknown", "comments": "Error while selecting in get_coverage_status"}
        connection.commit()
        cur.close()
        return status

    def sitetxrequest_response(self, data, connection):
        logger.info(" REQUEST LIST {} ".format(data))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        selectquery_resp = ''

        if 'tx_id' in data:
            tx_id = data['tx_id']
            site = data['site']
            try:
                logger.info("tx_id != None")
                select_query1 = """SELECT request_id,tx_id, market, site, status, create_time, start_time,end_time from aod.transmitter_coverage_request where tx_id = %s and site= %s order by create_time desc; """
                values = (tx_id, site)
                cur.execute(select_query1, values)
                selectquery_resp = cur.fetchone()
                logger.info("select Query:{}".format(select_query1))
                logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))

            except(Exception, psycopg2.DatabaseError) as error:
                logger.error('Error while selecting in selectquery_resp1 ')
                logger.error(error)
        else:
            site = data['site']
            try:
                logger.info("tx_id = None")
                select_query2 = """SELECT request_id,tx_id, market, site, status, create_time, start_time, end_time from aod.transmitter_coverage_request where site= %s order by create_time desc; """
                values = (site,)
                cur.execute(select_query2, values)
                selectquery_resp = cur.fetchall()
                logger.info("select Query:{}".format(select_query2))
                logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))

            except(Exception, psycopg2.DatabaseError) as error:
                logger.error('Error while selecting in selectquery_resp1 ')
                logger.error(error)
        connection.commit()
        cur.close()
        return selectquery_resp

    def refresh_response(self, request_id_list, connection):
        logger.info(" REQUEST LIST {} ".format(str(request_id_list)))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        query_list = []

        for request_id in request_id_list:
            logger.info(request_id)
            try:
                select_query1 = """SELECT request_id,tx_id, market, site, request_type, request_subtype, status, cast(end_time-start_time AS VARCHAR) as proc_time from aod.transmitter_coverage_request where request_id = %s; """
                values = (request_id,)
                cur.execute(select_query1, values)
                selectquery_resp1 = cur.fetchone()
                logger.info("select Query:{}".format(select_query1))
                logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))
                select_query2 = """SELECT request_id,output_file_type, create_time, threshold, file_path from aod.transmitter_cov_out_map where request_id = %s; """
                values = (request_id,)
                cur.execute(select_query2, values)
                selectquery_resp2 = cur.fetchall()
                selectquery_resp1['coverage_output'] = selectquery_resp2
                query_list.append(selectquery_resp1)

            except(Exception, psycopg2.DatabaseError) as error:
                logger.error('Error while selecting in transmitter_coverage_response ')
                logger.error(error)
        connection.commit()
        cur.close()
        return query_list

    def update_record(self, result_json, request_id, connection, status):
        cur = connection.cursor()
        try:
            print("Updating..")
            update_query = """UPDATE 
                                    aod.transmitter_coverage_request 
                                set 
                                    atoll_request_object = (%s),
                                    status = (%s) 
                                where 
                                    request_id= (%s);"""
            values = (result_json, status, request_id)
            cur.execute(update_query, values)
            logger.info("Update Query:{}".format(update_query))
            logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))
            logger.info("UPDATED!!")
        except(Exception, psycopg2.DatabaseError) as error:
            logger.error('Error while updating in transmitter_coverage_response ')
            logger.error(error)
        connection.commit()
        cur.close()
        # connection.close()
        # print("Connection Closed")

    def get_data_custom_request(self, data, connection, request_id):
        logger.info(" REQUEST LIST {} ".format(data))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        selectquery_resp = ''
        try:
            select_query = """SELECT output from aod.atoll_methods_response where request_id = {request_id}""".format(
                request_id=request_id)
            cur.execute(select_query)
            selectquery_resp = cur.fetchone()
            logger.info(cur.fetchall())
            logger.info("select Query:{}".format(select_query))
            logger.info(selectquery_resp)
            logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))

        except(Exception, psycopg2.DatabaseError) as error:
            logger.error('Error while selecting in get_data_custom_request ')
            logger.error(error)

        finally:
            cur.close()
            return selectquery_resp

    def insert_repeater_request(self, data, connection):
        logger.info("Insert Query")
        cur = connection.cursor()
        try:
            insert_query = """INSERT INTO aod.transmitter_coverage_request(request_type, request_subtype, market, tx_id, user_id, product_name, request_key) values (%s,%s,%s,%s,%s,%s,%s) returning request_id;"""
            values = (
                data['request_type'], data['request_subtype'], data['market'], data['repeater_id'], data['user_id'],
                data['product_name'], data['request_key'] if 'request_key' in data else None)
            cur.execute(insert_query, values)
            logger.info("Insert Q  uery:{}".format(insert_query))
            logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))
            logger.info("INSERTED!!")
            request_id = cur.fetchone()[0]
            print("request_id: {}".format(str(request_id)))
        except(Exception, psycopg2.DatabaseError) as error:
            logger.error('Error while inserting in transmitter_coverage_response ')
            logger.error(error)
        connection.commit()
        cur.close()
        return request_id

    def insert_trans_request(self, data, connection):
        logger.info("Insert Query")
        cur = connection.cursor()
        try:
            insert_query = """INSERT INTO aod.transmitter_coverage_request(request_type, request_subtype, market, site, tx_id, user_id, product_name, request_key) 
                            values (%s,%s,%s,%s,%s,%s,%s,%s) returning request_id;"""
            values = (
                data['request_type'],
                data['request_subtype'],
                data['market'],
                data['site'],
                data['tx_id'],
                data['user_id'],
                data['product_name'],
                data['request_key'] if 'request_key' in data else None
            )
            logger.info(values)
            cur.execute(insert_query, values)
            logger.info("Insert Query:{}".format(insert_query))
            logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))
            logger.info("INSERTED!!")
            request_id = cur.fetchone()[0]
            print("request_id: {}".format(str(request_id)))
        except(Exception, psycopg2.DatabaseError) as error:
            logger.error('Error while inserting in transmitter_coverage_response ')
            logger.error(error)
        connection.commit()
        cur.close()
        return request_id

    def response_json(self, request_id, connection):
        logger.info(" REQUEST LIST {} ".format(str(request_id)))
        cur = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # cur = connection.cursor()
        query_list = []

        try:
            select_query = """SELECT request_id,tx_id, market, site, request_type, request_subtype, status, cast(end_time-start_time AS VARCHAR) as proc_time from aod.transmitter_coverage_request where request_id = %s; """
            values = (request_id,)

            cur.execute(select_query, values)
            selectquery_resp1 = cur.fetchone()
            logger.info("select Query:{}".format(select_query))
            print("select Query:{}".format(select_query), flush=True)
            logger.info(''.join(cur.query.decode("utf-8").replace('\n', '').split()))

            select_query2 = """SELECT request_id,output_file_type, create_time, threshold, file_path from aod.transmitter_cov_out_map where request_id = %s; """
            values = (request_id,)
            cur.execute(select_query2, values)
            selectquery_resp2 = cur.fetchall()
            selectquery_resp1['coverage_output'] = selectquery_resp2
            query_list.append(selectquery_resp1)


        except(Exception, psycopg2.DatabaseError) as error:
            logger.error('Error while selecting in transmitter_coverage_response ')
            logger.error(error)
        connection.commit()
        cur.close()
        return query_list
