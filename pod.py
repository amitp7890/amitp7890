from flask import jsonify
from flask_cors import CORS, cross_origin
from flask import Flask, render_template, request
from datetime import datetime
import csv, io, os
import pandas as pd
import time
from contextlib import contextmanager
from multiprocessing.dummy import Pool as ThreadPool
import itertools
import json
import sys
import logging
import numpy as np
from logging.handlers import RotatingFileHandler

from configs import *
from npp_db_connection import NPPDBConnection
from ndl_db_connection import NDLDBConnection

LOG_FILENAME = './aodlog.txt'
# logging.basicConfig(level=logging.INFO, filename=LOG_FILENAME, filemode='w',
#                     format='%(asctime)s :: %(levelname)s :: %(message)s')
# add a rotating handler
# handler = RotatingFileHandler(LOG_FILENAME, maxBytes=10000000, backupCount=1)
# logging.getLogger(__name__).addHandler(handler)

log_fmt = logging.Formatter('%(asctime)-15s :: %(name)s :: %(levelname)s :: %(message)s')
log_handler = RotatingFileHandler(LOG_FILENAME, mode='a', maxBytes=10000000,
                                  backupCount=1, encoding=None, delay=0)
log_handler.setFormatter(log_fmt)
log_handler.setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

import yaml

# import sanepg2

# Flask application
app = Flask(__name__, template_folder='.')
app.config['JSON_SORT_KEYS'] = False
app.config['SECRET_KEY'] = 'coverage'
app.config['CORS_HEADERS'] = 'Content-Type'
cors = CORS(app, resources={r"/atoll/*": {"origins": "*"}})

querylist_trans = {'get_sites': "SELECT * FROM {market}.SITES where name  = '{site}' ",
                   'get_tx': "SELECT * FROM {market}.XGTRANSMITTERS x WHERE tx_id = '{tx_id}' ",
                   'get_cell_LTE': "SELECT * FROM {market}.XGCELLSLTE x WHERE tx_id = '{tx_id}' ",
                   'get_cell_NR': "SELECT * FROM {market}.XGCELLS5GNR x WHERE tx_id = '{tx_id}' "
                   }

querylist = {'get_repeater': "SELECT * FROM {market}.XGREPEATERS x WHERE tx_id = '{repeater_id}' ",
             'get_sites': "SELECT * FROM {market}.SITES where name  in {site}",
             'get_tx': "SELECT * FROM {market}.XGTRANSMITTERS x WHERE tx_id in {tx_id} ",
             'get_cell_LTE': "SELECT * FROM {market}.XGCELLSLTE x WHERE tx_id = '{tx_id}' ",
             'get_cell_NR': "SELECT * FROM {market}.XGCELLS5GNR x WHERE tx_id = '{tx_id}' "
             }


@app.route('/atoll/')
def active_server_check():
    return "Welcome to AOD"


@app.route('/atoll/stage/coveragerepeater_request', methods=['POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coveragerepeater_request_stage():
    logger.info("Request: Coverage Request Repeater")
    starttime = time.time()
    tx_names = []
    selectquery_list = []

    datas = request.get_json()
    logger.info(datas)

    for data in datas:

        market = data['market'].upper()
        logger.info(market)
        repeater = data['repeater_id']
        print(repeater)
        ndldb = NDLDBConnection()
        NDLconn = ndldb.get_ndl_connection_stage()

        donorcellid, sitenames = ndldb.get_tx_site(repeater, market, NDLconn)

        tx_names = [donorcellid, repeater]
        df_repeaterinfo = ndldb.getqueryresult(querylist['get_repeater'].format(market=market, repeater_id=repeater), NDLconn,
                                         'get repeater')
        print("df_repeaterinfo \n", df_repeaterinfo)
        df_siteinfo = ndldb.getqueryresult(querylist['get_sites'].format(market=market, site=tuple(sitenames)), NDLconn,
                                     'get sites')
        print("df_siteinfo \n", df_siteinfo)
        df_txinfo = ndldb.getqueryresult(querylist['get_tx'].format(market=market, tx_id=tuple(tx_names)), NDLconn, 'get tx')
        print("df_txinfo \n", df_txinfo)

        try:
            df_repeaterinfo = ndldb.getqueryresult(querylist['get_repeater'].format(market=market, repeater_id=repeater),
                                             NDLconn, 'get repeater')
            print("df_repeaterinfo \n", df_repeaterinfo)
            df_siteinfo = ndldb.getqueryresult(querylist['get_sites'].format(market=market, site=tuple(sitenames)), NDLconn,
                                         'get sites')
            print(df_siteinfo)
            df_txinfo = ndldb.getqueryresult(querylist['get_tx'].format(market=market, tx_id=tuple(tx_names)), NDLconn,
                                       'get tx')
            print("df_txinfo \n", df_txinfo)

            df_cell_LTE = ndldb.getqueryresult(querylist['get_cell_LTE'].format(market=market, tx_id=donorcellid), NDLconn,
                                         'get_cell_LTE')
            df_cell_NR = ndldb.getqueryresult(querylist['get_cell_NR'].format(market=market, tx_id=donorcellid), NDLconn,
                                        'get_cell_NR')
            logger.info("Select queries done!")

            result = {}

            result['repeater_id'] = data['repeater_id']
            result['market'] = data['market'].upper()
            result['request_type'] = data['request_type']
            result['request_subtype'] = data['request_subtype']
            result['output_request_list'] = data['output_request_list']
            # result['modified_attributes'] = data['modified_attributes']
            result['receiver_height'] = data['receiver_height'] if 'receiver_height' in data else ''
            result['prediction_resolution'] = data['prediction_resolution'] if 'prediction_resolution' in data else ''
            result['indoor_loss_calculations'] = data[
                'indoor_loss_calculations'] if 'indoor_loss_calculations' in data else ''
            result['product_name'] = data['product_name']
            result['modified_attributes'] = data['modified_attributes'] if 'modified_attributes' in data else {}
            result['persist_raster'] = data['persist_raster'] if 'persist_raster' in data else ''
            result['custom_doc_dir'] = data['custom_doc_dir'] if 'custom_doc_dir' in data else ''
            result['request_key'] = data['request_key'] if 'request_key' in data else ''
            result['repeater_info'] = df_repeaterinfo.to_dict('records')
            logger.info("***result['repeater_info'] {}".format(str(result['repeater_info'])))
            result['site_info'] = df_siteinfo.to_dict('records')
            logger.info("***result['site_info'] {}".format(str(result['site_info'])))
            result['tx_info'] = df_txinfo.to_dict('records')
            logger.info("***result['tx_info'] {}".format(str(result['tx_info'])))
            result['cell_LTE'] = df_cell_LTE.to_dict('records')
            logger.info("***result['cell_LTE'] {}".format(str(result['cell_LTE'])))
            result['cell_NR'] = df_cell_NR.to_dict('records')
            logger.info("***result['cell_NR'] {}".format(str(result['cell_NR'])))

            if "test" not in data['request_type']:
                status = "SENT_TO_ATOLL"
            else:
                status = "SENT_TO_ATOLL_TEST"
            nppdb = NPPDBConnection()
            connection = nppdb.get_npp_connection()
            # with get_npp_connection() as connection:
            logger.info("NPP Connection Established")
            request_id = nppdb.insert_repeater_request(data, connection)
            print("request_id:  "), request_id
            result["transaction_id"] = request_id
            print("result  : \n \n", result)
            result_json = json.dumps(result, indent=4)
            nppdb.update_record(result_json, request_id, connection, status)
            selectquery_resp = nppdb.response_json(request_id, connection)
            selectquery_list += selectquery_resp
            print("selectquery_list: \n \n", selectquery_list)

        except Exception as error:
            logger.info("Error in coverage_request")
            logger.error(error)
    return jsonify(request_obj=selectquery_list, meta={"status": "ok"})


@app.route('/atoll/coveragerepeater_request', methods=['POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coveragerepeater_request():
    logger.info("Request: Coverage Request Repeater")
    starttime = time.time()
    tx_names = []
    selectquery_list = []

    datas = request.get_json()
    logger.info(datas)

    for data in datas:

        market = data['market'].upper()
        logger.info(market)
        repeater = data['repeater_id']
        print(repeater)
        ndldb = NDLDBConnection()
        NDLconn = ndldb.get_ndl_connection()

        donorcellid, sitenames = ndldb.get_tx_site(repeater, market, NDLconn)

        tx_names = [donorcellid, repeater]
        df_repeaterinfo = ndldb.getqueryresult(querylist['get_repeater'].format(market=market, repeater_id=repeater), NDLconn,
                                         'get repeater')
        print("df_repeaterinfo \n", df_repeaterinfo)
        df_siteinfo = ndldb.getqueryresult(querylist['get_sites'].format(market=market, site=tuple(sitenames)), NDLconn,
                                     'get sites')
        print("df_siteinfo \n", df_siteinfo)
        df_txinfo = ndldb.getqueryresult(querylist['get_tx'].format(market=market, tx_id=tuple(tx_names)), NDLconn, 'get tx')
        print("df_txinfo \n", df_txinfo)

        try:
            df_repeaterinfo = ndldb.getqueryresult(querylist['get_repeater'].format(market=market, repeater_id=repeater),
                                             NDLconn, 'get repeater')
            print("df_repeaterinfo \n", df_repeaterinfo)
            df_siteinfo = ndldb.getqueryresult(querylist['get_sites'].format(market=market, site=tuple(sitenames)), NDLconn,
                                         'get sites')
            print(df_siteinfo)
            df_txinfo = ndldb.getqueryresult(querylist['get_tx'].format(market=market, tx_id=tuple(tx_names)), NDLconn,
                                       'get tx')
            print("df_txinfo \n", df_txinfo)

            df_cell_LTE = ndldb.getqueryresult(querylist['get_cell_LTE'].format(market=market, tx_id=donorcellid), NDLconn,
                                         'get_cell_LTE')
            df_cell_NR = ndldb.getqueryresult(querylist['get_cell_NR'].format(market=market, tx_id=donorcellid), NDLconn,
                                        'get_cell_NR')
            logger.info("Select queries done!")

            result = {}

            result['repeater_id'] = data['repeater_id']
            result['market'] = data['market'].upper()
            result['request_type'] = data['request_type']
            result['request_subtype'] = data['request_subtype']
            result['output_request_list'] = data['output_request_list']
            result['receiver_height'] = data['receiver_height'] if 'receiver_height' in data else ''
            result['prediction_resolution'] = data['prediction_resolution'] if 'prediction_resolution' in data else ''
            result['indoor_loss_calculations'] = data[
                'indoor_loss_calculations'] if 'indoor_loss_calculations' in data else ''
            result['product_name'] = data['product_name']
            result['modified_attributes'] = data['modified_attributes'] if 'modified_attributes' in data else {}
            result['persist_raster'] = data['persist_raster'] if 'persist_raster' in data else ''
            result['custom_doc_dir'] = data['custom_doc_dir'] if 'custom_doc_dir' in data else ''
            result['request_key'] = data['request_key'] if 'request_key' in data else ''
            result['repeater_info'] = df_repeaterinfo.to_dict('records')
            logger.info("***result['repeater_info'] {}".format(str(result['repeater_info'])))
            result['site_info'] = df_siteinfo.to_dict('records')
            logger.info("***result['site_info'] {}".format(str(result['site_info'])))
            result['tx_info'] = df_txinfo.to_dict('records')
            logger.info("***result['tx_info'] {}".format(str(result['tx_info'])))
            result['cell_LTE'] = df_cell_LTE.to_dict('records')
            logger.info("***result['cell_LTE'] {}".format(str(result['cell_LTE'])))
            result['cell_NR'] = df_cell_NR.to_dict('records')
            logger.info("***result['cell_NR'] {}".format(str(result['cell_NR'])))

            if "test" not in data['request_type']:
                status = "SENT_TO_ATOLL"
            else:
                status = "SENT_TO_ATOLL_TEST"
            nppdb = NPPDBConnection()
            connection = nppdb.get_npp_connection()
            # with get_npp_connection() as connection:
            logger.info("NPP Connection Established")
            request_id = nppdb.insert_repeater_request(data, connection)
            print("request_id:  "), request_id
            result["transaction_id"] = request_id
            print("result  : \n \n", result)
            result_json = json.dumps(result, indent=4)
            nppdb.update_record(result_json, request_id, connection, status)
            selectquery_resp = nppdb.response_json(request_id, connection)
            selectquery_list += selectquery_resp
            print("selectquery_list: \n \n", selectquery_list)

        except Exception as error:
            logger.info("Error in coverage_request")
            logger.error(error)
    return jsonify(request_obj=selectquery_list, meta={"status": "ok"})


def update_attributes(df, new_attrib):
    for k in new_attrib:
        df[k] = new_attrib[k]


@app.route('/atoll/stage/coverage_request', methods=['POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coverage_request_stage():
    logger.info("Request: Coverage Request transmitter")
    starttime = time.time()
    request_id_list = []
    selectquery_list = []

    datas = request.get_json()
    logger.info(datas)

    for data in datas:

        market = data['market'].upper()
        logger.info(market)
        print(data['tx_id'], data['site'])
        ndldb = NDLDBConnection()
        NDLconn = ndldb.get_ndl_connection_stage()

        try:
            df_siteinfo = ndldb.getqueryresult(querylist_trans['get_sites'].format(market=market, site=data['site']), NDLconn,
                                         'get sites')
            df_txinfo = ndldb.getqueryresult(querylist_trans['get_tx'].format(market=market, tx_id=data['tx_id']), NDLconn,
                                       'get tx')
            df_cell_LTE = ndldb.getqueryresult(querylist_trans['get_cell_LTE'].format(market=market, tx_id=data['tx_id']),
                                         NDLconn,
                                         'get_cell_LTE')
            df_cell_NR = ndldb.getqueryresult(querylist_trans['get_cell_NR'].format(market=market, tx_id=data['tx_id']),
                                        NDLconn,
                                        'get_cell_NR')
            logger.info("Select queries done!")
            # AZIMUT:--> tx_info , TILT:-->tx_info , azimut:-->tx_info

            print(df_txinfo)

            if 'SITES' in data['modified_attributes']:
                update_attributes(df_siteinfo, data['modified_attributes']['SITES'])
            if 'XGTRANSMITTERS' in data['modified_attributes']:
                update_attributes(df_txinfo, data['modified_attributes']['XGTRANSMITTERS'])
            if 'XGCELLSLTE' in data['modified_attributes']:
                update_attributes(df_cell_LTE, data['modified_attributes']['XGCELLSLTE'])
            if 'XGCELLSNR' in data['modified_attributes']:
                update_attributes(df_cell_LTE, data['modified_attributes']['XGCELLSNR'])

            if 'azimut' in data['modified_attributes']:
                logger.info(df_txinfo['AZIMUT'])
                df_txinfo['AZIMUT'] = data['modified_attributes']['azimut']
            if 'tilt' in data['modified_attributes']:
                logger.info(df_txinfo['TILT'])
                df_txinfo['TILT'] = data['modified_attributes']['tilt']

            print(df_txinfo)

            result = {}

            result['tx_id'] = data['tx_id']
            result['repeater_id'] = ''
            result["market"] = data['market'].upper()
            result['request_type'] = data['request_type']
            result['request_subtype'] = data['request_subtype']
            result['output_request_list'] = data['output_request_list']
            result['repeater_info'] = ''
            result['modified_attributes'] = data['modified_attributes']
            result['receiver_height'] = data['receiver_height'] if 'receiver_height' in data else ''
            result['prediction_resolution'] = data['prediction_resolution'] if 'prediction_resolution' in data else ''
            result['indoor_loss_calculations'] = data[
                'indoor_loss_calculations'] if 'indoor_loss_calculations' in data else ''
            result['persist_raster'] = data['persist_raster'] if 'persist_raster' in data else ''
            result['custom_doc_dir'] = data['custom_doc_dir'] if 'custom_doc_dir' in data else ''
            result['request_key'] = data['request_key'] if 'request_key' in data else ''
            result['site_info'] = df_siteinfo.to_dict('records')
            logger.info("***result['site_info'] {}".format(str(result['site_info'])))
            result['tx_info'] = df_txinfo.to_dict('records')
            logger.info("***result['tx_info'] {}".format(str(result['tx_info'])))
            result['cell_LTE'] = df_cell_LTE.to_dict('records')
            logger.info("***result['cell_LTE'] {}".format(str(result['cell_LTE'])))
            result['cell_NR'] = df_cell_NR.to_dict('records')
            logger.info("***result['cell_NR'] {}".format(str(result['cell_NR'])))

            if "test" not in data['request_type']:
                status = "SENT_TO_ATOLL"
            else:
                status = "SENT_TO_ATOLL_TEST"
            nppdb = NPPDBConnection()
            connection = nppdb.get_npp_connection()
            # with get_npp_connection() as connection:
            logger.info("NPP Connection Established")
            request_id = nppdb.insert_trans_request(data, connection)
            result["transaction id"] = request_id
            result_json = json.dumps(result, indent=4)
            nppdb.update_record(result_json, request_id, connection, status)
            selectquery_resp = nppdb.response_json(request_id, connection)
            selectquery_list += selectquery_resp

        except Exception as error:
            logger.info("Error in coverage_request")
            logger.error(error)
    return jsonify(request_obj=selectquery_list, meta={"status": "ok"})


@app.route('/atoll/coverage_request', methods=['POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coverage_request():
    logger.info("Request: Coverage Request transmitter")
    starttime = time.time()
    request_id_list = []
    selectquery_list = []

    datas = request.get_json()
    logger.info(datas)

    for data in datas:

        market = data['market'].upper()
        logger.info(market)
        print(data['tx_id'], data['site'])
        ndldb = NDLDBConnection()
        NDLconn = ndldb.get_ndl_connection()

        try:
            df_siteinfo = ndldb.getqueryresult(querylist_trans['get_sites'].format(market=market, site=data['site']), NDLconn,
                                         'get sites')
            df_txinfo = ndldb.getqueryresult(querylist_trans['get_tx'].format(market=market, tx_id=data['tx_id']), NDLconn,
                                       'get tx')
            df_cell_LTE = ndldb.getqueryresult(querylist_trans['get_cell_LTE'].format(market=market, tx_id=data['tx_id']),
                                         NDLconn,
                                         'get_cell_LTE')
            df_cell_NR = ndldb.getqueryresult(querylist_trans['get_cell_NR'].format(market=market, tx_id=data['tx_id']),
                                        NDLconn,
                                        'get_cell_NR')
            logger.info("Select queries done!")
            # AZIMUT:--> tx_info , TILT:-->tx_info , azimut:-->tx_info

            print(df_txinfo)

            if 'SITES' in data['modified_attributes']:
                update_attributes(df_siteinfo, data['modified_attributes']['SITES'])
            if 'XGTRANSMITTERS' in data['modified_attributes']:
                update_attributes(df_txinfo, data['modified_attributes']['XGTRANSMITTERS'])
            if 'XGCELLSLTE' in data['modified_attributes']:
                update_attributes(df_cell_LTE, data['modified_attributes']['XGCELLSLTE'])
            if 'XGCELLSNR' in data['modified_attributes']:
                update_attributes(df_cell_NR, data['modified_attributes']['XGCELLSNR'])

            if 'azimut' in data['modified_attributes']:
                logger.info(df_txinfo['AZIMUT'])
                df_txinfo['AZIMUT'] = data['modified_attributes']['azimut']
            if 'tilt' in data['modified_attributes']:
                logger.info(df_txinfo['TILT'])
                df_txinfo['TILT'] = data['modified_attributes']['tilt']

            print(df_txinfo)

            result = {}

            result['tx_id'] = data['tx_id']
            result['repeater_id'] = ''
            result["market"] = data['market'].upper()
            result['request_type'] = data['request_type']
            result['request_subtype'] = data['request_subtype']
            result['output_request_list'] = data['output_request_list']
            result['repeater_info'] = ''
            result['modified_attributes'] = data['modified_attributes'] if 'modified_attributes' in data else {}
            result['receiver_height'] = data['receiver_height'] if 'receiver_height' in data else ''
            result['prediction_resolution'] = data['prediction_resolution'] if 'prediction_resolution' in data else ''
            result['indoor_loss_calculations'] = data[
                'indoor_loss_calculations'] if 'indoor_loss_calculations' in data else ''
            result['persist_raster'] = data['persist_raster'] if 'persist_raster' in data else ''
            result['custom_doc_dir'] = data['custom_doc_dir'] if 'custom_doc_dir' in data else ''
            result['request_key'] = data['request_key'] if 'request_key' in data else ''
            result['site_info'] = df_siteinfo.to_dict('records')
            logger.info("***result['site_info'] {}".format(str(result['site_info'])))
            result['tx_info'] = df_txinfo.to_dict('records')
            logger.info("***result['tx_info'] {}".format(str(result['tx_info'])))
            result['cell_LTE'] = df_cell_LTE.to_dict('records')
            logger.info("***result['cell_LTE'] {}".format(str(result['cell_LTE'])))
            result['cell_NR'] = df_cell_NR.to_dict('records')
            logger.info("***result['cell_NR'] {}".format(str(result['cell_NR'])))
            nppdb = NPPDBConnection()
            connection = nppdb.get_npp_connection()
            # with get_npp_connection() as connection:
            logger.info("NPP Connection Established")

            if "test" not in data['request_type']:
                status = "SENT_TO_ATOLL"
            else:
                status = "SENT_TO_ATOLL_TEST"
            request_id = nppdb.insert_trans_request(data, connection)
            result["transaction id"] = request_id
            result_json = json.dumps(result, indent=4)
            nppdb.update_record(result_json, request_id, connection, status)
            selectquery_resp = nppdb.response_json(request_id, connection)
            selectquery_list += selectquery_resp

        except Exception as error:
            logger.info("Error in coverage_request")
            logger.error(error)
    return jsonify(request_obj=selectquery_list, meta={"status": "ok"})


@app.route('/atoll/coverage_status', methods=['GET', 'POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coverage_status():
    nppdb = NPPDBConnection()
    connection = nppdb.get_npp_connection()
    print("Connection Established")
    data = request.get_json()
    request_id = data['request_id']
    status_resp = nppdb.get_coverage_status(request_id, connection)

    # if status_resp.lower() != 'success':
    #     return jsonify(request_obj=None, meta={"status": status_resp.lower()})

    # query_resp = nppdb.get_data_custom_request(data, connection, request_id)
    return jsonify(request_obj=status_resp, meta={"status": "ok"})


@app.route('/atoll/atoll_methods', methods=['GET', 'POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def atoll_methods_response():
    nppdb = NPPDBConnection()
    connection = nppdb.get_npp_connection()
    print("Connection Established")
    data = request.get_json()
    request_id = data['request_id']
    status_resp = nppdb.get_status(request_id, connection)

    if status_resp.lower() != 'success':
        return jsonify(request_obj=None, meta={"status": status_resp.lower()})

    query_resp = nppdb.get_data_custom_request(data, connection, request_id)
    return jsonify(request_obj=query_resp, meta={"status": "ok"})


@app.route('/atoll/coverage_refresh', methods=['GET', 'POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coverage_response():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print(timestampStr, ":Request: Coverage Request transmitter")
    starttime = time.time()
    print("Establish the connection to db")
    nppdb = NPPDBConnection()
    connection = nppdb.get_npp_connection()
    print("Connection Established")
    data = request.get_json()
    print(data)
    selectquery_resp = nppdb.refresh_response(data['request_id_list'], connection)
    return jsonify(request_obj=selectquery_resp, meta={"status": "ok"})


@app.route('/atoll/coverage_sitetxrequest', methods=['GET', 'POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coverage_sitetxrequest():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print(timestampStr, ":Request: Coverage Request transmitter")
    starttime = time.time()
    print("Establish the connection to db")
    nppdb = NPPDBConnection()
    connection = nppdb.get_npp_connection()
    print("Connection Established")
    # cur = connection.cursor()
    data = request.get_json()
    print(data)
    query_resp = nppdb.sitetxrequest_response(data, connection)
    return jsonify(request_obj=query_resp, meta={"status": "ok"})


@app.route('/atoll/coverage_reqthreshold', methods=['GET', 'POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coverage_reqthreshold():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print(timestampStr, ":Request: Coverage Request transmitter")
    starttime = time.time()
    print("Establish the connection to db")
    nppdb = NPPDBConnection()
    connection = nppdb.get_npp_connection()
    print("Connection Established")
    # cur = connection.cursor()
    data = request.get_json()
    print(data, flush=True)
    request_id = data['request_id']
    status_resp = nppdb.get_status(request_id, connection)
    print(status_resp, flush=True)

    if status_resp.lower() != 'success':
        print("Request pending", request_id)
        return jsonify(request_obj=None, meta={"status": status_resp.lower()})

    query_resp = nppdb.reqthreshold_response(data, connection)
    return jsonify(request_obj=query_resp, meta={"status": "ok"})


@app.route('/atoll/coverage_bandrequest', methods=['GET', 'POST'])
@cross_origin(headers=['Content- Type', 'Authorization'])
def coverage_bandrequest():
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%d-%b-%Y (%H:%M:%S.%f)")
    print(timestampStr, ":Request: Coverage Request transmitter")
    starttime = time.time()
    print("Establish the connection to db")
    nppdb = NPPDBConnection()
    connection = nppdb.get_npp_connection()
    print("Connection Established")
    # cur = connection.cursor()
    data = request.get_json()
    print(data)
    query_resp = nppdb.coverage_bandrequest_response(data, connection)
    return jsonify(request_obj=query_resp, meta={"status": "ok"})


if __name__ == '__main__':
    NPPdetails = {}
    NDLdetails = {}

    # app.run( debug = True , port=8080)
    app.run(debug=True, port=10533, host='0.0.0.0')
