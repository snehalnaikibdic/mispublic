import logging
import time
import copy
import json
from itertools import chain
import redis
import ast
import requests
import datetime
import traceback
import pytz
import pandas as pd
from typing import Annotated
from io import BytesIO
from fastapi import FastAPI, Response
from datetime import datetime as dt
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from decouple import config as dconfig
from sqlalchemy import text, desc
from dateutil.relativedelta import relativedelta
import config
import models

from database import SessionLocal
from sqlalchemy.orm import Session

import utils
# from invoice_registry_app.routers.ledger import get_cache
# from invoice_registry_app.views import check_ledger
from models import MerchantDetails, LenderDetails, LenderInvoiceAssociation, InvoiceEncryptedData, Ledger, \
    DisbursedHistory, RepaymentHistory
from routers.auth import get_current_merchant_active_user, User
from routers.ledger import get_cache
from schema import InvoiceRequestSchema, FinanceSchema, CancelLedgerSchema, CheckStatusSchema, AsyncFinanceSchema, \
    GetInvoiceHubMisReportSchema, GetUserMisReportSchema
import views
from errors import ErrorCodes
from utils import get_financial_year, check_invoice_date, create_post_processing, InvoiceStatus, validate_signature
from views import check_ledger
from database import get_db

# logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)
asia_kolkata = pytz.timezone('Asia/Kolkata')

class MisReport:

    def __init__(self, from_date=False, to_date=False):
        self.from_date = from_date
        self.to_date = to_date
        self.view_query = ''

    def create_all_materialized_view(self):
        try:
            logger.info(f"::: create_all_materialized_view....in one go..:::")
            self.financing_api_materialized_view()
            self.registration_api_materialized_view()
            self.disbursement_api_materialized_view()
            self.cancellation_api_materialized_view()
            self.repayment_api_materialized_view()
            self.status_check_api_materialized_view()
            self.total_calls_for_all_api_materialized_view()
            return True
        except Exception as e:
            logger.error(f"Error while creating Materialized View : {e}")
            return False

    def create_materialized_view(self):
        db = next(get_db())
        try:
            db.execute(text(self.view_query))
            db.commit()
        except Exception as e:
            logger.error(f"Error Generating Materialized View : {e}")
            return False
        return True

    def refresh_materialized_view(self):
        db = next(get_db())
        logger.info(f"::: refresh_materialized_view....in one go..:::")
        self.view_query = (
            f" refresh materialized view financing_api_materialized_view ; "
            f" refresh materialized view cancellation_api_materialized_view ; "
            f" refresh materialized view registration_api_materialized_view ; "
            f" refresh materialized view disbursement_api_materialized_view ; "
            f" refresh materialized view repayment_api_materialized_view ; "
            f" refresh materialized view invoice_repayment_percent_materialized_view ; "
            f" refresh materialized view status_check_api_materialized_view ; "
            f" refresh materialized view total_calls_for_all_api_materialized_view ; "
        )
        try:
            db.execute(text(self.view_query))
            db.commit()
        except Exception as e:
            logger.error(f"Error Refreshing Materialized View : {e}")
            return False
        return True

    def registration_api_materialized_view(self):
        logger.info(f"....registration_api_materialized_view....")
        # -- DROP VIEW IF EXISTS registration_materialized_api_view ;
        # -- CREATE OR REPLACE VIEW registration_materialized_api_view ;
        # -- drop materialized view if exists registration_api_materialized_view ;
        try:
            self.view_query = """ 
                drop materialized view if exists registration_api_materialized_view ;

                CREATE Materialized VIEW 
                    registration_api_materialized_view 
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    data.api_url,
                    data.total_incoming_ping,
                    data.entity_reg_incoming_ping,
                    data.inv_reg_with_ec_incoming_ping,
                    data.inv_reg_without_ec_incoming_ping,
                    data.total_pass,
                    data.entity_reg_pass,
                    data.inv_reg_with_ec_pass,
                    data.inv_reg_without_ec_pass,

                    data.total_fail,
                    data.entity_reg_fail,
                    data.inv_reg_with_ec_fail,
                    data.inv_reg_without_ec_fail,

                    data.total_pass_perc,
                    data.entity_reg_pass_perc,
                    data.inv_reg_with_ec_pass_perc,
                    data.inv_reg_without_ec_pass_perc,

                    -- data."% Duplicate",
                    -- data."% Repeat",

                    data.total_invoice_req,
                    data.entity_reg_invoice_req,
                    data.inv_reg_with_ec_invoice_req,
                    data.inv_reg_without_ec_invoice_req,
                    
                    data.total_invoice_pass,
                    data.entity_reg_invoice_pass,
                    data.inv_reg_with_ec_invoice_pass,
                    data.inv_reg_without_ec_invoice_pass,
                    
                    data.total_avg_inv_ping,
                    data.entity_reg_avg_inv_ping,
                    data.inv_reg_with_ec_avg_inv_ping,
                    data.inv_reg_without_ec_avg_inv_ping
                from 
                (
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    arl.api_url,

                    COUNT(arl.id) as total_incoming_ping,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('sync-entity-registration') ) AS entity_reg_incoming_ping,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('sync-invoice-registration-with-code') ) AS inv_reg_with_ec_incoming_ping,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('sync-registration-without-code') ) AS inv_reg_without_ec_incoming_ping,

                    COUNT(arl.id) Filter ( WHERE arl.response_data->>'code'::text in ('200') ) as total_pass,
                    COUNT(arl.id) Filter ( WHERE arl.api_url in ('sync-entity-registration') and arl.response_data->>'code'::text in ('200') ) as entity_reg_pass,
                    COUNT(arl.id) Filter ( WHERE arl.api_url in ('sync-invoice-registration-with-code') and arl.response_data->>'code'::text in ('200') ) as inv_reg_with_ec_pass,
                    COUNT(arl.id) Filter ( WHERE arl.api_url in ('sync-registration-without-code') and arl.response_data->>'code'::text in ('200') ) as inv_reg_without_ec_pass,

                    COUNT(arl.id) Filter ( WHERE arl.response_data = '{}'::jsonb or arl.response_data->>'code'::text not in ('200') ) as total_fail,
                    COUNT(arl.id) Filter ( WHERE arl.api_url in ('sync-entity-registration') and ( arl.response_data = '{}'::jsonb or arl.response_data->>'code'::text not in ('200') ) ) as entity_reg_fail,
                    COUNT(arl.id) Filter ( WHERE arl.api_url in ('sync-invoice-registration-with-code') and (arl.response_data = '{}'::jsonb or arl.response_data->>'code'::text not in ('200') ) ) as inv_reg_with_ec_fail,
                    COUNT(arl.id) Filter ( WHERE arl.api_url in ('sync-registration-without-code') and (arl.response_data = '{}'::jsonb or arl.response_data->>'code'::text not in ('200')) ) as inv_reg_without_ec_fail,
                    
                    Round( ( case when count(arl.id)  != 0
                                  then ( count( case when arl.response_data->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                       / 
                                       count(arl.id) 
                                       ) * 100 
                                  else 0 
                                  END)::numeric, 2
                    ) AS total_pass_perc,
                    
                    Round( ( case when count(arl.id) FILTER (WHERE arl.api_url = 'sync-entity-registration') != 0
                                  then ( count( case when arl.api_url = 'sync-entity-registration' AND arl.response_data->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                         / 
                                         count(arl.id) FILTER (WHERE arl.api_url = 'sync-entity-registration')
                                        ) * 100
                                  else 0
                                  END)::numeric, 2 
                    ) AS entity_reg_pass_perc,
                    
                    Round( ( case when count(arl.id) FILTER (WHERE arl.api_url = 'sync-invoice-registration-with-code') != 0
                                  then ( count( case when arl.api_url = 'sync-invoice-registration-with-code' AND arl.response_data->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                         / 
                                         count(arl.id) FILTER (WHERE arl.api_url = 'sync-invoice-registration-with-code')
                                        ) * 100
                                  else 0
                                  END)::numeric, 2
                    ) AS inv_reg_with_ec_pass_perc,
                    
                    Round( ( case when count(arl.id) FILTER (WHERE arl.api_url = 'sync-registration-without-code') != 0
                                  then ( count( case when arl.api_url = 'sync-registration-without-code' AND arl.response_data->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                       / 
                                       count(arl.id) FILTER (WHERE arl.api_url = 'sync-registration-without-code')
                                       ) * 100
                                  else 0
                                  END)::numeric, 2
                    ) AS inv_reg_without_ec_pass_perc,

                    -- ''::text AS "% Duplicate",
                    -- ''::text AS "% Repeat", 

                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')), 0) AS total_invoice_req,
                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-entity-registration') ), 0) AS entity_reg_invoice_req,
                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-invoice-registration-with-code') ), 0) AS inv_reg_with_ec_invoice_req,
                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-registration-without-code') ), 0) AS inv_reg_without_ec_invoice_req,

                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')), 0) AS total_invoice_pass,
                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-entity-registration') and arl.response_data->>'code'::text in ('200') ), 0) AS entity_reg_invoice_pass,
                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-invoice-registration-with-code') and arl.response_data->>'code'::text in ('200') ), 0) AS inv_reg_with_ec_invoice_pass,
                    coalesce( sum(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-registration-without-code') and arl.response_data->>'code'::text in ('200') ), 0) AS inv_reg_without_ec_invoice_pass,

                    Round( ( sum( jsonb_array_length(arl.request_data->'ledgerData') )
                             / COUNT(arl.id) )::numeric, 2
                    ) AS total_avg_inv_ping,
                    Round( ( sum( jsonb_array_length(arl.request_data->'ledgerData') ) FILTER ( WHERE arl.api_url in ('sync-entity-registration') ) 
                             / COUNT(arl.id) FILTER ( WHERE arl.api_url in ('sync-entity-registration') ) )::numeric, 2
                    ) AS entity_reg_avg_inv_ping,
                    Round( ( sum( jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-invoice-registration-with-code') ) 
                            / COUNT(arl.id) FILTER ( WHERE arl.api_url in ('sync-invoice-registration-with-code') ) )::numeric, 2
                    ) AS inv_reg_with_ec_avg_inv_ping,
                    Round( ( sum( jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('sync-registration-without-code') ) 
                            / COUNT(arl.id) FILTER ( WHERE arl.api_url in ('sync-registration-without-code') ) )::numeric, 2
                    ) AS inv_reg_without_ec_avg_inv_ping
                from
                    api_request_log arl
                inner join merchant_details md on md.id::text = arl.merchant_id
                where
                    arl.api_url in ( 'sync-entity-registration', 
                                     'sync-registration-without-code', 
                                     'sync-invoice-registration-with-code'
                                    ) 
                group by 
                    md.name, md.unique_id, period, arl.api_url

                union all 

                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(ppr.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    ppr.type as api_url,

                    COUNT(ppr.id) as total_incoming_ping,
                    COUNT(ppr.id) FILTER ( WHERE ppr.type in ('async_entity_registration') ) AS entity_reg_incoming_ping,
                    COUNT(ppr.id) FILTER ( WHERE ppr.type in ('async_invoice_registration_with_code') ) AS inv_reg_with_ec_incoming_ping,
                    COUNT(ppr.id) FILTER ( WHERE ppr.type in ('async_registration_without_code') ) AS inv_reg_without_ec_incoming_ping,

                    COUNT(ppr.id) Filter ( WHERE ppr.webhook_response->>'code'::text in ('200') ) as total_pass,
                    COUNT(ppr.id) Filter ( WHERE ppr.type in ('async_entity_registration') and ppr.webhook_response->>'code'::text in ('200') ) as entity_reg_pass,
                    COUNT(ppr.id) Filter ( WHERE ppr.type in ('async_invoice_registration_with_code') and ppr.webhook_response->>'code'::text in ('200') ) as inv_reg_with_ec_pass,
                    COUNT(ppr.id) Filter ( WHERE ppr.type in ('async_registration_without_code') and ppr.webhook_response->>'code'::text in ('200') ) as inv_reg_without_ec_pass,

                    COUNT(ppr.id) Filter ( WHERE ppr.webhook_response->>'code'::text not in ('200') ) as total_fail,
                    COUNT(ppr.id) Filter ( WHERE ppr.type in ('async_entity_registration') and ppr.webhook_response->>'code'::text not in ('200') ) as entity_reg_fail,
                    COUNT(ppr.id) Filter ( WHERE ppr.type in ('async_invoice_registration_with_code') and ppr.webhook_response->>'code'::text not in ('200') ) as inv_reg_with_ec_fail,
                    COUNT(ppr.id) Filter ( WHERE ppr.type in ('async_registration_without_code') and ppr.webhook_response->>'code'::text not in ('200') ) as inv_reg_without_ec_fail,
                    
                    Round( ( case when count(ppr.id)  != 0
                                 then ( count( case when ppr.webhook_response->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                        / 
                                        count(ppr.id) 
                                       ) * 100
                                else 0
                                END)::numeric, 2
                    ) AS total_pass_perc,
                    
                    Round( ( case when count(ppr.id) FILTER (WHERE ppr.type = 'async_entity_registration') != 0
                                 then ( count( case when ppr.type = 'async_entity_registration' AND ppr.webhook_response->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                        / 
                                        count(ppr.id) FILTER (WHERE ppr.type = 'async_entity_registration')
                                       ) * 100
                                 else 0
                                 END)::numeric, 2
                    ) AS entity_reg_pass_perc,
                    
                    Round( ( case when count(ppr.id) FILTER (WHERE ppr.type = 'async_invoice_registration_with_code') != 0
                                 then ( count( case when ppr.type = 'async_invoice_registration_with_code' AND ppr.webhook_response->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                        / 
                                        count(ppr.id) FILTER (WHERE ppr.type = 'async_invoice_registration_with_code')
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS inv_reg_with_ec_pass_perc,
                    
                    Round( ( case when count(ppr.id) FILTER (WHERE ppr.type = 'async_registration_without_code') != 0
                                 then ( count( case when ppr.type = 'async_registration_without_code' AND ppr.webhook_response->>'code'::TEXT = '200' then 1 end )::FLOAT 
                                        / 
                                        count(ppr.id) FILTER (WHERE ppr.type = 'async_registration_without_code')
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS inv_reg_without_ec_pass_perc,

                    -- ''::text AS "% Duplicate",
                    -- ''::text AS "% Repeat", 

                    coalesce( sum(jsonb_array_length(ppr.request_extra_data->'ledgerData')), 0) AS total_invoice_req,
                    coalesce( sum(jsonb_array_length(ppr.request_extra_data->'ledgerData')) FILTER ( WHERE ppr.type in ('async_entity_registration') ), 0) AS entity_reg_invoice_req,
                    coalesce( sum(jsonb_array_length(ppr.request_extra_data->'ledgerData')) FILTER ( WHERE ppr.type in ('async_invoice_registration_with_code') ), 0) AS inv_reg_with_ec_invoice_req,
                    coalesce( sum(jsonb_array_length(ppr.request_extra_data->'ledgerData')) FILTER ( WHERE ppr.type in ('async_registration_without_code') ), 0) AS inv_reg_without_ec_invoice_req,

                    coalesce( sum( jsonb_array_length(ppr.request_extra_data->'ledgerData') ), 0) AS total_invoice_pass,
                    coalesce( sum( jsonb_array_length(ppr.request_extra_data->'ledgerData') ) FILTER ( WHERE ppr.type in ('async_entity_registration') and ppr.webhook_response->>'code'::text in ('200') ), 0) AS entity_reg_invoice_pass,
                    coalesce( sum( jsonb_array_length(ppr.request_extra_data->'ledgerData') ) FILTER ( WHERE ppr.type in ('async_invoice_registration_with_code') and ppr.webhook_response->>'code'::text in ('200') ), 0) AS inv_reg_with_ec_invoice_pass,
                    coalesce( sum( jsonb_array_length(ppr.request_extra_data->'ledgerData') ) FILTER ( WHERE ppr.type in ('async_registration_without_code') and ppr.webhook_response->>'code'::text in ('200') ), 0) AS inv_reg_without_ec_invoice_pass,

                    Round( ( sum( jsonb_array_length(ppr.request_extra_data->'ledgerData') ) 
                             / COUNT(ppr.id) )::numeric, 2
                    ) AS total_avg_inv_ping,
                    Round( ( sum( jsonb_array_length( ppr.request_extra_data->'ledgerData') ) FILTER ( WHERE ppr.type in ('async_entity_registration') ) 
                           / COUNT(ppr.id) FILTER ( WHERE ppr.type in ('async_entity_registration') ) )::numeric, 2                        
                    ) AS entity_reg_avg_inv_ping,
                    Round( ( sum( jsonb_array_length(ppr.request_extra_data->'ledgerData') ) FILTER ( WHERE ppr.type in ('async_invoice_registration_with_code') ) 
                           / COUNT(ppr.id) FILTER ( WHERE ppr.type in ('async_invoice_registration_with_code') ) )::numeric, 2
                    ) AS inv_reg_with_ec_avg_inv_ping,
                    Round( ( sum( jsonb_array_length(ppr.request_extra_data->'ledgerData') ) FILTER ( WHERE ppr.type in ('async_registration_without_code') ) 
                           / COUNT(ppr.id) FILTER ( WHERE ppr.type in ('async_registration_without_code') ) )::numeric, 2
                    ) AS inv_reg_without_ec_avg_inv_ping
                from
                    post_processing_request ppr
                inner join merchant_details md on md.id::text = ppr.merchant_id
                where
                    ppr.type in ( 'async_entity_registration',
                                  'async_registration_without_code',
                                  'async_invoice_registration_with_code', 
                                  'async_bulk_registration_with_code',  
                                  'async_bulk_registration_without_codes'
                                ) 
                group by 
                    md.name, md.unique_id, period, ppr.type
                ) AS data 
                order by data.period desc
            """
            # 'Registration API',
            # 'Entity Registration',
            # 'Invoice Registration with Entity Code',
            # 'Invoice Registration without Entity Code'
            self.create_materialized_view()
        except Exception as e:
            logger.error(f"Exception registration_api_materialized_view :: {e}")
            pass

    def financing_api_materialized_view(self):
        logger.info(f"....financing_api_materialized_view....")
        # --TO_CHAR(TIMEZONE('Asia/Kolkata', arl.created_at), 'DD/MM/YYYY') = '29-02-2024'
        # --TO_CHAR(TIMEZONE('Asia/Kolkata', arl.created_at), 'DD/MM/YYYY') = (:parsed_date)::text
        # str_date = datetime.datetime.strptime(request_data.get('date'), '%d/%m/%Y').strftime('%d-%m-%Y')

        # -- DROP VIEW IF EXISTS financing_materialized_api_view ;
        # -- CREATE OR REPLACE VIEW financing_materialized_api_view ;
        # -- drop materialized view if exists financing_materialized_api_view ;
        try:
            self.view_query = """ 
                drop materialized view if exists financing_api_materialized_view ;
                                
                create materialized view 
                    financing_api_materialized_view 
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    data.api_url,
                    data.total_of_request,
                    data.f_with_ec_of_request,
                    data.f_without_ec_of_request,
                    
                    coalesce(data.total_successful, 0) as total_successful,
                    data.f_with_ec_successful,
                    data.f_without_ec_successful,
                    
                    data.funding_ok_perc as "Funding Ok %",
                    
                    data.total_repeat_perc,
                    data.f_with_ec_repeat_perc,
                    data.f_without_ec_repeat_perc,
                    
                    data.total_duplicate_perc,
                    data.f_with_ec_duplicate_perc,
                    data.f_without_ec_duplicate_perc,
                    
                    data.total_amount_of_request,
                    data.f_with_ec_amount_of_request,
                    data.f_without_ec_amount_of_request,
                    
                    coalesce(data.total_invoices_request, 0) as total_invoices_request,
                    data.f_with_ec_invoices_request,
                    data.f_without_ec_invoices_request,
                    
                    data."% of Invoices Ok",
                    data."Amount Ok for funding",
                    data."% Funding Value"
                from 
                (
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    arl.api_url,
                    COUNT(arl.id) as total_of_request,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') ) AS f_with_ec_of_request,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') ) AS f_without_ec_of_request,

                    COUNT(arl.id) FILTER ( WHERE arl.response_data->>'code' in ('200', '1013') ) AS total_successful,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') and arl.response_data->>'code' in ('200', '1013') ) AS f_with_ec_successful,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') and arl.response_data->>'code' in ('200', '1013') ) AS f_without_ec_successful,
                    
                    Round( ( case when SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') and arl.response_data->>'code' in ('200', '1013') ) != 0
                                 then (  SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') and arl.response_data->>'code' in ('200', '1013') )
                                        / 
                                        SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') )
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS funding_ok_perc,
                    
                    case when ( COUNT(arl.id) ) != 0
                         then ( COUNT(arl.id) FILTER ( WHERE arl.response_data->>'code' not in ('200', '1013') ) / COUNT(arl.id) ) * 100
                         else 0
                    end AS total_repeat_perc,
                    
                    case when ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') ) ) != 0
                         then ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') and arl.response_data->>'code' not in ('200', '1013') ) / COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') ) ) * 100
                         else 0
                    end  AS f_with_ec_repeat_perc,
                    
                    case when ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') ) ) !=0
                         then ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') and arl.response_data->>'code' not in ('200', '1013') ) / COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') ) ) * 100
                         else 0
                    end AS f_without_ec_repeat_perc,
                    
                    case when ( COUNT(arl.id) ) != 0
                         then ( COUNT(arl.id) FILTER ( WHERE arl.response_data->>'code' not in ('200', '1013') ) /  COUNT(arl.id) ) * 100
                         else 0 
                    end AS total_duplicate_perc,
                    
                    case when ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') ) ) != 0
                         then ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') and arl.response_data->>'code' not in ('200', '1013') ) / COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') ) ) * 100 
                         else 0
                    end AS f_with_ec_duplicate_perc,
                    
                    case when ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') ) ) !=0
                         then ( COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') and arl.response_data->>'code' not in ('200', '1013') ) / COUNT(arl.id) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') ) ) * 100  
                         else 0
                    end AS f_without_ec_duplicate_perc,
                    
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(arl.request_data->'ledgerData') AS item ) ), 0) AS total_amount_of_request,
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(arl.request_data->'ledgerData') AS item )) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') ) , 0) AS f_with_ec_amount_of_request,
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(arl.request_data->'ledgerData') AS item )) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') ), 0) AS f_without_ec_amount_of_request,
                     
                    SUM(jsonb_array_length(arl.request_data->'ledgerData')) AS total_invoices_request,
                    SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('Financing API with Entity Code', 'syncFinancing') ) AS f_with_ec_invoices_request,
                    SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('Financing API without Entity Code', 'syncFinancing') ) AS f_without_ec_invoices_request,
                    
                    Round( ( case when SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') and arl.response_data->>'code' in ('200', '1013') ) != 0
                                 then (  SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') and arl.response_data->>'code' in ('200', '1013') )
                                        / 
                                        SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') )
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS "% of Invoices Ok", 
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(arl.request_data->'ledgerData') AS item ) ), 0) AS "Amount Ok for funding",
                    Round( ( case when SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') and arl.response_data->>'code' in ('200', '1013') ) != 0
                                 then (  SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') and arl.response_data->>'code' in ('200', '1013') )
                                        / 
                                        SUM(jsonb_array_length(arl.request_data->'ledgerData')) FILTER ( WHERE arl.api_url in ('syncFinancing') )
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS "% Funding Value"
                from
                    api_request_log arl
                inner join merchant_details md on md.id::text = arl.merchant_id
                where
                    arl.api_url in ( 'syncFinancing', 
                                     'Financing API with Entity Code', 'Financing API without Entity Code'
                                    ) 
                group by 
                    md.id, md.name, md.unique_id, period, arl.api_url
                
                union all
                
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(ppr.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    ppr.type as api_url,
                    COUNT(ppr.id) as total_of_request,
                    COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') ) AS f_with_ec_of_request,
                    COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') ) AS f_without_ec_of_request,

                    COUNT(ppr.id) FILTER ( WHERE ppr.webhook_response->>'code' in ('200', '1013') ) AS total_successful,
                    COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') ) AS f_with_ec_successful,
                    COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') ) AS f_without_ec_successful,
                    
                    Round( ( case when SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') ) != 0
                                 then (  SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') )
                                        / 
                                        SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') )
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS funding_ok_perc,
                    
                    case when ( COUNT(ppr.id) ) != 0
                         then ( COUNT(ppr.id) FILTER ( WHERE ppr.webhook_response->>'code' not in ('200', '1013') ) / COUNT(ppr.id) ) * 100
                         else 0
                    end AS total_repeat_perc,
                    
                    case when ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') ) ) != 0
                         then ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') and ppr.webhook_response->>'code' not in ('200', '1013') ) / COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') ) ) * 100
                         else 0
                    end  AS f_with_ec_repeat_perc,
                    
                    case when ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') ) ) !=0
                         then ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') and ppr.webhook_response->>'code' not in ('200', '1013') ) / COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') ) ) * 100
                         else 0
                    end AS f_without_ec_repeat_perc,
                    
                    case when ( COUNT(ppr.id) ) != 0
                         then ( COUNT(ppr.id) FILTER ( WHERE ppr.webhook_response->>'code' not in ('200', '1013') ) /  COUNT(ppr.id) ) * 100
                         else 0 
                    end AS total_duplicate_perc,
                    
                    case when ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') ) ) != 0
                         then ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') and ppr.webhook_response->>'code' not in ('200', '1013') ) / COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') ) ) * 100 
                         else 0
                    end AS f_with_ec_duplicate_perc,
                    
                    case when ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') ) ) !=0
                         then ( COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') and ppr.webhook_response->>'code' not in ('200', '1013') ) / COUNT(ppr.id) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') ) ) * 100  
                         else 0
                    end AS f_without_ec_duplicate_perc,
                    
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(ppr.webhook_response->'ledgerData') AS item ) ), 0) AS total_amount_of_request,
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(ppr.webhook_response->'ledgerData') AS item )) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') ) , 0) AS f_with_ec_amount_of_request,
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(ppr.webhook_response->'ledgerData') AS item )) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') ), 0) AS f_without_ec_amount_of_request,
                     
                    SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) AS total_invoices_request,
                    SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('Financing API with Entity Code', 'asyncFinancing') ) AS f_with_ec_invoices_request,
                    SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('Financing API without Entity Code', 'asyncFinancing') ) AS f_without_ec_invoices_request,
                    
                    Round( ( case when SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') ) != 0
                                 then (  SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') )
                                        / 
                                        SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') )
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS "% of Invoices Ok",
                    coalesce( sum( ( SELECT SUM((item->>'invoiceAmt')::numeric) FROM jsonb_array_elements(ppr.webhook_response->'ledgerData') AS item ) ), 0) AS "Amount Ok for funding",   
                    Round( ( case when SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') ) != 0
                                 then (  SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') and ppr.webhook_response->>'code' in ('200', '1013') )
                                        / 
                                        SUM(jsonb_array_length(ppr.webhook_response->'ledgerData')) FILTER ( WHERE ppr.type in ('asyncFinancing') )
                                       ) * 100
                            else 0
                            END)::numeric, 2
                    ) AS "% Funding Value"
                from
                    post_processing_request ppr
                inner join merchant_details md on md.id::text = ppr.merchant_id
                where
                    ppr.type in ( 'asyncFinancing', 'Financing API with Entity Code', 'Financing API without Entity Code') 
                group by 
                    md.id, md.name, md.unique_id, period, ppr.type
                
                ) AS data
                order by data.period desc
                ;
            """

            self.create_materialized_view()
            # '''
            # create materialized view
            #         financing_api_materialized_view
            #     as
            #     select
            #         data.category,
            #         data.idp_name,
            #         data.idp_code,
            #         data.created_date,
            #         data.period,
            #         data.api_url,
            #         data."# Of Request",
            #         data."# Successful",
            #         data."Funding Ok %",
            #         data."Repeat %",
            #         data."Duplicate%",
            #         data."Amount of Request",
            #         data."# Invoices Request",
            #         data."% of Invoices Ok",
            #         data."Amount Ok for funding",
            #         data."% Funding Value"
            #     from
            #     (
            #     select
            #         md.username as category,
            #         md.name as idp_name,
            #         md.unique_id as idp_code,
            #         to_date(to_char(TIMEZONE('Asia/Kolkata', arl.created_at), 'DD/MM/YYYY'), 'DD/MM/YYYY') as created_date,
            #         ''::text as period,
            #         arl.api_url,
            #         (   select
            #                 Count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'asyncFinancing')
            #         ) AS "# Of Request",
            #         (   select
            #                 Count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'asyncFinancing') AND
            #                 arl_inner.response_data->>'code' in ('200', '1013')
            #         ) AS "# Successful",
            #         ''::text AS "Funding Ok %",
            #         (   select
            #                 count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #           where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'asyncFinancing') AND
            #                 arl_inner.response_data->>'code' not in ('200', '1013')
            #         ) as "Repeat %",
            #         (   select
            #                 count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'asyncFinancing') AND
            #                 arl_inner.response_data->>'code' NOT IN ('200', '1013')
            #         ) AS "Duplicate%",
            #         '0'::text as "Amount of Request",
            #         (   select
            #                 SUM(jsonb_array_length(arl_inner.request_data->'ledgerData'))
            #             from
            #                 api_request_log as arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'asyncFinancing') AND
            #                 arl_inner.response_data->>'code' not in ('200', '1013') AND
            #                 arl_inner.request_data->'ledgerData' is not null AND
            #                 jsonb_array_length(arl_inner.request_data->'ledgerData') > 0
            #         ) AS "# Invoices Request",
            #         ''::text AS "% of Invoices Ok",
            #         '0'::text AS "Amount Ok for funding",
            #         ''::text AS "% Funding Value"
            #     from
            #         api_request_log arl
            #     inner join merchant_details md on md.id::text = arl.merchant_id
            #     where
            #         arl.api_url in ('syncFinancing', 'asyncFinancing')
            #     group by
            #         md.id, md.username, md.name, md.unique_id, created_date, 5, 6, 7, 8
            #     union all
            #     select
            #         md.username as category,
            #         md.name as idp_name,
            #         md.unique_id as idp_code,
            #         to_date(to_char(TIMEZONE('Asia/Kolkata', arl.created_at), 'DD/MM/YYYY'), 'DD/MM/YYYY') as created_date,
            #         ''::text as period,
            #         arl.api_url,
            #         (   select
            #                 Count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API with Entity Code')
            #         ) AS "# Of Request",
            #         (   select
            #                 Count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API with Entity Code') AND
            #                 arl_inner.response_data->>'code' in ('200', '1013')
            #         ) AS "# Successful",
            #         ''::text AS "Funding Ok %",
            #         (   select
            #                 count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API with Entity Code') AND
            #                 arl_inner.response_data->>'code' not in ('200', '1013')
            #         ) as "Repeat %",
            #         (   select
            #                 count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API with Entity Code') AND
            #                 arl_inner.response_data->>'code' NOT IN ('200', '1013')
            #         ) AS "Duplicate%",
            #         '0'::text as "Amount of Request",
            #         (   select
            #                 SUM(jsonb_array_length(arl_inner.request_data->'ledgerData'))
            #             from
            #                 api_request_log as arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API with Entity Code') AND
            #                 arl_inner.response_data->>'code' not in ('200', '1013') AND
            #                 arl_inner.request_data->'ledgerData' is not null AND
            #                 jsonb_array_length(arl_inner.request_data->'ledgerData') > 0
            #         ) AS "# Invoices Request",
            #         ''::text AS "% of Invoices Ok",
            #         '0'::text AS "Amount Ok for funding",
            #         ''::text AS "% Funding Value"
            #     from
            #         api_request_log arl
            #     inner join merchant_details md on md.id::text = arl.merchant_id
            #     where
            #         arl.api_url in ('syncFinancing', 'Financing API with Entity Code')
            #     group by
            #         md.id, md.username, md.name, md.unique_id, created_date, 5, 6, 7, 8
            #     union all
            #     select
            #         md.username as category,
            #         md.name as idp_name,
            #         md.unique_id as idp_code,
            #         to_date(to_char(TIMEZONE('Asia/Kolkata', arl.created_at), 'DD/MM/YYYY'), 'DD/MM/YYYY') as created_date,
            #         ''::text as period,
            #         arl.api_url,
            #         (   select
            #                 Count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API without Entity Code')
            #         ) AS "# Of Request",
            #         (   select
            #                 count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API without Entity Code') AND
            #                 arl_inner.response_data->>'code' in ('200', '1013')
            #         ) as "# Successful",
            #         ''::text AS "Funding Ok %",
            #         (   select
            #                 count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API without Entity Code') AND
            #                 arl_inner.response_data->>'code' not in ('200', '1013')
            #         ) as "Repeat %",
            #         (   select
            #                 count(arl_inner.id)
            #             from
            #                 api_request_log arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API without Entity Code') AND
            #                 arl_inner.response_data->>'code' NOT IN ('200', '1013')
            #         ) AS "Duplicate%",
            #         '0'::text as "Amount of Request",
            #         (   select
            #                 SUM(jsonb_array_length(arl_inner.request_data->'ledgerData'))
            #             from
            #                 api_request_log as arl_inner
            #             where
            #                 arl_inner.merchant_id = md.id::text AND
            #                 arl_inner.api_url in ('syncFinancing', 'Financing API without Entity Code') AND
            #                 arl_inner.response_data->>'code' not in ('200', '1013') AND
            #                 arl_inner.request_data->'ledgerData' is not null AND
            #                 jsonb_array_length(arl_inner.request_data->'ledgerData') > 0
            #         ) AS "# Invoices Request",
            #         ''::text AS "% of Invoices Ok",
            #         '0'::text AS "Amount Ok for funding",
            #         ''::text AS "% Funding Value"
            #     from
            #         api_request_log arl
            #     inner join merchant_details md on md.id::text = arl.merchant_id
            #     where
            #         arl.api_url in ('syncFinancing', 'Financing API without Entity Code')
            #     group by
            #         md.id, md.username, md.name, md.unique_id, created_date, 5, 6, 7, 8
            #     ) AS data
            # '''
        except Exception as e:
            logger.error(f"Exception financing_api_materialized_view :: {e}")
            pass

    def cancellation_api_materialized_view(self):
        logger.info(f"....cancellation_api_materialized_view....")
        # -- DROP VIEW IF EXISTS cancellation_materialized_api_view ;
        # -- CREATE OR REPLACE VIEW cancellation_materialized_api_view ;
        # -- drop materialized view if exists cancellation_api_materialized_view ;
        try:
            self.view_query = """ 
                drop materialized view if exists cancellation_api_materialized_view ;
                
                create materialized view 
                    cancellation_api_materialized_view 
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    data.api_url,
                    
                    sum(data.total_request) as total_request,
                    sum(data.ledger_cancel_total_request) as ledger_cancel_total_request,
                    sum(data.inv_cancel_total_request) as inv_cancel_total_request,
                    
                    sum(data.total_successful) as total_successful, 
                    sum(data.ledger_cancel_successful) as ledger_cancel_successful,
                    sum(data.inv_cancel_successful) as inv_cancel_successful,
                    
                    sum(data.total_invoice_requested) as total_invoice_requested, 	
                    sum(data.ledger_cancel_invoice_requested) as ledger_cancel_invoice_requested,
                    sum(data.inv_cancel_invoice_requested) as inv_cancel_invoice_requested,
                    
                    sum(data.total_inv_cancelled) as total_inv_cancelled,
                    sum(data.ledger_cancel_inv_cancelled) as ledger_cancel_inv_cancelled,
                    sum(data.inv_cancel_inv_cancelled) as inv_cancel_inv_cancelled,
                    Round( avg(data.total_success_perc)::numeric, 2) as total_success_percent
                from 
                (
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    arl.api_url,
                    
                    COUNT(arl.id) as total_request,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('cancel', 'Ledger Cancellation API') ) AS ledger_cancel_total_request,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('cancel', 'Ledger Cancellation API') ) AS inv_cancel_total_request,
                     
                    COUNT(arl.id) FILTER ( WHERE arl.response_data->>'code' in ('200')) as total_successful,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('cancel', 'Ledger Cancellation API') and arl.response_data->>'code' in ('200') ) AS ledger_cancel_successful,
                    COUNT(arl.id) FILTER ( WHERE arl.api_url in ('cancel', 'Ledger Cancellation API') and arl.response_data->>'code' in ('200') ) AS inv_cancel_successful,
                    
                    Round( ( case when count(arl.id)  != 0
                                 then ( count( case when arl.response_data->>'code'::TEXT in ('200') then 1 end )::FLOAT 
                                        / 
                                        count(arl.id) 
                                       ) * 100
                                else 0
                                END)::numeric, 2
                    ) AS total_success_perc,
                    
                    (   select
                            coalesce(count(i.id), 0)
                        from
                            invoice_ledger_association ila
                            inner join ledger l on l.id = ila.ledger_id
                            inner join merchant_details md_inner on md_inner.id = l.merchant_id
                            inner join invoice i ON i.id = ila.invoice_id
                        where
                            md_inner.id::text = arl.merchant_id
                            and l.ledger_id = arl.request_data->>'ledgerNo'
                    ) as total_invoice_requested,
                    
                    (   select
                            count(i.id)
                        from
                            invoice_ledger_association ila
                            inner join ledger l on l.id = ila.ledger_id
                            inner join merchant_details md_inner on md_inner.id = l.merchant_id
                            inner join invoice i ON i.id = ila.invoice_id
                        where
                            md_inner.id::text = arl.merchant_id
                            and l.ledger_id = arl.request_data->>'ledgerNo'
                    ) as ledger_cancel_invoice_requested,
                    
                    (   select
                            count(i.id)
                        from
                            invoice_ledger_association ila
                            inner join ledger l on l.id = ila.ledger_id
                            inner join merchant_details md_inner on md_inner.id = l.merchant_id
                            inner join invoice i ON i.id = ila.invoice_id
                        where
                            md_inner.id::text = arl.merchant_id
                            and l.ledger_id = arl.request_data->>'ledgerNo'
                    ) as inv_cancel_invoice_requested,
                    
                    (   select
                            count(i.id)
                        from
                            invoice_ledger_association ila
                        inner join ledger l on l.id = ila.ledger_id
                        inner join merchant_details md_inner on md_inner.id = l.merchant_id
                        inner join invoice i ON i.id = ila.invoice_id
                        where
                            md_inner.id::text = arl.merchant_id
                            and l.ledger_id = arl.request_data->>'ledgerNo'
                            and arl.response_data->>'code' in ('200')
                    ) as total_inv_cancelled,
                    
                    (   select
                            count(i.id)
                        from
                            invoice_ledger_association ila
                            inner join ledger l on l.id = ila.ledger_id
                            inner join merchant_details md_inner on md_inner.id = l.merchant_id
                            inner join invoice i ON i.id = ila.invoice_id
                        where
                            md_inner.id::text = arl.merchant_id
                            and l.ledger_id = arl.request_data->>'ledgerNo'
                            and arl.api_url in ('cancel', 'Ledger Cancellation API')
                            and arl.response_data->>'code' in ('200')
                    ) as ledger_cancel_inv_cancelled,
                    
                    (   select
                            count(i.id)
                        from
                            invoice_ledger_association ila
                            inner join ledger l on l.id = ila.ledger_id
                            inner join merchant_details md_inner on md_inner.id = l.merchant_id
                            inner join invoice i ON i.id = ila.invoice_id
                        where
                            md_inner.id::text = arl.merchant_id
                            and l.ledger_id = arl.request_data->>'ledgerNo'
                            and arl.api_url in ('cancel', 'Invoice Cancellation API')
                            and arl.response_data->>'code' in ('200')
                    ) as inv_cancel_inv_cancelled
                from
                    api_request_log arl
                inner join merchant_details md on md.id::text = arl.merchant_id
                where
                    arl.api_url in ('cancel', 'Ledger Cancellation API', 'Invoice Cancellation API') 
                group by 
                    md.name, md.unique_id, period, arl.api_url, arl.merchant_id, arl.request_data, arl.response_data
                ) AS data
                group by 
                    data.category, data.idp_name, data.idp_code, data.period, data.api_url
                order by 
                    data.period desc
                ;
                """
            # 'Cancellation API', 'Ledger Cancellation API', 'Invoice Cancellation API'
            self.create_materialized_view()
        except Exception as e:
            logger.error(f"Exception cancellation_api_materialized_view :: {e}")
            pass

    def disbursement_api_materialized_view(self):
        logger.info(f"....disbursement_api_materialized_view....")
        # -- DROP VIEW IF EXISTS disbursement_materialized_api_view ;
        # -- CREATE OR REPLACE VIEW disbursement_materialized_api_view ;
        # -- drop materialized view if exists disbursement_api_materialized_view ;
        try:
            self.view_query = """ 
                drop materialized view if exists disbursement_api_materialized_view ;
                
                CREATE Materialized VIEW 
                    disbursement_api_materialized_view 
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    data.api_url,
                    sum(data."# of Request") as "# of Request",
                    sum(data."# of Updated") as "# of Updated",
                    sum(data."# finance but not disb") as "# Financed but not Disb",
                    sum(data."% Unfunded") as "% Unfunded",
                    sum(data."% Success") as "% Success"
                from 
                (
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char( arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    arl.api_url,
                    COUNT(arl.id) as "# of Request", 
                    count(arl.id) Filter ( where arl.response_data->>'code'::text in ('200', '1027') ) AS "# of Updated", 
                    (   select
                            count(i.id)
                        from
                            invoice_ledger_association ila
                        inner join ledger l on l.id = ila.ledger_id
                        inner join merchant_details md on l.merchant_id = l.merchant_id
                        inner join invoice i ON i.id = ila.invoice_id
                        where
                            md.id::text = arl.merchant_id
                            and l.ledger_id = arl.request_data->>'ledgerNo'
                            and i.status in ('funded', 'partial_funded')
                            and i.invoice_no in (
                                                    select 
                                                        (item->>'invoiceNo')::text
                                                    from
                                                        jsonb_array_elements( arl.request_data->'ledgerData') AS item
                                                )
                    ) AS "# finance but not disb",
                    ( count(arl.id) Filter (where arl.request_data->>'code'::text in ('1030')) / count(arl.id) ) * 100 AS "% Unfunded",
                    ( count(arl.id) Filter (where arl.request_data->>'code'::text in ('200', '1013')) / count(arl.id) ) * 100 AS "% Success"
                from
                    api_request_log arl
                inner join merchant_details md on md.id::text = arl.merchant_id
                where
                    arl.api_url in ('syncDisbursement') 
                group by 
                    md.name, md.unique_id, period, arl.api_url, arl.merchant_id, arl.request_data
                
                union all
                
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char( ppr.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    ppr.type as api_url,
                    count(ppr.id) as "# of Request", 
                    count(ppr.id) Filter ( where ppr.webhook_response->>'code'::text in ('200', '1027') ) AS "# of Updated", 
                    (   select
                            count(i.id)
                        from
                            invoice_ledger_association ila
                        inner join ledger l on l.id = ila.ledger_id
                        inner join merchant_details md on l.merchant_id = l.merchant_id
                        inner join invoice i ON i.id = ila.invoice_id
                        where
                            md.id::text = ppr.merchant_id
                            and l.ledger_id = ppr.webhook_response->>'ledgerNo'
                            and i.status in ('funded', 'partial_funded')
                            and i.invoice_no in (
                                                select 
                                                    (item->>'invoiceNo')::text
                                                from
                                                    jsonb_array_elements( ppr.webhook_response->'ledgerData') AS item
                                                )
                    ) AS "# finance but not disb",
                    ( count(ppr.id) Filter (where ppr.webhook_response->>'code'::text in ('1030')) / count(ppr.id) ) * 100 AS "% Unfunded",
                    ( count(ppr.id) Filter (where ppr.webhook_response->>'code'::text in ('200', '1013')) / count(ppr.id) ) * 100 AS "% Success"
                from
                    post_processing_request ppr
                inner join merchant_details md on md.id::text = ppr.merchant_id
                where
                    ppr.type in ('asyncDisbursement') 
                group by 
                    md.name, md.unique_id, period, ppr.type, ppr.merchant_id, ppr.webhook_response
                ) AS data
                group by
                    data.category, data.idp_name, data.idp_code, data.period, data.api_url
                order by
                    data.period       
                ;
            """
            self.create_materialized_view()
        except Exception as e:
            logger.error(f"Exception disbursement_api_view :: {e}")
            pass

    def repayment_api_materialized_view(self):
        logger.info(f"....repayment_api_materialized_view....")
        # -- DROP VIEW IF EXISTS repayment_materialized_api_view ;
        # -- CREATE OR REPLACE VIEW repayment_materialized_api_view ;
        # -- drop materialized view if exists repayment_api_materialized_view ;
        try:
            self.view_query = """ 
                drop materialized view if exists repayment_api_materialized_view ;
                CREATE Materialized VIEW 
                    repayment_api_materialized_view 
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    data.api_url,
                    data."# of Request",
                    data."# of Updated",
                    data."# Repaid but not Disb",
                    data."% Success"
                from 
                (
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    arl.api_url,
                    COUNT(arl.id) as "# of Request",
                    count(arl.id) Filter ( where arl.response_data->>'code'::text in ('200', '1031') ) AS "# of Updated",
                    count(arl.id) Filter ( where arl.response_data->>'code'::text in ('1032') ) AS "# Repaid but not Disb",
                    ( count(arl.id) Filter (where arl.request_data->>'code'::text in ('200', '1031')) / count(arl.id) ) * 100 AS "% Success" 
                from
                    api_request_log arl
                inner join merchant_details md on md.id::text = arl.merchant_id
                where
                    arl.api_url in ('syncRepayment', 'asyncRepayment') 
                group by 
                    md.name, md.unique_id, period, arl.api_url
                
                union all
                
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(ppr.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    ppr.type as api_url,
                    COUNT(ppr.id) as "# of Request",
                    count(ppr.id) Filter ( where ppr.webhook_response->>'code'::text in ('200', '1031') ) AS "# of Updated", 
                    count(ppr.id) Filter ( where ppr.webhook_response->>'code'::text in ('1032') ) AS "# Repaid but not Disb",
                    ( count(ppr.id) Filter (where ppr.webhook_response->>'code'::text in ('200', '1031')) / count(ppr.id) ) * 100 AS "% Success" 
                from
                    post_processing_request ppr
                inner join merchant_details md on md.id::text = ppr.merchant_id
                where
                    ppr.type in ('asyncRepayment') 
                group by 
                    md.name, md.unique_id, period, ppr.type
                ) AS data  
                order by 
                    data.period desc   
                ;
            """
            self.create_materialized_view()

            self.view_query = """ 
                drop materialized view if exists invoice_repayment_percent_materialized_view ;
                CREATE Materialized VIEW 
                    invoice_repayment_percent_materialized_view
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    --data.api_url,
                    data."Paid before Time",   
                    data."On Time", 
                    data."Less than 7 days Delay",  
                    data."7-30 days delay",
                    data."> 30 days delay"
                from 
                (
                select 
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(rh.updated_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    COUNT(i.id) FILTER (WHERE to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') > to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY')) AS "Paid before Time",
                    COUNT(i.id) FILTER (WHERE to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') = to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY')) AS "On Time",
                    COUNT(i.id) FILTER (WHERE to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY') > to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') AND to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY') <= (to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') + INTERVAL '7 DAY')) AS "Less than 7 days Delay",
                    COUNT(i.id) FILTER (WHERE to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY') > to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') AND to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY') >= (to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') + INTERVAL '7 DAY') AND to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY') < (to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') + INTERVAL '30 DAY')) AS "7-30 days delay",
                    COUNT(i.id) FILTER (WHERE to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY') > to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') AND to_date(rh.extra_data->>'repaymentDate', 'DD/MM/YYYY') > (to_date(rh.extra_data->>'dueDate', 'DD/MM/YYYY') + INTERVAL '30 DAY')) AS "> 30 days delay"
                        --COUNT(i.id) Filter ( WHERE (rh.extra_data ->'dueDate') < (rh.extra_data ->'repaymentDate') ) as "Paid before Time",
                        --COUNT(i.id) Filter ( WHERE (rh.extra_data ->'dueDate') = (rh.extra_data ->'repaymentDate') ) as "On Time",
                        --COUNT(i.id) Filter ( WHERE (rh.extra_data ->'repaymentDate') > (rh.extra_data ->'dueDate') and ((rh.extra_data ->'repaymentDate')::text::date) <= (((rh.extra_data ->' dueDate')::text::date + INTERVAL '7 DAY')::date )) as "Less than 7 days Delay",
                        --COUNT(i.id) Filter ( WHERE (rh.extra_data ->'repaymentDate') > (rh.extra_data ->'dueDate') and ((rh.extra_data ->'repaymentDate')::text::date) >= (( (rh.extra_data ->'dueDate')::text::date + INTERVAL '7 DAY')::date ) and ((rh.extra_data ->'repaymentDate')::text::date) < (( (rh.extra_data ->'dueDate')::text::date + INTERVAL '30 DAY')::date )) as "7-30 days delay",
                        --COUNT(i.id) Filter ( WHERE (rh.extra_data ->'repaymentDate') > (rh.extra_data ->'dueDate') and ((rh.extra_data ->'repaymentDate')::text::date) > (( (rh.extra_data ->'dueDate')::text::date + INTERVAL '30 DAY')::date )) as "> 30 days delay"
                from
                    invoice i 
                inner join invoice_ledger_association ila on i.id = ila.invoice_id 
                inner join ledger l on l.id = ila.ledger_id
                inner join merchant_details md on md.id = l.merchant_id
                inner join repayment_history rh on rh.invoice_id = i.id
                where 
                    i.status in ('partial_repaid', 'repaid')
                    and rh.extra_data ->'dueDate' is not null and rh.extra_data ->'repaymentDate' is not null
                group by 
                    md.name, md.unique_id, period
                ) AS data       
                ;
            """
            self.create_materialized_view()
        except Exception as e:
            logger.error(f"Exception repayment_api_materialized_view :: {e}")
            pass

    def status_check_api_materialized_view(self):
        logger.info(f"....status_check_api_materialized_view....")
        # -- DROP VIEW IF EXISTS status_check_api_view ;
        # -- CREATE OR REPLACE VIEW status_check_api_view ;
        # -- DROP materialized VIEW IF EXISTS status_check_api_materialized_view ;
        try:
            self.view_query = """ 
                DROP materialized VIEW IF EXISTS status_check_api_materialized_view ;
                
                CREATE Materialized VIEW
                    status_check_api_materialized_view  
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    data.api_url,
                    data."# of Request",
                    data."% Success"
                from 
                (
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char( arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    arl.api_url,
                    COUNT(arl.id) as "# of Request",
                    count(arl.id) Filter ( where arl.response_data->>'code'::text in ('200') ) AS "% Success"
                from
                    api_request_log arl
                inner join merchant_details md on md.id::text = arl.merchant_id
                where
                    arl.api_url in ( 'sync-invoice-status-check-with-code',  
                                     'sync-invoice-status-check-without-code', 
                                     'sync-ledger-status-check'
                                   ) 
                group by 
                    md.name, md.unique_id, period, arl.api_url
                
                union all
                
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char( ppr.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    ppr.type as api_url,
                    COUNT(ppr.id) as "# of Request",
                    count(ppr.id) Filter ( where ppr.webhook_response->>'code'::text in ('200') ) AS "% Success"
                from
                    post_processing_request ppr
                inner join merchant_details md on md.id::text = ppr.merchant_id
                where
                    ppr.type in ( 'invoice_status_check_with_code', 
                                  'invoice_status_check_without_code', 
                                  'ledger_status_check'
                                 ) 
                group by 
                    md.name, md.unique_id, period, ppr.type
                ) AS data
                order by data.period   
                ;
            """
            self.create_materialized_view()
        except Exception as e:
            logger.error(f"Exception status_check_api_view :: {e}")
            pass

    def total_calls_for_all_api_materialized_view(self):
        logger.info(f"....total_calls_for_all_api_materialized_view....")
        # -- DROP VIEW IF EXISTS registration_materialized_api_view ;
        # -- CREATE OR REPLACE VIEW registration_materialized_api_view ;
        # -- drop materialized view if exists registration_api_materialized_view ;
        try:
            self.view_query = """ 
                drop materialized view if exists total_calls_for_all_api_materialized_view ;

                CREATE Materialized VIEW 
                    total_calls_for_all_api_materialized_view 
                as
                select 
                    data.category,
                    data.idp_name,
                    data.idp_code,
                    data.period,
                    data.api_url,
                    data.total_incoming_ping,
                    data.total_successful,
                    data.total_success_perc
                from 
                (
                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    arl.api_url,
                    COUNT(arl.id) as total_incoming_ping,
                    COUNT(arl.id) Filter ( WHERE arl.response_data->>'code'::text in ('200', '1013', '1027', '1031') ) as total_successful,
                    Round( ( case when count(arl.id)  != 0
                                  then ( count( case when arl.response_data->>'code'::TEXT in ('200', '1013', '1027', '1031') then 1 end )::FLOAT 
                                       / 
                                       count(arl.id) 
                                       ) * 100 
                                  else 0 
                                  END)::numeric, 2
                    ) AS total_success_perc
                from
                    api_request_log arl
                inner join merchant_details md on md.id::text = arl.merchant_id
                where
                    arl.api_url in ( 'sync-entity-registration', 
                                     'sync-registration-without-code', 
                                     'sync-invoice-registration-with-code',
                                     'syncFinancing',
                                     'cancel',
                                     'syncDisbursement',
                                     'syncRepayment',
                                     'sync-invoice-status-check-with-code',  
                                     'sync-invoice-status-check-without-code', 
                                     'sync-ledger-status-check'
                                     
                                    ) 
                group by 
                    md.name, md.unique_id, period, arl.api_url

                union all 

                select  
                    '-'::text as category,
                    md.name as idp_name,
                    md.unique_id as idp_code,
                    to_date(to_char(ppr.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as period,
                    ppr.type as api_url,
                    COUNT(ppr.id) as total_incoming_ping,
                    COUNT(ppr.id) Filter ( WHERE ppr.webhook_response->>'code'::text in ('200', '1013', '1027', '1031') ) as total_successful,
                    Round( ( case when count(ppr.id)  != 0
                                 then ( count( case when ppr.webhook_response->>'code'::TEXT in ('200', '1013', '1027', '1031') then 1 end )::FLOAT 
                                        / 
                                        count(ppr.id) 
                                       ) * 100
                                else 0
                                END)::numeric, 2
                    ) AS total_success_perc
                from
                    post_processing_request ppr
                inner join merchant_details md on md.id::text = ppr.merchant_id
                where
                    ppr.type in ( 'async_entity_registration',
                                  'async_registration_without_code',
                                  'async_invoice_registration_with_code', 
                                  'async_bulk_registration_with_code',  
                                  'async_bulk_registration_without_codes',
                                  'asyncFinancing',
                                  'asyncDisbursement',
                                  'asyncRepayment',
                                  'invoice_status_check_with_code', 
                                  'invoice_status_check_without_code', 
                                  'ledger_status_check'
                                ) 
                group by 
                    md.name, md.unique_id, period, ppr.type
                ) AS data 
                order by data.period desc
            """

            self.create_materialized_view()
        except Exception as e:
            logger.error(f"Exception total_calls_for_all_api_materialized_view :: {e}")
            pass


@router.post("/create-materialized-view")
def create_materialized_view():
    try:
        logger.info(f"/create-materialized-view")
        resp = MisReport().create_all_materialized_view()
        resp = MisReport().refresh_materialized_view()
        logger.info(f"create_materialized_view :: response {resp}")
        return {
            **ErrorCodes.get_error_response(200)
        }
    except Exception as e:
        logger.exception(f"Exception create_materialized_view :: {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


def mis_report_query(request_data):
    db = next(get_db())
    financing_materialized_api_view = """
        select 
            category, idp_name, idp_code, created_date, period, api_url, "# Of Request", "# Successful", 
            "Funding Ok %", "Repeat %", "Duplicate%", "Amount of Request", "# Invoices Request", "% of Invoices Ok", 
            "Amount Ok for funding", "% Funding Value" 
        from 
            financing_api_materialized_view
    """
    cancellation_materialized_api_view = """ 
        select 
            category, idp_name, idp_code, created_date, period, api_url, "# Request", "# Successful", 
            "% of Invoices Requested", "# Inv. Cancelled" 
        from 
            cancellation_api_materialized_view
    """
    registration_materialized_api_view = """
        select
            category, idp_name, idp_code, created_date, period, api_url, "Incoming Ping", "Pass", "Fail", "% Pass", 
            "% Duplicate", "% Repeat", "# Invoice Req", "# Inv Pass", "Avg. Inv/Ping" 
        from 
            registration_api_materialized_view
    """
    disbursement_materialized_api_view = """
        select
            category, idp_name, idp_code, created_date, period, api_url, "# of Request", "# of Updated", 
            "# Financed but not Disb", "% Unfunded" 
        from 
            disbursement_api_materialized_view
    """
    repayment_materialized_api_view = """
        select 
            category, idp_name, idp_code, created_date, period, api_url, "# of Request", 
            "# of Updated", "# Repaid but not Disb" 
        from 
            repayment_api_materialized_view
    """
    status_check_materialized_api_view = """
        select 
            category, idp_name, idp_code, created_date, period, api_url, "# of Request", "% Success" 
        from 
            status_check_api_materialized_view
    """

    query_mapper = {
        "finance": financing_materialized_api_view,
        "cancel": cancellation_materialized_api_view,
        "registration": registration_materialized_api_view,
        "disbursement": disbursement_materialized_api_view,
        "repayment": repayment_materialized_api_view,
        "statusCheck": status_check_materialized_api_view
    }
    report_type = request_data.get('reportType', '')
    where_clause = ""
    if request_data.get("fromDate") and request_data.get("toDate"):
        where_clause = """
            where created_date between (:fromDate)::date and (:toDate)::date
        """
        # query += """ order by timefilter desc limit """ + str(self.tmp_dict.get("pages")) + """ offset """ + str(
        #     self.tmp_dict.get("offset"))

    if report_type:
        return query_mapper.get(report_type) + where_clause
    else:
        return 'invalid reportType'


class GetInvoiceHubMisReport:

    @staticmethod
    def cumulative_finance_data(query_data):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()

        ftd_df = df
        ftd_df = ftd_df[ftd_df.period == curr_date]
        ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)

        finance_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            '# Of Request': 'sum',
            '# Successful': 'sum',
            'Funding Ok %': 'mean',
            'Repeat %': 'mean',
            'Duplicate%': 'mean',
            'Amount of Request': 'sum',
            '# Invoices Request': 'sum',
            '% of Invoices Ok': 'mean',
            'Amount Ok for funding': 'sum',
            '% Funding Value': 'mean'
        }).reset_index()
        # Round numeric columns to two decimal places
        numeric_cols = ['Funding Ok %', 'Repeat %', 'Duplicate%', 'Amount of Request', '% of Invoices Ok', 'Amount Ok for funding', '% Funding Value']
        finance_df[numeric_cols] = finance_df[numeric_cols].astype(float).round(2)
        finance_df['period'] = 'FTD'

        mtd_df = df
        curr_month = now_time.date().month
        mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
        mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        mtd_finance_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            '# Of Request': 'sum',
            '# Successful': 'sum',
            'Funding Ok %': 'mean',
            'Repeat %': 'mean',
            'Duplicate%': 'mean',
            'Amount of Request': 'sum',
            '# Invoices Request': 'sum',
            '% of Invoices Ok': 'mean',
            'Amount Ok for funding': 'sum',
            '% Funding Value': 'mean'
        }).reset_index()
        mtd_finance_df[numeric_cols] = mtd_finance_df[numeric_cols].astype(float).round(2)
        mtd_finance_df['period'] = 'MTD'

        ytd_df = df
        curr_year = now_time.date().year
        ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year] # '2024'
        ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        ytd_finance_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            '# Of Request': 'sum',
            '# Successful': 'sum',
            'Funding Ok %': 'mean',
            'Repeat %': 'mean',
            'Duplicate%': 'mean',
            'Amount of Request': 'sum',
            '# Invoices Request': 'sum',
            "% of Invoices Ok": 'mean',
            "Amount Ok for funding": 'sum',
            "% Funding Value": 'mean'
        }).reset_index()
        ytd_finance_df[numeric_cols] = ytd_finance_df[numeric_cols].astype(float).round(2)
        ytd_finance_df['period'] = 'YTD'

        lmst_df = df
        lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
        lmst_month = lmst_date.month
        lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
        lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        lmst_finance_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            '# Of Request': 'sum',
            '# Successful': 'sum',
            'Funding Ok %': 'mean',
            'Repeat %': 'mean',
            'Duplicate%': 'mean',
            'Amount of Request': 'sum',
            '# Invoices Request': 'sum',
            '% of Invoices Ok': 'mean',
            'Amount Ok for funding': 'sum',
            '% Funding Value': 'mean'
        }).reset_index()
        lmst_finance_df[numeric_cols] = lmst_finance_df[numeric_cols].astype(float).round(2)
        lmst_finance_df['period'] = 'LMST'

        cumulative_list = pd.concat([finance_df, mtd_finance_df, ytd_finance_df, lmst_finance_df])
        cumulative_list.reset_index(drop=True)
        # cumulative_list = cumulative_list.sort_values(by='idp_code')
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')# Remove 'period' from its current position
        cols.insert(3, 'period') # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))

        return cumulative_list

    @staticmethod
    def get_finance_data(request_data, db):
        query1 = """
        select 
            category, 
            idp_name, 
            idp_code, 
            period, 
            api_url, 
            total_of_request as "# Of Request", 
            total_successful as "# Successful", 
            coalesce("Funding Ok %", '0') as "Funding Ok %", 
            total_repeat_perc as "Repeat %", 
            total_duplicate_perc as "Duplicate%", 
            total_amount_of_request as "Amount of Request", 
            total_invoices_request as "# Invoices Request",
            "% of Invoices Ok", 
            "Amount Ok for funding", 
            "% Funding Value" 
        from 
            financing_api_materialized_view
        where 
                api_url in ('syncFinancing', 'asyncFinancing')  
            and idp_code = ANY(:idp_id)
        """
        ##--and period between (:fromDate)::date and (:toDate)::date
        query2 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                f_with_ec_of_request as "# Of Request", 
                f_with_ec_successful as "# Successful", 
                coalesce("Funding Ok %", '0') as "Funding Ok %",
                f_with_ec_repeat_perc as "Repeat %", 
                f_with_ec_duplicate_perc as "Duplicate%", 
                f_with_ec_amount_of_request as "Amount of Request", 
                f_with_ec_invoices_request as "# Invoices Request",
                "% of Invoices Ok", 
                "Amount Ok for funding", 
                "% Funding Value" 
            from 
                financing_api_materialized_view
            where 
                api_url in ('Financing API with Entity Code', 'syncFinancing') and 
                period between (:fromDate)::date and (:toDate)::date
                and idp_code = ANY(:idp_id)
        """
        query3 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                f_without_ec_of_request as "# Of Request", 
                f_without_ec_successful as "# Successful", 
                coalesce("Funding Ok %", '0') as "Funding Ok %",
                f_without_ec_repeat_perc as "Repeat %", 
                f_without_ec_duplicate_perc as "Duplicate%", 
                f_without_ec_amount_of_request as "Amount of Request", 
                coalesce(f_without_ec_invoices_request, 0) as "# Invoices Request", 
                "% of Invoices Ok", 
                "Amount Ok for funding", 
                "% Funding Value" 
            from 
                financing_api_materialized_view
            where 
                api_url in ('Financing API without Entity Code', 'syncFinancing') and 
                period between (:fromDate)::date and (:toDate)::date
                and idp_code = ANY(:idp_id)
        """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            query1_result = db.execute(text(query1), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            query1 = query1 + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))

        logger.info(f"query1_result {query1_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        data = {
            'financingAPI': data1
        }
        if request_data.get('filterType', '') == 'all':
            finance_data = GetInvoiceHubMisReport.cumulative_finance_data(data['financingAPI'])
            data = {
                'financingAPI': finance_data
            }
        return data

    @staticmethod
    def cumulative_registration_data(query_data, type):
        from datetime import datetime
        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()
        if type in ('registrationAPI', 'invoiceRegWithEC', 'invoiceRegWithoutEC'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['Incoming Ping', 'Pass', 'Fail', '% Pass', '# Invoice Req', '# Inv Pass', 'Avg. Inv/Ping', '% Duplicate', '% Repeat']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type =='entityRegistration':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "Entity Req": 'sum',
                "Entity Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['Incoming Ping', 'Pass', 'Fail', '% Pass', '% Duplicate', '% Repeat', 'Entity Req', 'Entity Pass', 'Avg. Inv/Ping']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "Entity Req": 'sum',
                "Entity Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "Entity Req": 'sum',
                "Entity Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                "% Duplicate": 'sum',
                "% Repeat": 'sum',
                "Entity Req": 'sum',
                "Entity Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        cumulative_list = pd.concat([ftd_reg_df, mtd_reg_df, ytd_reg_df, lmst_reg_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_registration_data(request_data, db):

        query1 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as "Incoming Ping", 
                total_pass as "Pass", 
                total_fail as "Fail", 
                total_pass_perc as "% Pass",
                0::text as "% Duplicate", 
                0::text as "% Repeat", 
                total_invoice_req as "# Invoice Req", 
                total_invoice_pass as "# Inv Pass", 
                total_avg_inv_ping as "Avg. Inv/Ping" 
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-registration-without-code', 'async_registration_without_code', 
                              'sync-invoice-registration-with-code', 'async_invoice_registration_with_code', 
                             -- 'sync-entity-registration', 'async_entity_registration', 
                              'async_bulk_registration_with_code',  'async_bulk_registration_without_codes'
                            )
                and idp_code = ANY(:idp_id)
        """

        query2 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                entity_reg_incoming_ping as "Incoming Ping", 
                entity_reg_pass as "Pass", 
                entity_reg_fail as "Fail", 
                entity_reg_pass_perc as "% Pass",
                '0'::text as "% Duplicate", 
                '0'::text as "% Repeat",
                entity_reg_invoice_req as "Entity Req", 
                entity_reg_invoice_pass as "Entity Pass", 
                entity_reg_avg_inv_ping as "Avg. Inv/Ping" 
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-entity-registration', 'async_entity_registration' ) 
                and idp_code = ANY(:idp_id)
        """

        query3 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                inv_reg_with_ec_incoming_ping as "Incoming Ping", 
                inv_reg_with_ec_pass as "Pass", 
                inv_reg_with_ec_fail as "Fail", 
                inv_reg_with_ec_pass_perc as "% Pass",
                '0'::text as  "% Duplicate", 
                '0'::text as  "% Repeat", 
                inv_reg_with_ec_invoice_req as "# Invoice Req", 
                inv_reg_with_ec_invoice_pass as "# Inv Pass", 
                inv_reg_with_ec_avg_inv_ping as "Avg. Inv/Ping" 
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-invoice-registration-with-code', 'async_invoice_registration_with_code', 
                             'async_bulk_registration_with_code' ) 
                and idp_code = ANY(:idp_id)
        """

        query4 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                inv_reg_without_ec_incoming_ping as "Incoming Ping", 
                inv_reg_without_ec_pass as "Pass", 
                inv_reg_without_ec_fail as "Fail", 
                inv_reg_without_ec_pass_perc as "% Pass",
                '0'::text as "% Duplicate", 
                '0'::text as "% Repeat", 
                inv_reg_without_ec_invoice_req as "# Invoice Req", 
                inv_reg_without_ec_invoice_pass as "# Inv Pass", 
                inv_reg_without_ec_avg_inv_ping as  "Avg. Inv/Ping" 
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-registration-without-code', 'async_registration_without_code', 
                             'async_bulk_registration_without_codes' ) 
                and idp_code = ANY(:idp_id)
        """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            query1_result = db.execute(text(query1), [{'idp_id': idp_id}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id}])
            query3_result = db.execute(text(query3), [{'idp_id': idp_id}])
            query4_result = db.execute(text(query4), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            query1 = query1 + "and period between (:fromDate)::date and (:toDate)::date"
            query2 = query2 + "and period between (:fromDate)::date and (:toDate)::date"
            query3 = query3 + "and period between (:fromDate)::date and (:toDate)::date"
            query4 = query4 + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query3_result = db.execute(text(query3), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query4_result = db.execute(text(query4), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))
            query2_result = db.execute(text(query2))
            query3_result = db.execute(text(query3))
            query4_result = db.execute(text(query4))

        logger.info(f"query1_result {query1_result}")
        logger.info(f"query2_result {query2_result}")
        logger.info(f"query3_result {query3_result}")
        logger.info(f"query3_result {query4_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        columns = query2_result.keys()
        data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]

        columns = query3_result.keys()
        data3 = [dict(zip(columns, row)) for row in query3_result.fetchall()]

        columns = query4_result.keys()
        data4 = [dict(zip(columns, row)) for row in query4_result.fetchall()]

        data = {
            'registrationAPI': data1,
            'entityRegistration': data2,
            'invoiceRegWithEC': data3,
            'invoiceRegWithoutEC': data4
        }
        if request_data.get('filterType', '') == 'all':
            registration_api = GetInvoiceHubMisReport.cumulative_registration_data(data['registrationAPI'], 'registrationAPI') if data['registrationAPI'] != [] else []
            entity_registration = GetInvoiceHubMisReport.cumulative_registration_data(data['entityRegistration'], 'entityRegistration') if data['entityRegistration'] != [] else []
            invoice_reg_with_ec = GetInvoiceHubMisReport.cumulative_registration_data(data['invoiceRegWithEC'], 'invoiceRegWithEC') if data['invoiceRegWithEC'] != [] else []
            invoice_reg_without_ec = GetInvoiceHubMisReport.cumulative_registration_data(data['invoiceRegWithoutEC'], 'invoiceRegWithoutEC') if data['invoiceRegWithoutEC'] != [] else []
            data = {
                'registrationAPI': registration_api,
                'entityRegistration': entity_registration,
                'invoiceRegWithEC': invoice_reg_with_ec,
                'InvoiceRegWithoutEC': invoice_reg_without_ec
            }
        return data

    @staticmethod
    def cumulative_cancel_data(query_data):
        # from datetime import datetime

        df = pd.DataFrame(query_data)
        now_time = dt.now()
        curr_date = now_time.date()

        ftd_df = df
        ftd_df = ftd_df[ftd_df.period == curr_date]
        # ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        cancel_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# Request": 'sum',
            "# Successful": 'sum',
            "# of Invoices Requested": 'sum',
            "# Inv. Cancelled": 'sum'
        }).reset_index()
        # Round numeric columns to two decimal places
        numeric_cols = ["# Request", "# Successful", "# of Invoices Requested", "# Inv. Cancelled"]
        cancel_df[numeric_cols] = cancel_df[numeric_cols].astype(float).round(2)
        cancel_df['period'] = 'FTD'

        mtd_df = df
        curr_month = now_time.date().month
        mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
        # mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        mtd_cancel_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# Request": 'sum',
            "# Successful": 'sum',
            "# of Invoices Requested": 'sum',
            "# Inv. Cancelled": 'sum'
        }).reset_index()
        mtd_cancel_df[numeric_cols] = mtd_cancel_df[numeric_cols].astype(float).round(2)
        mtd_cancel_df['period'] = 'MTD'

        ytd_df = df
        curr_year = now_time.date().year
        ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
        # ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        ytd_cancel_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# Request": 'sum',
            "# Successful": 'sum',
            "# of Invoices Requested": 'sum',
            "# Inv. Cancelled": 'sum'
        }).reset_index()
        ytd_cancel_df[numeric_cols] = ytd_cancel_df[numeric_cols].astype(float).round(2)
        ytd_cancel_df['period'] = 'YTD'

        lmst_df = df
        lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
        lmst_month = lmst_date.month
        lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
        # lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        lmst_cancel_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# Request": 'sum',
            "# Successful": 'sum',
            "# of Invoices Requested": 'sum',
            "# Inv. Cancelled": 'sum'
        }).reset_index()
        lmst_cancel_df[numeric_cols] = lmst_cancel_df[numeric_cols].astype(float).round(2)
        lmst_cancel_df['period'] = 'LMST'

        cumulative_list = pd.concat([cancel_df, mtd_cancel_df, ytd_cancel_df, lmst_cancel_df])
        cumulative_list.reset_index(drop=True)
        # cumulative_list = cumulative_list.sort_values(by='idp_code')
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))

        return cumulative_list

    @staticmethod
    def get_cancel_data(request_data, db):
        ## Cancellation API, Ledger Cancellation API, Invoice Cancellation API
        query1 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_request as "# Request", 
                total_successful as "# Successful",
                total_invoice_requested as "# of Invoices Requested", 
                total_inv_cancelled as "# Inv. Cancelled" 
            from 
                cancellation_api_materialized_view
            where 
                api_url in ('cancel', 'Cancellation API')
                and idp_code = ANY(:idp_id)
            """

        query2 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                ledger_cancel_total_request as "# Request", 
                ledger_cancel_successful as "# Successful", 
                ledger_cancel_invoice_requested as "# of Invoices Requested", 
                ledger_cancel_inv_cancelled as "# Inv. Cancelled" 
            from 
                cancellation_api_materialized_view
            where 
                api_url in ( 'cancel', 'Ledger Cancellation API') 
                and idp_code = ANY(:idp_id)
            """

        query3 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                inv_cancel_total_request as "# Request", 
                inv_cancel_successful as "# Successful", 
                inv_cancel_invoice_requested as "# of Invoices Requested", 
                inv_cancel_inv_cancelled as "# Inv. Cancelled" 
            from 
                cancellation_api_materialized_view
            where 
                api_url in ( 'cancel', 'Invoice Cancellation API')  
                and idp_code = ANY(:idp_id)
            """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            query1_result = db.execute(text(query1), [{'idp_id': idp_id}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id}])
            query3_result = db.execute(text(query3), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            query1 = query1 + "and period between (:fromDate)::date and (:toDate)::date"
            query2 = query2 + "and period between (:fromDate)::date and (:toDate)::date"
            query3 = query3 + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query3_result = db.execute(text(query3), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))
            query2_result = db.execute(text(query2))
            query3_result = db.execute(text(query3))

        logger.info(f"query1_result {query1_result}")
        logger.info(f"query2_result {query2_result}")
        logger.info(f"query3_result {query3_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        columns = query2_result.keys()
        data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]

        columns = query3_result.keys()
        data3 = [dict(zip(columns, row)) for row in query3_result.fetchall()]

        data = {
            'cancellationAPI': data1,
            'ledgerCancellationAPI': data2,
            'invoiceCancellationAPI': data3
        }
        if request_data.get('filterType', '') == 'all':
            cancellation_api = GetInvoiceHubMisReport.cumulative_cancel_data(data['cancellationAPI']) if data['cancellationAPI'] != [] else []
            ledger_cancellation_api = GetInvoiceHubMisReport.cumulative_cancel_data(data['ledgerCancellationAPI']) if data['ledgerCancellationAPI'] != [] else []
            invoice_cancellation_api = GetInvoiceHubMisReport.cumulative_cancel_data(data['invoiceCancellationAPI']) if data['invoiceCancellationAPI'] != [] else []
            data = {
                'cancellationAPI': cancellation_api,
                'ledgerCancellationAPI': ledger_cancellation_api,
                'invoiceCancellationAPI': invoice_cancellation_api
            }
        return data

    @staticmethod
    def cumulative_disbursement_data(query_data):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()

        ftd_df = df
        ftd_df = ftd_df[ftd_df.period == curr_date]
        # ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)

        dis_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "# of Updated": 'sum',
            "# Financed but not Disb": 'sum',
            "% Unfunded": 'mean'
        }).reset_index()
        # Round numeric columns to two decimal places
        numeric_cols = ["# of Request", "# of Updated", "# Financed but not Disb", "% Unfunded"]
        dis_df[numeric_cols] = dis_df[numeric_cols].astype(float).round(2)
        dis_df['period'] = 'FTD'

        mtd_df = df
        curr_month = now_time.date().month
        mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
        # mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        mtd_dis_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "# of Updated": 'sum',
            "# Financed but not Disb": 'sum',
            "% Unfunded": 'mean'
        }).reset_index()
        mtd_dis_df[numeric_cols] = mtd_dis_df[numeric_cols].astype(float).round(2)
        mtd_dis_df['period'] = 'MTD'

        ytd_df = df
        curr_year = now_time.date().year
        ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
        # ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        ytd_dis_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "# of Updated": 'sum',
            "# Financed but not Disb": 'sum',
            "% Unfunded": 'mean'
        }).reset_index()
        ytd_dis_df[numeric_cols] = ytd_dis_df[numeric_cols].astype(float).round(2)
        ytd_dis_df['period'] = 'YTD'

        lmst_df = df
        lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
        lmst_month = lmst_date.month
        lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
        # lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        lmst_dis_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "# of Updated": 'sum',
            "# Financed but not Disb": 'sum',
            "% Unfunded": 'mean'
        }).reset_index()
        lmst_dis_df[numeric_cols] = lmst_dis_df[numeric_cols].astype(float).round(2)
        lmst_dis_df['period'] = 'LMST'

        cumulative_list = pd.concat([dis_df, mtd_dis_df, ytd_dis_df, lmst_dis_df])
        cumulative_list.reset_index(drop=True)
        # cumulative_list = cumulative_list.sort_values(by='idp_code')
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_disbursement_data(request_data, db):
        ## Disbursal API
        query1 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated", 
                "# Financed but not Disb", 
                "% Unfunded" 
            from 
                disbursement_api_materialized_view
            where 
                api_url in ('syncDisbursement', 'asyncDisbursement') AND 
                period between (:fromDate)::date and (:toDate)::date
                and idp_code = ANY(:idp_id)
        """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if from_date != "" and to_date != "":
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))

        logger.info(f"query1_result {query1_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        data = {'disbursalAPI': data1}
        if request_data.get('filterType', '') == 'all':
            disbursal_api = GetInvoiceHubMisReport.cumulative_disbursement_data(data['disbursalAPI']) if data['disbursalAPI'] != [] else []
            data = {
                'disbursalAPI': disbursal_api,
            }
        return data

    @staticmethod
    def cumulative_repayment_data(query_data, type):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()
        if type in ('repaymentAPI'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)

            repay_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "# Repaid but not Disb": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ["# of Request", "# of Updated", "# Repaid but not Disb"]
            repay_df[numeric_cols] = repay_df[numeric_cols].astype(float).round(2)
            repay_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            mtd_repay_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "# Repaid but not Disb": 'sum'
            }).reset_index()
            mtd_repay_df[numeric_cols] = mtd_repay_df[numeric_cols].astype(float).round(2)
            mtd_repay_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            ytd_repay_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "# Repaid but not Disb": 'sum'
            }).reset_index()
            ytd_repay_df[numeric_cols] = ytd_repay_df[numeric_cols].astype(float).round(2)
            ytd_repay_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_repay_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "# Repaid but not Disb": 'sum'
            }).reset_index()
            lmst_repay_df[numeric_cols] = lmst_repay_df[numeric_cols].astype(float).round(2)
            lmst_repay_df['period'] = 'LMST'

        elif type in ('invoiceRepayment%'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)

            repay_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Paid before Time": 'sum',
                "On Time": 'sum',
                "Less than 7 days Delay": 'sum',
                "7-30 days delay": 'sum',
                "> 30 days delay": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ["Paid before Time", "On Time", "Less than 7 days Delay", "7-30 days delay", "> 30 days delay"]
            repay_df[numeric_cols] = repay_df[numeric_cols].astype(float).round(2)
            repay_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            mtd_repay_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Paid before Time": 'sum',
                "On Time": 'sum',
                "Less than 7 days Delay": 'sum',
                "7-30 days delay": 'sum',
                "> 30 days delay": 'sum'
            }).reset_index()
            mtd_repay_df[numeric_cols] = mtd_repay_df[numeric_cols].astype(float).round(2)
            mtd_repay_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            ytd_repay_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Paid before Time": 'sum',
                "On Time": 'sum',
                "Less than 7 days Delay": 'sum',
                "7-30 days delay": 'sum',
                "> 30 days delay": 'sum'
            }).reset_index()
            ytd_repay_df[numeric_cols] = ytd_repay_df[numeric_cols].astype(float).round(2)
            ytd_repay_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_repay_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Paid before Time": 'sum',
                "On Time": 'sum',
                "Less than 7 days Delay": 'sum',
                "7-30 days delay": 'sum',
                "> 30 days delay": 'sum'
            }).reset_index()
            lmst_repay_df[numeric_cols] = lmst_repay_df[numeric_cols].astype(float).round(2)
            lmst_repay_df['period'] = 'LMST'

        cumulative_list = pd.concat([repay_df, mtd_repay_df, ytd_repay_df, lmst_repay_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_repayment_data(request_data, db):
        ## Repayment API, Invoice Repayment %
        query1 = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated", 
                "# Repaid but not Disb" 
            from 
                repayment_api_materialized_view
            where 
                api_url in ('syncRepayment', 'asyncRepayment') 
                and idp_code = ANY(:idp_id)
            """
        # query2 = """
        # select
        #         category, idp_name, idp_code, period, api_url,
        #         ''::text as "Paid before Time",
        #         ''::text as "On Time",
        #         ''::text as "Less than 7 days Delay",
        #         ''::text as "7-30 days delay",
        #         ''::text as "> 30 days delay"
        #     from
        #         invoice_repayment_percent_materialized_view
        #     where
        #         api_url in ('syncRepayment', 'asyncRepayment')
        #         and idp_code = ANY(:idp_id)
        # """
        query2 = """
            select
                category,
                idp_name, 
                idp_code,
                period, 
                "Paid before Time",
                "On Time", 
                "Less than 7 days Delay", 
                "7-30 days delay", 
                "> 30 days delay"
            from 
                invoice_repayment_percent_materialized_view
            where  
                idp_code = ANY(:idp_id)
        """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            query1_result = db.execute(text(query1), [{'idp_id': idp_id}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            query1 = query1 + "and period between (:fromDate)::date and (:toDate)::date"
            query2 = query2 + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))
            query2_result = db.execute(text(query2))

        logger.info(f"query1_result {query1_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        columns = query2_result.keys()
        data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]

        data = {
            'repaymentAPI': data1,
            'invoiceRepayment%': data2
        }
        if request_data.get('filterType', '') == 'all':
            repayment_api = GetInvoiceHubMisReport.cumulative_repayment_data(data['repaymentAPI'], 'repaymentAPI') if data['repaymentAPI'] != [] else []
            invoice_repayment_per = GetInvoiceHubMisReport.cumulative_repayment_data(data['invoiceRepayment%'], 'invoiceRepayment%') if data['invoiceRepayment%'] != [] else []
            data = {
                'repaymentAPI': repayment_api,
                'invoiceRepayment%': invoice_repayment_per
            }
        return data

    @staticmethod
    def cumulative_status_check_data(query_data):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()

        ftd_df = df
        ftd_df = ftd_df[ftd_df.period == curr_date]
        # ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)

        status_check_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "% Success": 'mean'
        }).reset_index()
        # Round numeric columns to two decimal places
        numeric_cols = ["# of Request", "% Success"]
        status_check_df[numeric_cols] = status_check_df[numeric_cols].astype(float).round(2)
        status_check_df['period'] = 'FTD'

        mtd_df = df
        curr_month = now_time.date().month
        mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
        # mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        mtd_status_check_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "% Success": 'mean'
        }).reset_index()
        mtd_status_check_df[numeric_cols] = mtd_status_check_df[numeric_cols].astype(float).round(2)
        mtd_status_check_df['period'] = 'MTD'

        ytd_df = df
        curr_year = now_time.date().year
        ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
        # ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        ytd_status_check_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "% Success": 'mean'
        }).reset_index()
        ytd_status_check_df[numeric_cols] = ytd_status_check_df[numeric_cols].astype(float).round(2)
        ytd_status_check_df['period'] = 'YTD'

        lmst_df = df
        lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
        lmst_month = lmst_date.month
        lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
        # lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
        lmst_status_check_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
            "# of Request": 'sum',
            "% Success": 'mean'
        }).reset_index()
        lmst_status_check_df[numeric_cols] = lmst_status_check_df[numeric_cols].astype(float).round(2)
        lmst_status_check_df['period'] = 'LMST'

        cumulative_list = pd.concat([status_check_df, mtd_status_check_df, ytd_status_check_df, lmst_status_check_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_status_check_data(request_data, db):
        ## Status Check, Status Check with Entity, Status Ch without Entity
        query1 = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "% Success" 
            from 
                status_check_api_materialized_view
            where 
                api_url in ('sync-ledger-status-check', 'ledger_status_check')
                and idp_code = ANY(:idp_id)
        """

        query2 = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "% Success" 
            from 
                status_check_api_materialized_view
            where 
                api_url in ('sync-invoice-status-check-with-code', 'invoice_status_check_with_code') 
                and idp_code = ANY(:idp_id)
        """

        query3 = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "% Success" 
            from 
                status_check_api_materialized_view
            where 
                api_url in ( 'sync-invoice-status-check-without-code', 'invoice_status_check_without_code') 
                and idp_code = ANY(:idp_id)
        """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            query1_result = db.execute(text(query1), [{'idp_id': idp_id}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id}])
            query3_result = db.execute(text(query3), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            query1 = query1 + "and period between (:fromDate)::date and (:toDate)::date"
            query2 = query2 + "and period between (:fromDate)::date and (:toDate)::date"
            query3 = query3 + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query2_result = db.execute(text(query2), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            query3_result = db.execute(text(query3), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))
            query2_result = db.execute(text(query2))
            query3_result = db.execute(text(query3))

        logger.info(f"query1_result {query1_result}")
        logger.info(f"query2_result {query2_result}")
        logger.info(f"query3_result {query3_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        columns = query2_result.keys()
        data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]

        columns = query3_result.keys()
        data3 = [dict(zip(columns, row)) for row in query3_result.fetchall()]

        data = {
            'statusCheck': data1,
            'statusCheckWithEntity': data2,
            'statusCheckWithoutEntity': data3
        }
        if request_data.get('filterType', '') == 'all':
            status_check = GetInvoiceHubMisReport.cumulative_status_check_data(data['statusCheck']) if data['statusCheck'] != [] else []
            status_check_with_entity = GetInvoiceHubMisReport.cumulative_status_check_data(data['statusCheckWithEntity']) if data['statusCheckWithEntity'] != [] else []
            status_check_without_entity = GetInvoiceHubMisReport.cumulative_status_check_data(data['statusCheckWithoutEntity']) if data['statusCheckWithoutEntity'] != [] else []
            data = {
                'statusCheck': status_check,
                'statusCheckWithEntity': status_check_with_entity,
                'statusCheckWithoutEntity': status_check_without_entity
            }
        return data

    @staticmethod
    def cumulative_hub_mis_data(query_data, type):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()
        # if type in ('registrationAPI', 'invoiceRegWithEC', 'invoiceRegWithoutEC'):
        if type == 'registration':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['Incoming Ping', 'Pass', 'Fail', '% Pass', '# Invoice Req', '# Inv Pass', 'Avg. Inv/Ping']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'finance':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Of Request', '# Successful', 'Funding Ok %', '# Invoices Request', '% of Invoices Ok']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        elif type == 'cancellation':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Request', '# Successful']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type in ('disburse', 'repayment'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '# of Updated']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'statusCheck':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '% Success']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        cumulative_list = pd.concat([ftd_reg_df, mtd_reg_df, ytd_reg_df, lmst_reg_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_hub_mis_data(request_data, db):

        reg_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as "Incoming Ping", 
                total_pass as "Pass", 
                total_fail as "Fail", 
                total_pass_perc as "% Pass",
                total_invoice_req as "# Invoice Req", 
                total_invoice_pass as "# Inv Pass", 
                total_avg_inv_ping as "Avg. Inv/Ping" 
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-registration-without-code', 'async-registration-without-code', 
                              'sync-invoice-registration-with-code', 'async-invoice-registration-with-code', 
                              'sync-entity-registration', 'async-entity-registration', 
                              'async-bulk-registration-with-code',  'async-bulk-registration-without-codes'
                            )
                and idp_code = ANY(:idp_id)
            """

        finance_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url,
                total_of_request as "# Of Request",
                total_successful as "# Successful", 
                "Funding Ok %", 
                total_invoices_request as "# Invoices Request", 
                "% of Invoices Ok" 
            from 
                financing_api_materialized_view
            where 
                api_url in ('syncFinancing', 'asyncFinancing')
                and idp_code = ANY(:idp_id)
        """

        cancel_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_request as "# Request", 
                total_successful as "# Successful" 
            from 
                cancellation_api_materialized_view
            where 
                api_url in ('cancel', 'Cancellation API', 'Ledger Cancellation API', 'Invoice Cancellation API')
                and idp_code = ANY(:idp_id)
            """

        disburse_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated"  
            from 
                disbursement_api_materialized_view
            where 
                api_url in ('syncDisbursement', 'asyncDisbursement')
                and idp_code = ANY(:idp_id)
            """

        repay_query = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "# of Updated" 
            from 
                repayment_api_materialized_view
            where 
                api_url in ('syncRepayment', 'asyncRepayment')
                and idp_code = ANY(:idp_id)
        """

        status_check_query = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "% Success" 
            from 
                status_check_api_materialized_view
            where 
                api_url in ('sync-ledger-status-check', 'async-ledger-status-check',
                'sync-invoice-status-check-with-code', 'async-invoice-status-check-with-code'
                'sync-invoice-status-check-without-code', 'async-invoice-status-check-without-code')
                and idp_code = ANY(:idp_id)
        """

        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            reg_query = reg_query + "and period between (:fromDate)::date and (:toDate)::date"
            finance_query = finance_query + "and period between (:fromDate)::date and (:toDate)::date"
            cancel_query = cancel_query + "and period between (:fromDate)::date and (:toDate)::date"
            disburse_query = disburse_query + "and period between (:fromDate)::date and (:toDate)::date"
            repay_query = repay_query + "and period between (:fromDate)::date and (:toDate)::date"
            status_check_query = status_check_query + "and period between (:fromDate)::date and (:toDate)::date"

            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")

            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id,'fromDate': from_date, 'toDate': to_date}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            reg_result = db.execute(text(reg_query))
            finance_result = db.execute(text(finance_query))
            cancel_result = db.execute(text(cancel_query))
            disburse_result = db.execute(text(disburse_query))
            repay_result = db.execute(text(repay_query))
            status_check_result = db.execute(text(status_check_query))

        logger.info(f"reg_result {reg_result}")
        logger.info(f"finance_result {finance_result}")
        logger.info(f"cancel_result {cancel_result}")
        logger.info(f"disburse_result {disburse_result}")
        logger.info(f"query3_result {repay_result}")
        logger.info(f"query3_result {status_check_result}")

        columns = reg_result.keys()
        data1 = [dict(zip(columns, row)) for row in reg_result.fetchall()]

        columns = finance_result.keys()
        data2 = [dict(zip(columns, row)) for row in finance_result.fetchall()]

        columns = cancel_result.keys()
        data3 = [dict(zip(columns, row)) for row in cancel_result.fetchall()]

        columns = disburse_result.keys()
        data4 = [dict(zip(columns, row)) for row in disburse_result.fetchall()]

        columns = repay_result.keys()
        data5 = [dict(zip(columns, row)) for row in repay_result.fetchall()]

        columns = status_check_result.keys()
        data6 = [dict(zip(columns, row)) for row in status_check_result.fetchall()]

        sheets = {
            'registration': data1, 'finance': data2, 'cancellation': data3,
            'disburse': data4, 'repayment': data5, 'statusCheck': data6
        }
        if request_data.get('filterType', '') == 'all':
            registration = GetInvoiceHubMisReport.cumulative_hub_mis_data(sheets['registration'], 'registration') if sheets['registration'] != [] else []
            finance = GetInvoiceHubMisReport.cumulative_hub_mis_data(sheets['finance'], 'finance') if sheets['finance'] != [] else []
            cancel = GetInvoiceHubMisReport.cumulative_hub_mis_data(sheets['cancellation'], 'cancellation') if sheets['cancellation'] != [] else []
            disburse = GetInvoiceHubMisReport.cumulative_hub_mis_data(sheets['disburse'], 'disburse') if sheets['disburse'] != [] else []
            repayment = GetInvoiceHubMisReport.cumulative_hub_mis_data(sheets['repayment'], 'repayment') if sheets['repayment'] != [] else []
            status_check = GetInvoiceHubMisReport.cumulative_hub_mis_data(sheets['statusCheck'], 'statusCheck') if sheets['statusCheck'] != [] else []
            sheets = {
                'registration': registration,
                'finance': finance,
                'cancellation': cancel,
                'disburse': disburse,
                'repayment': repayment,
                'statusCheck': status_check
            }
        return sheets

    @staticmethod
    def cumulative_direct_ibdic_data(query_data, type):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()
        # if type in ('registrationAPI', 'invoiceRegWithEC', 'invoiceRegWithoutEC'):
        if type == 'registration':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['Incoming Ping', 'Pass', 'Fail', '% Pass', '# Invoice Req', '# Inv Pass', 'Avg. Inv/Ping']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'finance':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Successful', 'Funding Ok %', '# Invoices Request', '% of Invoices Ok']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        elif type == 'cancellation':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Request', '# Successful']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type in ('disburse', 'repayment'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '# of Updated']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'statusCheck':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '% Success']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        cumulative_list = pd.concat([ftd_reg_df, mtd_reg_df, ytd_reg_df, lmst_reg_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_direct_ibdic_data(request_data, db):

        reg_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as "Incoming Ping", 
                total_pass as "Pass", 
                total_fail as "Fail",  
                total_pass_perc as "% Pass", 
                total_invoice_req as "# Invoice Req", 
                total_invoice_pass as "# Inv Pass", 
                total_avg_inv_ping as "Avg. Inv/Ping"
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-registration-without-code', 'async-registration-without-code', 
                              'sync-invoice-registration-with-code', 'async-invoice-registration-with-code', 
                              'sync-entity-registration', 'async-entity-registration', 
                              'async-bulk-registration-with-code',  'async-bulk-registration-without-codes'
                            ) 
                and idp_code = ANY(:idp_id)
            """

        finance_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url,
                total_successful as "# Successful", 
                "Funding Ok %", 
                total_invoices_request as "# Invoices Request", 
                "% of Invoices Ok" 
            from 
                financing_api_materialized_view
            where 
                api_url in ('syncFinancing', 'asyncFinancing')
                and idp_code = ANY(:idp_id)
        """

        cancel_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_request as "# Request", 
                total_successful as "# Successful" 
            from 
                cancellation_api_materialized_view
            where 
                api_url in ('cancel', 'Cancellation API', 'Ledger Cancellation API', 'Invoice Cancellation API')
                and idp_code = ANY(:idp_id)
            """

        disburse_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated"  
            from 
                disbursement_api_materialized_view
            where 
                api_url in ('syncDisbursement', 'asyncDisbursement')
                and idp_code = ANY(:idp_id)
            """

        repay_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated" 
            from 
                repayment_api_materialized_view
            where 
                api_url in ('syncRepayment', 'asyncRepayment')
                and idp_code = ANY(:idp_id)
        """

        status_check_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "% Success" 
            from 
                status_check_api_materialized_view
            where 
                api_url in ('sync-ledger-status-check', 'async-ledger-status-check',
                            'sync-invoice-status-check-with-code', 'async-invoice-status-check-with-code'
                            'sync-invoice-status-check-without-code', 'async-invoice-status-check-without-code'
                            )
                and idp_code = ANY(:idp_id)
            """

        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            reg_query = reg_query + "and period between (:fromDate)::date and (:toDate)::date"
            finance_query = finance_query + "and period between (:fromDate)::date and (:toDate)::date"
            cancel_query = cancel_query + "and period between (:fromDate)::date and (:toDate)::date"
            disburse_query = disburse_query + "and period between (:fromDate)::date and (:toDate)::date"
            repay_query = repay_query + "and period between (:fromDate)::date and (:toDate)::date"
            status_check_query = status_check_query + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")

            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            reg_result = db.execute(text(reg_query))
            finance_result = db.execute(text(finance_query))
            cancel_result = db.execute(text(cancel_query))
            disburse_result = db.execute(text(disburse_query))
            repay_result = db.execute(text(repay_query))
            status_check_result = db.execute(text(status_check_query))

        logger.info(f"reg_result {reg_result}")
        logger.info(f"finance_result {finance_result}")
        logger.info(f"cancel_result {cancel_result}")
        logger.info(f"disburse_result {disburse_result}")
        logger.info(f"repay_result {repay_result}")
        logger.info(f"status_check_result {status_check_result}")

        columns = reg_result.keys()
        data1 = [dict(zip(columns, row)) for row in reg_result.fetchall()]

        columns = finance_result.keys()
        data2 = [dict(zip(columns, row)) for row in finance_result.fetchall()]

        columns = cancel_result.keys()
        data3 = [dict(zip(columns, row)) for row in cancel_result.fetchall()]

        columns = disburse_result.keys()
        data4 = [dict(zip(columns, row)) for row in disburse_result.fetchall()]

        columns = repay_result.keys()
        data5 = [dict(zip(columns, row)) for row in repay_result.fetchall()]

        columns = status_check_result.keys()
        data6 = [dict(zip(columns, row)) for row in status_check_result.fetchall()]

        sheets = {
            'registration': data1, 'finance': data2, 'cancellation': data3,
            'disburse': data4, 'repayment': data5, 'statusCheck': data6
        }
        if request_data.get('filterType', '') == 'all':

            registration = GetInvoiceHubMisReport.cumulative_direct_ibdic_data(sheets['registration'], 'registration') if sheets['registration'] != [] else []
            finance = GetInvoiceHubMisReport.cumulative_direct_ibdic_data(sheets['finance'], 'finance') if sheets['finance'] != [] else []
            cancel = GetInvoiceHubMisReport.cumulative_direct_ibdic_data(sheets['cancellation'], 'cancellation') if sheets['cancellation'] != [] else []
            disburse = GetInvoiceHubMisReport.cumulative_direct_ibdic_data(sheets['disburse'], 'disburse') if sheets['disburse'] != [] else []
            repayment = GetInvoiceHubMisReport.cumulative_direct_ibdic_data(sheets['repayment'], 'repayment') if sheets['repayment'] != [] else []
            status_check = GetInvoiceHubMisReport.cumulative_direct_ibdic_data(sheets['statusCheck'], 'statusCheck') if sheets['statusCheck'] != [] else []
            sheets = {
                'registration': registration,
                'finance': finance,
                'cancellation': cancel,
                'disburse': disburse,
                'repayment': repayment,
                'statusCheck': status_check
            }
        return sheets

    @staticmethod
    def cumulative_total_business_data(query_data, type):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()
        # if type in ('registrationAPI', 'invoiceRegWithEC', 'invoiceRegWithoutEC'):
        if type == 'registration':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['Incoming Ping', 'Pass', 'Fail', '% Pass', '# Invoice Req', '# Inv Pass', 'Avg. Inv/Ping']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "Incoming Ping": 'sum',
                "Pass": 'sum',
                "Fail": 'sum',
                "% Pass": 'mean',
                # "% Duplicate": 'mean',
                # "% Repeat": 'mean',
                "# Invoice Req": 'sum',
                "# Inv Pass": 'sum',
                "Avg. Inv/Ping": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'finance':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            ftd_df['# Invoices Request'] = pd.to_numeric(ftd_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Of Request', '# Successful', 'Funding Ok %', '# Invoices Request', '% of Invoices Ok']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(
                0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Of Request": 'sum',
                "# Successful": 'sum',
                "Funding Ok %": 'mean',
                "# Invoices Request": 'sum',
                "% of Invoices Ok": 'mean'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        elif type == 'cancellation':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Request', '# Successful']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Successful": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type in ('disburse', 'repayment'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '# of Updated']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'statusCheck':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '% Success']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        cumulative_list = pd.concat([ftd_reg_df, mtd_reg_df, ytd_reg_df, lmst_reg_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_total_business_data(request_data, db):

        reg_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as "Incoming Ping", 
                total_pass as "Pass", 
                total_fail as "Fail",  
                total_pass_perc as "% Pass", 
                total_invoice_req as "# Invoice Req", 
                total_invoice_pass as "# Inv Pass", 
                total_avg_inv_ping as "Avg. Inv/Ping"
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-registration-without-code', 'async-registration-without-code', 
                              'sync-invoice-registration-with-code', 'async-invoice-registration-with-code', 
                              'sync-entity-registration', 'async-entity-registration', 
                              'async-bulk-registration-with-code',  'async-bulk-registration-without-codes'
                            ) 
                and idp_code = ANY(:idp_id)
            """

        finance_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url,
                total_of_request as "# Of Request",
                total_successful as "# Successful", 
                "Funding Ok %", 
                total_invoices_request as "# Invoices Request", 
                "% of Invoices Ok" 
            from 
                financing_api_materialized_view
            where 
                api_url in ('syncFinancing', 'asyncFinancing')
                and idp_code = ANY(:idp_id)
        """

        cancel_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_request as "# Request", 
                total_successful as "# Successful" 
            from 
                cancellation_api_materialized_view
            where 
                api_url in ('cancel', 'Cancellation API', 'Ledger Cancellation API', 'Invoice Cancellation API')
                and idp_code = ANY(:idp_id)
            """

        disburse_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated"  
            from 
                disbursement_api_materialized_view
            where 
                api_url in ('syncDisbursement', 'asyncDisbursement')
                and idp_code = ANY(:idp_id)
            """

        repay_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated" 
            from 
                repayment_api_materialized_view
            where 
                api_url in ('syncRepayment', 'asyncRepayment')
                and idp_code = ANY(:idp_id)
        """

        status_check_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "% Success" 
            from 
                status_check_api_materialized_view
            where 
                api_url in ('sync-ledger-status-check', 'async-ledger-status-check',
                            'sync-invoice-status-check-with-code', 'async-invoice-status-check-with-code'
                            'sync-invoice-status-check-without-code', 'async-invoice-status-check-without-code'
                            )
                and idp_code = ANY(:idp_id)
            """

        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            reg_query = reg_query + "and period between (:fromDate)::date and (:toDate)::date"
            finance_query = finance_query + "and period between (:fromDate)::date and (:toDate)::date"
            cancel_query = cancel_query + "and period between (:fromDate)::date and (:toDate)::date"
            disburse_query = disburse_query + "and period between (:fromDate)::date and (:toDate)::date"
            repay_query = repay_query + "and period between (:fromDate)::date and (:toDate)::date"
            status_check_query = status_check_query + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")

            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            reg_result = db.execute(text(reg_query))
            finance_result = db.execute(text(finance_query))
            cancel_result = db.execute(text(cancel_query))
            disburse_result = db.execute(text(disburse_query))
            repay_result = db.execute(text(repay_query))
            status_check_result = db.execute(text(status_check_query))

        logger.info(f"reg_result {reg_result}")
        logger.info(f"finance_result {finance_result}")
        logger.info(f"cancel_result {cancel_result}")
        logger.info(f"disburse_result {disburse_result}")
        logger.info(f"repay_result {repay_result}")
        logger.info(f"status_check_result {status_check_result}")

        columns = reg_result.keys()
        data1 = [dict(zip(columns, row)) for row in reg_result.fetchall()]

        columns = finance_result.keys()
        data2 = [dict(zip(columns, row)) for row in finance_result.fetchall()]

        columns = cancel_result.keys()
        data3 = [dict(zip(columns, row)) for row in cancel_result.fetchall()]

        columns = disburse_result.keys()
        data4 = [dict(zip(columns, row)) for row in disburse_result.fetchall()]

        columns = repay_result.keys()
        data5 = [dict(zip(columns, row)) for row in repay_result.fetchall()]

        columns = status_check_result.keys()
        data6 = [dict(zip(columns, row)) for row in status_check_result.fetchall()]

        sheets = {
            'registration': data1, 'finance': data2, 'cancellation': data3,
            'disburse': data4, 'repayment': data5, 'statusCheck': data6
        }
        if request_data.get('filterType', '') == 'all':
            registration = GetInvoiceHubMisReport.cumulative_total_business_data(sheets['registration'],'registration') if sheets['registration'] != [] else []
            finance = GetInvoiceHubMisReport.cumulative_total_business_data(sheets['finance'], 'finance') if sheets['finance'] != [] else []
            cancel = GetInvoiceHubMisReport.cumulative_total_business_data(sheets['cancellation'], 'cancellation') if sheets['cancellation'] != [] else []
            disburse = GetInvoiceHubMisReport.cumulative_total_business_data(sheets['disburse'], 'disburse') if sheets['disburse'] != [] else []
            repayment = GetInvoiceHubMisReport.cumulative_total_business_data(sheets['repayment'], 'repayment') if sheets['repayment'] != [] else []
            status_check = GetInvoiceHubMisReport.cumulative_total_business_data(sheets['statusCheck'], 'statusCheck') if sheets['statusCheck'] != [] else []
            sheets = {
                'registration': registration,
                'finance': finance,
                'cancellation': cancel,
                'disburse': disburse,
                'repayment': repayment,
                'statusCheck': status_check
            }
        return sheets

    @staticmethod
    def get_idp_wise_billing_mis_data(request_data, db):
        ## IDP Wise Billing MIS
        query1 = """
            select 						
                row_number() over() AS s_no,
                coalesce( arl.response_data->>'channel', '') as hub,
                to_date(to_char(arl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as date,
                arl.api_url as typeOfApi,
                to_char( arl.created_at AT TIME ZONE 'Asia/Kolkata', 'HH:MI:SS:US') as requestTime,
                to_char( arl.updated_at AT TIME ZONE 'Asia/Kolkata', 'HH:MI:SS:US') as responseTime,	
                coalesce( arl.response_data->>'message', '') as statusOfApi 
            from
                api_request_log arl
            inner join merchant_details md on md.id::text = arl.merchant_id
            where 
                md.unique_id = ANY(:idp_id)
                and arl.created_at::date between (:fromDate)::date and (:toDate)::date
            order by 
                --arl.created_at::date desc 
                s_no asc
            ;
            """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if from_date != "" and to_date != "":
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))

        logger.info(f"query1_result {query1_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        # data = {'IdpWiseBillingMis': data1}
        data = {'UsageMISforIDP': data1}
        return data

    @staticmethod
    def cumulative_idp_wise_data(query_data, type):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()
        # if type in ('registrationAPI', 'invoiceRegWithEC', 'invoiceRegWithoutEC'):
        if type == 'all_api_calls':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "incoming": 'sum',
                "successful": 'sum',
                "% Success": 'mean'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['incoming', 'successful', '% Success']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "incoming": 'sum',
                "successful": 'sum',
                "% Success": 'mean'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "incoming": 'sum',
                "successful": 'sum',
                "% Success": 'mean'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "incoming": 'sum',
                "successful": 'sum',
                "% Success": 'mean'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'registration':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "enquiry": 'sum',
                "successful": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['enquiry', 'successful', 'Avg Inc/Ping']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "enquiry": 'sum',
                "successful": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "enquiry": 'sum',
                "successful": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "enquiry": 'sum',
                "successful": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'finance':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['% of funded Inv'] = pd.to_numeric(ftd_df['% of funded Inv'], errors='coerce').fillna(0).astype(float)
            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum'
                #"% of funded Inv": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '# Successful', '# of Invoices']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        elif type == 'cancellation':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'mean'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Request', '% Successful']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'mean'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'mean'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'mean'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type in ('disburse', 'repayment'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '% Success']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'statusCheck':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '% Success']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'mean'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        cumulative_list = pd.concat([ftd_reg_df, mtd_reg_df, ytd_reg_df, lmst_reg_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_idp_wise_data(request_data, db):

        all_api_call_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as incoming, 
                total_successful as successful,
                total_success_perc as "% Success",
                '0'::text as "% Time Outliers"
            from 
                total_calls_for_all_api_materialized_view
            where 
                idp_code = ANY(:idp_id)
            """

        reg_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as enquiry, 
                total_pass as successful,  
                total_invoice_req as "No of Inv",
                total_avg_inv_ping as "Avg Inc/Ping",
                '0'::text as "% Time Outliers"
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-registration-without-code', 'async_registration_without_code', 
                              'sync-invoice-registration-with-code', 'async_invoice_registration_with_code', 
                              'sync-entity-registration', 'async_entity_registration', 
                              'async_bulk_registration_with_code',  'async_bulk_registration_without_codes'
                            )
                and idp_code = ANY(:idp_id)
            """

        finance_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url,
                total_of_request as "# of Request",
                total_successful as "# Successful", 
                total_invoices_request as "# of Invoices",
                '-'::text As "% of funded Inv",
                '0'::text as "% Time Outliers"
            from 
                financing_api_materialized_view
            where 
                api_url in ('syncFinancing', 'asyncFinancing')
                and idp_code = ANY(:idp_id)
            """

        cancel_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_request as "# Request", 
                total_success_percent as "% Successful",
                '0'::text as "% Time Outliers"
            from 
                cancellation_api_materialized_view
            where 
                api_url in ('cancel', 'Cancellation API', 'Ledger Cancellation API', 'Invoice Cancellation API')
                and idp_code = ANY(:idp_id)
            """

        disburse_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "% Success"
            from 
                disbursement_api_materialized_view
            where 
                api_url in ('syncDisbursement', 'asyncDisbursement')
                and idp_code = ANY(:idp_id)
            """

        repay_query = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "% Success"
            from 
                repayment_api_materialized_view
            where 
                api_url in ('syncRepayment', 'asyncRepayment')
                and idp_code = ANY(:idp_id)
            """

        status_check_query = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "% Success", 
                '0'::text as "% Time Outliers"
            from 
                status_check_api_materialized_view
            where 
                api_url in ('sync-ledger-status-check', 'async-ledger-status-check',
                'sync-invoice-status-check-with-code', 'async-invoice-status-check-with-code'
                'sync-invoice-status-check-without-code', 'async-invoice-status-check-without-code')
                and idp_code = ANY(:idp_id)
            """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            all_api_call_result = db.execute(text(all_api_call_query), [{'idp_id': idp_id}])
            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            all_api_call_query = all_api_call_query + "and period between (:fromDate)::date and (:toDate)::date"
            reg_query = reg_query + "and period between (:fromDate)::date and (:toDate)::date"
            finance_query = finance_query + "and period between (:fromDate)::date and (:toDate)::date"
            cancel_query = cancel_query + "and period between (:fromDate)::date and (:toDate)::date"
            disburse_query = disburse_query + "and period between (:fromDate)::date and (:toDate)::date"
            repay_query = repay_query + "and period between (:fromDate)::date and (:toDate)::date"
            status_check_query = status_check_query + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")

            all_api_call_result = db.execute(text(all_api_call_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            finance_result = db.execute(text(finance_query),
                                        [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            cancel_result = db.execute(text(cancel_query),
                                       [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            disburse_result = db.execute(text(disburse_query),
                                         [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            status_check_result = db.execute(text(status_check_query),
                                             [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            all_api_call_result = db.execute(text(all_api_call_query))
            reg_result = db.execute(text(reg_query))
            finance_result = db.execute(text(finance_query))
            cancel_result = db.execute(text(cancel_query))
            disburse_result = db.execute(text(disburse_query))
            repay_result = db.execute(text(repay_query))
            status_check_result = db.execute(text(status_check_query))

        logger.info(f"all_api_call_result {all_api_call_result}")
        logger.info(f"reg_result {reg_result}")
        logger.info(f"finance_result {finance_result}")
        logger.info(f"cancel_result {cancel_result}")
        logger.info(f"disburse_result {disburse_result}")
        logger.info(f"query3_result {repay_result}")
        logger.info(f"query3_result {status_check_result}")

        columns = all_api_call_result.keys()
        data = [dict(zip(columns, row)) for row in all_api_call_result.fetchall()]

        columns = reg_result.keys()
        data1 = [dict(zip(columns, row)) for row in reg_result.fetchall()]

        columns = finance_result.keys()
        data2 = [dict(zip(columns, row)) for row in finance_result.fetchall()]

        columns = cancel_result.keys()
        data3 = [dict(zip(columns, row)) for row in cancel_result.fetchall()]

        columns = disburse_result.keys()
        data4 = [dict(zip(columns, row)) for row in disburse_result.fetchall()]

        columns = repay_result.keys()
        data5 = [dict(zip(columns, row)) for row in repay_result.fetchall()]

        columns = status_check_result.keys()
        data6 = [dict(zip(columns, row)) for row in status_check_result.fetchall()]

        sheets = {
            'all_api_calls': data,
            'registration': data1, 'finance': data2, 'cancellation': data3,
            'disburse': data4, 'repayment': data5, 'statusCheck': data6
        }
        if request_data.get('filterType', '') == 'all':
            all_api_calls = GetInvoiceHubMisReport.cumulative_idp_wise_data(sheets['all_api_calls'], 'all_api_calls') if sheets['all_api_calls'] != [] else []
            registration = GetInvoiceHubMisReport.cumulative_idp_wise_data(sheets['registration'], 'registration') if sheets['registration'] != [] else []
            finance = GetInvoiceHubMisReport.cumulative_idp_wise_data(sheets['finance'], 'finance') if sheets['finance'] != [] else []
            cancel = GetInvoiceHubMisReport.cumulative_idp_wise_data(sheets['cancellation'], 'cancellation') if sheets['cancellation'] != [] else []
            disburse = GetInvoiceHubMisReport.cumulative_idp_wise_data(sheets['disburse'], 'disburse') if sheets['disburse'] != [] else []
            repayment = GetInvoiceHubMisReport.cumulative_idp_wise_data(sheets['repayment'], 'repayment') if sheets['repayment'] != [] else []
            status_check = GetInvoiceHubMisReport.cumulative_idp_wise_data(sheets['statusCheck'], 'statusCheck') if sheets['statusCheck'] != [] else []
            sheets = {
                'all_api_calls': all_api_calls,
                'registration': registration,
                'finance': finance,
                'cancellation': cancel,
                'disburse': disburse,
                'repayment': repayment,
                'statusCheck': status_check
            }
        return sheets

    @staticmethod
    def cumulative_idp_wise_daily_trend_data(query_data, type):
        from datetime import datetime

        df = pd.DataFrame(query_data)
        # df.to_json()
        # now_time = datetime.now().replace(day=26, month=3)
        now_time = datetime.now()
        curr_date = now_time.date()
        # if type in ('registrationAPI', 'invoiceRegWithEC', 'invoiceRegWithoutEC'):
        if type == 'all_api_calls':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "% Success": 'mean',
                "% Time Outliers": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Request', '# Success', '% Success', '% Time Outliers']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "% Success": 'mean',
                "% Time Outliers": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "% Success": 'mean',
                "% Time Outliers": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "% Success": 'mean',
                "% Time Outliers": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'registration':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Request', '# Success', 'Avg Inc/Ping', '% Time Outliers']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "# Success": 'sum',
                "No of Inv": 'sum',
                "Avg Inc/Ping": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'finance':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['% of funded Inv'] = pd.to_numeric(ftd_df['% of funded Inv'], errors='coerce').fillna(0).astype(float)
            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum',
                "% Time Outliers": 'sum'
                # "% of funded Inv": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '# Successful', '# of Invoices', '% Time Outliers']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Request'] = pd.to_numeric(mtd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Request'] = pd.to_numeric(ytd_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Request'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# Successful": 'sum',
                "# of Invoices": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        elif type == 'cancellation':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# Request', '% Successful', '% Time Outliers']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# Request": 'sum',
                "% Successful": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type in ('disburse', 'repayment'):
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '# of Updated', '% Time Outliers']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "# of Updated": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'
        elif type == 'statusCheck':
            ftd_df = df
            ftd_df = ftd_df[ftd_df.period == curr_date]
            # ftd_df['# Invoices Req'] = pd.to_numeric(ftd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)

            ftd_reg_df = ftd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            # Round numeric columns to two decimal places
            numeric_cols = ['# of Request', '% Success', '% Time Outliers']
            ftd_reg_df[numeric_cols] = ftd_reg_df[numeric_cols].astype(float).round(2)
            ftd_reg_df['period'] = 'FTD'

            mtd_df = df
            curr_month = now_time.date().month
            mtd_df = mtd_df[pd.to_datetime(mtd_df.period).dt.month == curr_month]
            # mtd_df['# Invoices Req'] = pd.to_numeric(mtd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            mtd_reg_df = mtd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            mtd_reg_df[numeric_cols] = mtd_reg_df[numeric_cols].astype(float).round(2)
            mtd_reg_df['period'] = 'MTD'

            ytd_df = df
            curr_year = now_time.date().year
            ytd_df = ytd_df[pd.to_datetime(ytd_df.period).dt.year == curr_year]  # '2024'
            # ytd_df['# Invoices Req'] = pd.to_numeric(ytd_df['# Invoices Req'], errors='coerce').fillna(0).astype(float)
            ytd_reg_df = ytd_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            ytd_reg_df[numeric_cols] = ytd_reg_df[numeric_cols].astype(float).round(2)
            ytd_reg_df['period'] = 'YTD'

            lmst_df = df
            lmst_date = now_time.replace(month=curr_month) - relativedelta(months=1)
            lmst_month = lmst_date.month
            lmst_df = lmst_df[pd.to_datetime(lmst_df.period).dt.month == lmst_month]
            # lmst_df['# Invoices Req'] = pd.to_numeric(lmst_df['# Invoices Request'], errors='coerce').fillna(0).astype(float)
            lmst_reg_df = lmst_df.groupby(['category', 'idp_name', 'idp_code']).agg({
                "# of Request": 'sum',
                "% Success": 'sum',
                "% Time Outliers": 'sum'
            }).reset_index()
            lmst_reg_df[numeric_cols] = lmst_reg_df[numeric_cols].astype(float).round(2)
            lmst_reg_df['period'] = 'LMST'

        cumulative_list = pd.concat([ftd_reg_df, mtd_reg_df, ytd_reg_df, lmst_reg_df])
        cumulative_list.reset_index(drop=True)
        cumulative_list.sort_values('idp_name', ascending=True, inplace=True)
        cols = list(cumulative_list.columns)
        cols.remove('period')  # Remove 'period' from its current position
        cols.insert(3, 'period')  # Insert 'period' at the desired position
        cumulative_list = cumulative_list[cols]
        cumulative_list = jsonable_encoder(cumulative_list.to_dict(orient='records'))
        return cumulative_list

    @staticmethod
    def get_idp_wise_daily_trend(request_data, db):

        all_api_call_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as "# Request", 
                total_successful as "# Success",
                total_success_perc as "% Success",
                '0'::text as "% Time Outliers"
            from 
                total_calls_for_all_api_materialized_view
            where 
                idp_code = ANY(:idp_id)
            """

        reg_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_incoming_ping as "# Request", 
                total_pass as "# Success",  
                total_invoice_req as "No of Inv",
                total_avg_inv_ping as "Avg Inc/Ping",
                '0'::text as "% Time Outliers"
            from 
                registration_api_materialized_view
            where 
                api_url in ( 'sync-registration-without-code', 'async_registration_without_code', 
                              'sync-invoice-registration-with-code', 'async_invoice_registration_with_code', 
                              'sync-entity-registration', 'async_entity_registration', 
                              'async_bulk_registration_with_code',  'async_bulk_registration_without_codes'
                            )
                and idp_code = ANY(:idp_id)
            """

        finance_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url,
                total_of_request as "# of Request",
                total_successful as "# Successful", 
                total_invoices_request as "# of Invoices",
                '-'::text As "% of funded Inv",
                '0'::text as "% Time Outliers"
            from 
                financing_api_materialized_view
            where 
                api_url in ('syncFinancing', 'asyncFinancing')
                and idp_code = ANY(:idp_id)
        """

        cancel_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                total_request as "# Request", 
                total_success_percent as "% Successful",
                '0'::text as "% Time Outliers"
            from 
                cancellation_api_materialized_view
            where 
                api_url in ('cancel', 'Cancellation API', 'Ledger Cancellation API', 'Invoice Cancellation API')
                and idp_code = ANY(:idp_id)
            """

        disburse_query = """
            select 
                category, 
                idp_name, 
                idp_code, 
                period, 
                api_url, 
                "# of Request", 
                "# of Updated",
                '0'::text as "% Time Outliers"
            from 
                disbursement_api_materialized_view
            where 
                api_url in ('syncDisbursement', 'asyncDisbursement')
                and idp_code = ANY(:idp_id)
            """

        repay_query = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "# of Updated", 
                '0'::text as "% Time Outliers"
            from 
                repayment_api_materialized_view
            where 
                api_url in ('syncRepayment', 'asyncRepayment')
                and idp_code = ANY(:idp_id)
        """

        status_check_query = """
            select 
                category, idp_name, idp_code, period, api_url, 
                "# of Request", 
                "% Success", 
                '0'::text as "% Time Outliers"
            from 
                status_check_api_materialized_view
            where 
                api_url in ('sync-ledger-status-check', 'async-ledger-status-check',
                'sync-invoice-status-check-with-code', 'async-invoice-status-check-with-code'
                'sync-invoice-status-check-without-code', 'async-invoice-status-check-without-code')
                and idp_code = ANY(:idp_id)
        """
        idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if request_data.get('filterType', '') == 'all':
            all_api_call_result = db.execute(text(all_api_call_query), [{'idp_id': idp_id}])
            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id}])
        elif from_date != "" and to_date != "":
            all_api_call_query = all_api_call_query + "and period between (:fromDate)::date and (:toDate)::date"
            reg_query = reg_query + "and period between (:fromDate)::date and (:toDate)::date"
            finance_query = finance_query + "and period between (:fromDate)::date and (:toDate)::date"
            cancel_query = cancel_query + "and period between (:fromDate)::date and (:toDate)::date"
            disburse_query = disburse_query + "and period between (:fromDate)::date and (:toDate)::date"
            repay_query = repay_query + "and period between (:fromDate)::date and (:toDate)::date"
            status_check_query = status_check_query + "and period between (:fromDate)::date and (:toDate)::date"
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")

            all_api_call_result = db.execute(text(all_api_call_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            reg_result = db.execute(text(reg_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            finance_result = db.execute(text(finance_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            cancel_result = db.execute(text(cancel_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            disburse_result = db.execute(text(disburse_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            repay_result = db.execute(text(repay_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
            status_check_result = db.execute(text(status_check_query), [{'idp_id': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            all_api_call_result = db.execute(text(all_api_call_query))
            reg_result = db.execute(text(reg_query))
            finance_result = db.execute(text(finance_query))
            cancel_result = db.execute(text(cancel_query))
            disburse_result = db.execute(text(disburse_query))
            repay_result = db.execute(text(repay_query))
            status_check_result = db.execute(text(status_check_query))

        logger.info(f"all_api_call_result {all_api_call_result}")
        logger.info(f"reg_result {reg_result}")
        logger.info(f"finance_result {finance_result}")
        logger.info(f"cancel_result {cancel_result}")
        logger.info(f"disburse_result {disburse_result}")
        logger.info(f"query3_result {repay_result}")
        logger.info(f"query3_result {status_check_result}")

        columns = all_api_call_result.keys()
        data = [dict(zip(columns, row)) for row in all_api_call_result.fetchall()]

        columns = reg_result.keys()
        data1 = [dict(zip(columns, row)) for row in reg_result.fetchall()]

        columns = finance_result.keys()
        data2 = [dict(zip(columns, row)) for row in finance_result.fetchall()]

        columns = cancel_result.keys()
        data3 = [dict(zip(columns, row)) for row in cancel_result.fetchall()]

        columns = disburse_result.keys()
        data4 = [dict(zip(columns, row)) for row in disburse_result.fetchall()]

        columns = repay_result.keys()
        data5 = [dict(zip(columns, row)) for row in repay_result.fetchall()]

        columns = status_check_result.keys()
        data6 = [dict(zip(columns, row)) for row in status_check_result.fetchall()]

        sheets = {
            'all_api_calls': data,
            'registration': data1, 'finance': data2, 'cancellation': data3,
            'disburse': data4, 'repayment': data5, 'statusCheck': data6
        }
        if request_data.get('filterType', '') == 'all':
            all_api_calls = GetInvoiceHubMisReport.cumulative_idp_wise_daily_trend_data(sheets['all_api_calls'], 'all_api_calls') if sheets['all_api_calls'] != [] else []
            registration = GetInvoiceHubMisReport.cumulative_idp_wise_daily_trend_data(sheets['registration'], 'registration') if sheets['registration'] != [] else []
            finance = GetInvoiceHubMisReport.cumulative_idp_wise_daily_trend_data(sheets['finance'], 'finance') if sheets['finance'] != [] else []
            cancel = GetInvoiceHubMisReport.cumulative_idp_wise_daily_trend_data(sheets['cancellation'], 'cancellation') if sheets['cancellation'] != [] else []
            disburse = GetInvoiceHubMisReport.cumulative_idp_wise_daily_trend_data(sheets['disburse'], 'disburse') if sheets['disburse'] != [] else []
            repayment = GetInvoiceHubMisReport.cumulative_idp_wise_daily_trend_data(sheets['repayment'], 'repayment') if sheets['repayment'] != [] else []
            status_check = GetInvoiceHubMisReport.cumulative_idp_wise_daily_trend_data(sheets['statusCheck'], 'statusCheck') if sheets['statusCheck'] != [] else []
            sheets = {
                'all_api_calls': all_api_calls,
                'registration': registration,
                'finance': finance,
                'cancellation': cancel,
                'disburse': disburse,
                'repayment': repayment,
                'statusCheck': status_check
            }
        return sheets

    @staticmethod
    def get_consent_data(request_data, db):
        ## Consent Data
        query1 = """
            select 
                gud.gstin as gstin, 
                gud.gsp as gsp_name,	
                gud.pan as pan, 
                gud.email as email, 
                gud.mobile_number as mobile, 
                'IBDIC UI':: text as reference,
                to_date(to_char(gud.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as created_at,
                to_date(to_char(gud.updated_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as updated_at
            from 
                gsp_user_details gud
            where	
                gud.created_at::date between (:fromDate)::date and (:toDate)::date
            """
        # idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if from_date != "" and to_date != "":
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))

        logger.info(f"query1_result {query1_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        data = {'consentData': data1}
        return data

    @staticmethod
    def get_gsp_api_calls(request_data, db):
        ## GSP API Calls
        # query1 = """
        #     select
        #         to_date(to_char(garl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as request_date,
        #         TO_CHAR(garl.created_at, 'HH24:MI:SS') as request_time,
        #         garl.request_id as gsp_reference_number,
        #         'success'::text as status_of_request_with_gsp,
        #         coalesce( ( select
        #                         i.extra_data->>'validation_type'
        #                     from
        #                         gsp_api_request_log as inner_garl
        #                     inner join invoice i on i.extra_data->>'ewb_no' = inner_garl.request_data->'payload'->0->>'ewbNumber'
        #                     where
        #                         inner_garl.request_id=garl.request_id
        #                     limit 1
        #         ), '') as validationType,
        #         coalesce( ( select
        #                         inner_garl.request_data->'payload'->0->>'ewbNumber'
        #                     from
        #                         gsp_api_request_log as inner_garl
        #                     inner join invoice i on i.extra_data->>'ewb_no' = inner_garl.request_data->'payload'->0->>'ewbNumber'
        #                     where
        #                         inner_garl.request_id=garl.request_id
        #                     limit 1
        #         ), '') as validationId,
        #         coalesce( ( select
        #                         case when i.gst_status='true' then 'Y' else 'N' end
        #                     from
        #                         gsp_api_request_log as inner_garl
        #                     inner join invoice i on i.extra_data->>'ewb_no' = inner_garl.request_data->'payload'->0->>'ewbNumber'
        #                     where
        #                         inner_garl.request_id=garl.request_id
        #                     limit 1
        #         ), '') as gstinVerified,
        #         ''::text as idpId,
        #         ''::text as hubId,
        #         garl.api_url as api_call_type
        #     from
        #         gsp_api_request_log garl
        #     where
        #         garl.created_at::date between (:fromDate)::date and (:toDate)::date
        # ;
        # """
        query1 = """
            select 
                data.request_date, data.request_time, data.gsp_reference_number, data.status_of_request_with_gsp, 
                data.validationType, data.validationId, data.gstinVerified, data.idpId, data.hubId, data.api_call_type
            from 
            (
            select 
                to_date(to_char(garl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as request_date,
                TO_CHAR(garl.created_at, 'HH24:MI:SS') as request_time,
                garl.request_id as gsp_reference_number,
                'success'::text as status_of_request_with_gsp,
                coalesce( ( select 
                                i.extra_data->>'validation_type'
                            from 
                                invoice i 
                            where 
                                i.extra_data->>'ewb_no'=payload_data->>'ewbNumber'
                            limit 1
                ), '') as validationType,
                coalesce( ( select 
                                payload_data->>'ewbNumber'
                            from 
                                invoice i 
                            where 
                                i.extra_data->>'ewb_no'=payload_data->>'ewbNumber'
                            limit 1
                            ), '') as validationId,
                coalesce( ( select 
                                case when i.gst_status='true' then 'Y' else 'N' end 
                            from 
                                invoice i 
                            where 
                                i.extra_data->>'ewb_no'=payload_data->>'ewbNumber'
                            limit 1
                            ), '') as gstinVerified,
                ''::text as idpId,
                ''::text as hubId,
                split_part(garl.api_url,'/', array_upper(string_to_array(garl.api_url, '/'), 1)) as api_call_type
            from 
                gsp_api_request_log as garl
            cross join lateral jsonb_array_elements(garl.request_data->'payload') AS payload_data
            where	
                split_part(garl.api_url,'/', array_upper(string_to_array(garl.api_url, '/'), 1)) in ('verify-ewb')
                and garl.created_at::date between (:fromDate)::date and (:toDate)::date
            union all
            select
                to_date(to_char(garl.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as request_date,
                TO_CHAR(garl.created_at, 'HH24:MI:SS') as request_time,
                garl.request_id as gsp_reference_number,
                'success'::text as status_of_request_with_gsp,
                coalesce( ( select
                                i.extra_data->>'validation_type'
                            from
                                gsp_api_request_log as inner_garl
                            inner join invoice i on i.extra_data->>'ewb_no' = inner_garl.request_data->'payload'->0->>'ewbNumber'
                            where
                                inner_garl.request_id=garl.request_id
                            limit 1
                ), '') as validationType,
                coalesce( ( select
                                inner_garl.request_data->'payload'->0->>'ewbNumber'
                            from
                                gsp_api_request_log as inner_garl
                            inner join invoice i on i.extra_data->>'ewb_no' = inner_garl.request_data->'payload'->0->>'ewbNumber'
                            where
                                inner_garl.request_id=garl.request_id
                            limit 1
                ), '') as validationId,
                coalesce( ( select
                                case when i.gst_status='true' then 'Y' else 'N' end
                            from
                                gsp_api_request_log as inner_garl
                            inner join invoice i on i.extra_data->>'ewb_no' = inner_garl.request_data->'payload'->0->>'ewbNumber'
                            where
                                inner_garl.request_id=garl.request_id
                            limit 1
                ), '') as gstinVerified,
                ''::text as idpId,
                ''::text as hubId,
                split_part(garl.api_url,'/', array_upper(string_to_array(garl.api_url, '/'), 1)) as api_call_type
            from
                gsp_api_request_log garl
            where
                split_part(garl.api_url,'/', array_upper(string_to_array(garl.api_url, '/'), 1)) in ('verify-ewb-credential')
                and garl.created_at::date between (:fromDate)::date and (:toDate)::date
        ) as data
        order by 
            (data.request_date, data.request_time)  desc
        ;
        """

        # idp_id = request_data.get('idpId', '')
        from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        if from_date != "" and to_date != "":
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
        else:
            query1_result = db.execute(text(query1))

        logger.info(f"query1_result {query1_result}")

        columns = query1_result.keys()
        data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]

        data = {'gspApiCalls': data1}
        return data


@router.post("/get-invoice-hub-mis-report/")
def get_invoice_hub_mis_report(request: GetInvoiceHubMisReportSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    report_type_list = ['finance', 'registration', 'cancellation', 'disbursement', 'repayment', 'statusCheck', 'misHub',
                        'directIBDIC', 'totalBusiness', 'summary', 'UsageMISforIDP', 'IdpWiseDailyTrend',
                        'IdpWise', 'consentData', 'gspApiCalls']
    try:
        response_data = {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(200)
        }

        # idp_id = request_data.get('idpId', '')
        # merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == idp_id).first()
        # merchant_key = merchant_details and merchant_details.merchant_key or None
        if request_data.get('filterType', '') == 'all':
            idp_ids = db.query(models.MerchantDetails.unique_id).all()
            idp_id = [unique_id for (unique_id,) in idp_ids]
            request_data.update({'idpId': idp_id})

        if request_data.get('reportType', '') not in report_type_list:
            return {
                "requestId": request_data.get('requestId'),
                **ErrorCodes.get_error_response(1132)
            }

        report_type = request_data.get('reportType', '')
        data = []
        if report_type == 'finance':
            data = GetInvoiceHubMisReport.get_finance_data(request_data, db)
        elif report_type == 'registration':
            data = GetInvoiceHubMisReport.get_registration_data(request_data, db)
        elif report_type == 'cancellation':
            data = GetInvoiceHubMisReport.get_cancel_data(request_data, db)
        elif report_type == 'disbursement':
            data = GetInvoiceHubMisReport.get_disbursement_data(request_data, db)
        elif report_type == 'repayment':
            data = GetInvoiceHubMisReport.get_repayment_data(request_data, db)
        elif report_type == 'statusCheck':
            data = GetInvoiceHubMisReport.get_status_check_data(request_data, db)
        elif report_type == 'misHub':
            data = GetInvoiceHubMisReport.get_hub_mis_data(request_data, db)
        elif report_type == 'directIBDIC':
            data = GetInvoiceHubMisReport.get_direct_ibdic_data(request_data, db)
        elif report_type == 'totalBusiness':
            data = GetInvoiceHubMisReport.get_total_business_data(request_data, db)
        elif report_type == 'summary':
            data = GetInvoiceHubMisReport.get_total_business_data(request_data, db)
        elif report_type == 'UsageMISforIDP':    #'IdpWiseBillingMis'
            data = GetInvoiceHubMisReport.get_idp_wise_billing_mis_data(request_data, db)
        elif report_type == 'IdpWiseDailyTrend':
            data = GetInvoiceHubMisReport.get_idp_wise_daily_trend(request_data, db)
        elif report_type == 'IdpWise':
            data = GetInvoiceHubMisReport.get_idp_wise_data(request_data, db)
        elif report_type == 'consentData':
            data = GetInvoiceHubMisReport.get_consent_data(request_data, db)
        elif report_type == 'gspApiCalls':
            data = GetInvoiceHubMisReport.get_gsp_api_calls(request_data, db)
        else:
            return {
                'code': 200,
                'message': 'invalid reportType'
            }
        response_data.update({'data': data})
        return response_data

        # query = mis_report_query(request_data)
        # from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        # if from_date != "" and to_date != "":
        #     from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        #     to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        #
        #     result = db.execute(text(query), [{'fromDate': from_date, 'toDate': to_date}])
        # else:
        #     result = db.execute(text(query))
        # # logger.info(f"result {result}")
        # # response_data.update(result)
        # # rows = result.fetchall()
        # # rows = [dict(row) for row in result]
        # # rows = [dict(row.items()) for row in rows]
        # columns = result.keys()
        # rows = [dict(zip(columns, row)) for row in result.fetchall()]
        # data = {'data': []}
        # if rows:
        #     data = {'data': rows}
        #     response_data.update(data)
        #     return response_data
        # response_data.update(data)
        # return response_data
    except Exception as e:
        logger.exception(f"Exception get_invoice_hub_mis_report :: {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


# class DownloadInvoiceHubMisReport:
#
#     @staticmethod
#     def gen_finance_xlsx(request_data, db):
#         query1 = """
#         select
#             category, idp_name, idp_code, created_date, period, api_url, "# Of Request", "# Successful",
#             "Funding Ok %", "Repeat %", "Duplicate%", "Amount of Request", "# Invoices Request", "% of Invoices Ok",
#             "Amount Ok for funding", "% Funding Value"
#         from
#             financing_api_materialized_view
#         where
#             api_url in ('syncFinancing') and
#             created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         query2 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# Of Request", "# Successful",
#                 "Funding Ok %", "Repeat %", "Duplicate%", "Amount of Request", "# Invoices Request", "% of Invoices Ok",
#                 "Amount Ok for funding", "% Funding Value"
#             from
#                 financing_api_materialized_view
#             where
#                 api_url in ('asyncFinancing') and
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#         query3 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# Of Request", "# Successful",
#                 "Funding Ok %", "Repeat %", "Duplicate%", "Amount of Request", "# Invoices Request", "% of Invoices Ok",
#                 "Amount Ok for funding", "% Funding Value"
#             from
#                 financing_api_materialized_view
#             where
#                 api_url in ('asyncFinancing') and
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#         from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
#         if from_date != "" and to_date != "":
#             from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
#             query2_result = db.execute(text(query2), [{'fromDate': from_date, 'toDate': to_date}])
#             query3_result = db.execute(text(query3), [{'fromDate': from_date, 'toDate': to_date}])
#         else:
#             query1_result = db.execute(text(query1))
#             query2_result = db.execute(text(query2))
#             query3_result = db.execute(text(query3))
#
#         logger.info(f"query1_result {query1_result}")
#         logger.info(f"query2_result {query2_result}")
#         logger.info(f"query3_result {query3_result}")
#
#         columns = query1_result.keys()
#         data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]
#         df1 = pd.DataFrame(data1)
#
#         columns = query1_result.keys()
#         data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]
#         df2 = pd.DataFrame(data2)
#
#         columns = query1_result.keys()
#         data3 = [dict(zip(columns, row)) for row in query3_result.fetchall()]
#         df3 = pd.DataFrame(data3)
#
#         sheets = {
#             'FinancingAPI': df1,
#             'FinancingAPIWithEntityCode': df2,
#             'FinancingAPIWithoutEntityCode': df3
#         }
#         return sheets
#
#     @staticmethod
#     def gen_registration_xlsx(request_data, db):
#
#         query1 = """
#             select
#             category, idp_name, idp_code, created_date, period, api_url, "Incoming Ping", "Pass", "Fail", "% Pass",
#             "% Duplicate", "% Repeat", "# Invoice Req", "# Inv Pass", "Avg. Inv/Ping"
#             from
#                 registration_api_materialized_view
#             where
#                 api_url in ( 'sync-registration-without-code', 'async-registration-without-code',
#                               'sync-invoice-registration-with-code', 'async-invoice-registration-with-code',
#                               'sync-entity-registration', 'async-entity-registration',
#                               'async-bulk-registration-with-code',  'async-bulk-registration-without-codes'
#                             )  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         query2 = """
#             select
#             category, idp_name, idp_code, created_date, period, api_url, "Incoming Ping", "Pass", "Fail", "% Pass",
#             "% Duplicate", "% Repeat", "# Invoice Req", "# Inv Pass", "Avg. Inv/Ping"
#             from
#                 registration_api_materialized_view
#             where
#                 api_url in ( 'sync-entity-registration', 'async-entity-registration' ) AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         query3 = """
#             select
#             category, idp_name, idp_code, created_date, period, api_url, "Incoming Ping", "Pass", "Fail", "% Pass",
#             "% Duplicate", "% Repeat", "# Invoice Req", "# Inv Pass", "Avg. Inv/Ping"
#             from
#                 registration_api_materialized_view
#             where
#                 api_url in ( 'sync-invoice-registration-with-code', 'async-invoice-registration-with-code',
#                              'async-bulk-registration-with-code' ) AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         query4 = """
#             select
#             category, idp_name, idp_code, created_date, period, api_url, "Incoming Ping", "Pass", "Fail", "% Pass",
#             "% Duplicate", "% Repeat", "# Invoice Req", "# Inv Pass", "Avg. Inv/Ping"
#             from
#                 registration_api_materialized_view
#             where
#                 api_url in ( 'sync-registration-without-code', 'async-registration-without-code',
#                              'async-bulk-registration-without-codes' ) AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
#         if from_date != "" and to_date != "":
#             from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
#             query2_result = db.execute(text(query2), [{'fromDate': from_date, 'toDate': to_date}])
#             query3_result = db.execute(text(query3), [{'fromDate': from_date, 'toDate': to_date}])
#             query4_result = db.execute(text(query4), [{'fromDate': from_date, 'toDate': to_date}])
#         else:
#             query1_result = db.execute(text(query1))
#             query2_result = db.execute(text(query2))
#             query3_result = db.execute(text(query3))
#             query4_result = db.execute(text(query4))
#
#         logger.info(f"query1_result {query1_result}")
#         logger.info(f"query2_result {query2_result}")
#         logger.info(f"query3_result {query3_result}")
#         logger.info(f"query3_result {query4_result}")
#
#         columns = query1_result.keys()
#         data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]
#         df1 = pd.DataFrame(data1)
#
#         columns = query2_result.keys()
#         data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]
#         df2 = pd.DataFrame(data2)
#
#         columns = query3_result.keys()
#         data3 = [dict(zip(columns, row)) for row in query3_result.fetchall()]
#         df3 = pd.DataFrame(data3)
#
#         columns = query4_result.keys()
#         data4 = [dict(zip(columns, row)) for row in query4_result.fetchall()]
#         df4 = pd.DataFrame(data4)
#
#         sheets = {
#             'RegistrationAPI': df1,
#             'EntityRegistration': df2,
#             'InvoiceRegWithEC': df3,
#             'InvoiceRegWithoutEC': df4
#         }
#         return sheets
#
#     @staticmethod
#     def gen_cancel_xlsx(request_data, db):
#         ## Cancellation API, Ledger Cancellation API, Invoice Cancellation API
#         query1 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# Request", "# Successful",
#                 "% of Invoices Requested", "# Inv. Cancelled"
#             from
#                 cancellation_api_materialized_view
#             where
#                 api_url in ('cancel', 'Cancellation API')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#             """
#
#         query2 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# Request", "# Successful",
#                 "% of Invoices Requested", "# Inv. Cancelled"
#             from
#                 cancellation_api_materialized_view
#             where
#                 api_url in ( 'Ledger Cancellation API')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#             """
#
#         query3 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# Request", "# Successful",
#                 "% of Invoices Requested", "# Inv. Cancelled"
#             from
#                 cancellation_api_materialized_view
#             where
#                 api_url in ( 'Invoice Cancellation API')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#             """
#
#         from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
#         if from_date != "" and to_date != "":
#             from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
#             query2_result = db.execute(text(query2), [{'fromDate': from_date, 'toDate': to_date}])
#             query3_result = db.execute(text(query3), [{'fromDate': from_date, 'toDate': to_date}])
#         else:
#             query1_result = db.execute(text(query1))
#             query2_result = db.execute(text(query2))
#             query3_result = db.execute(text(query3))
#
#         logger.info(f"query1_result {query1_result}")
#         logger.info(f"query2_result {query2_result}")
#         logger.info(f"query3_result {query3_result}")
#
#         columns = query1_result.keys()
#         data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]
#         df1 = pd.DataFrame(data1)
#
#         columns = query2_result.keys()
#         data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]
#         df2 = pd.DataFrame(data2)
#
#         columns = query3_result.keys()
#         data3 = [dict(zip(columns, row)) for row in query3_result.fetchall()]
#         df3 = pd.DataFrame(data3)
#
#         sheets = {
#             'CancellationAPI': df1,
#             'LedgerCancellationAPI': df2,
#             'InvoiceCancellationAPI': df3
#         }
#         return sheets
#
#     @staticmethod
#     def gen_disbursement_xlsx(request_data, db):
#         ## Disbursal API
#         query1 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# of Request", "# of Updated",
#                 "# Financed but not Disb", "% Unfunded"
#             from
#                 disbursement_api_materialized_view
#             where
#                 api_url in ('syncDisbursement', 'asyncDisbursement') AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
#         if from_date != "" and to_date != "":
#             from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
#         else:
#             query1_result = db.execute(text(query1))
#
#         logger.info(f"query1_result {query1_result}")
#
#         columns = query1_result.keys()
#         data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]
#         df1 = pd.DataFrame(data1)
#
#         sheets = {'Disbursal API': df1}
#         return sheets
#
#     @staticmethod
#     def gen_repayment_xlsx(request_data, db):
#         ## Repayment API, Invoice Repayment %
#         query1 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# of Request",
#                 "# of Updated", "# Repaid but not Disb"
#             from
#                 repayment_api_materialized_view
#             where
#                 api_url in ('syncRepayment', 'asyncRepayment') AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#             """
#
#         from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
#         if from_date != "" and to_date != "":
#             from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
#         else:
#             query1_result = db.execute(text(query1))
#
#         logger.info(f"query1_result {query1_result}")
#
#         columns = query1_result.keys()
#         data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]
#         df1 = pd.DataFrame(data1)
#
#         sheets = {
#             'RepaymentAPI': df1,
#             'InvoiceRepayment%': df1
#         }
#         return sheets
#
#     @staticmethod
#     def gen_status_check_xlsx(request_data, db):
#         ## Status Check, Status Check with Entity, Status Ch without Entity
#         query1 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# of Request", "% Success"
#             from
#                 status_check_api_materialized_view
#             where
#                 api_url in ('sync-ledger-status-check', 'async-ledger-status-check')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         query2 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# of Request", "% Success"
#             from
#                 status_check_api_materialized_view
#             where
#                 api_url in ('sync-invoice-status-check-with-code', 'async-invoice-status-check-with-code')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         query3 = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url, "# of Request", "% Success"
#             from
#                 status_check_api_materialized_view
#             where
#                 api_url in ( 'sync-invoice-status-check-without-code', 'async-invoice-status-check-without-code')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
#         if from_date != "" and to_date != "":
#             from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             query1_result = db.execute(text(query1), [{'fromDate': from_date, 'toDate': to_date}])
#             query2_result = db.execute(text(query2), [{'fromDate': from_date, 'toDate': to_date}])
#             query3_result = db.execute(text(query3), [{'fromDate': from_date, 'toDate': to_date}])
#         else:
#             query1_result = db.execute(text(query1))
#             query2_result = db.execute(text(query2))
#             query3_result = db.execute(text(query3))
#
#         logger.info(f"query1_result {query1_result}")
#         logger.info(f"query2_result {query2_result}")
#         logger.info(f"query3_result {query3_result}")
#
#         columns = query1_result.keys()
#         data1 = [dict(zip(columns, row)) for row in query1_result.fetchall()]
#         df1 = pd.DataFrame(data1)
#
#         columns = query2_result.keys()
#         data2 = [dict(zip(columns, row)) for row in query2_result.fetchall()]
#         df2 = pd.DataFrame(data2)
#
#         columns = query3_result.keys()
#         data3 = [dict(zip(columns, row)) for row in query3_result.fetchall()]
#         df3 = pd.DataFrame(data3)
#
#         sheets = {
#             'StatusCheck': df1,
#             'StatusCheckWithEntity': df2,
#             'StatusCheckWithoutEntity': df3
#         }
#         return sheets
#
#     @staticmethod
#     def gen_hub_mis_xlsx(request_data, db):
#
#         reg_query = """
#             select
#             category, idp_name, idp_code, created_date, period, api_url,
#             "Incoming Ping", "Pass", "Fail", "% Pass", "# Invoice Req", "# Inv Pass", "Avg. Inv/Ping"
#             from
#                 registration_api_materialized_view
#             where
#                 api_url in ( 'sync-registration-without-code', 'async-registration-without-code',
#                               'sync-invoice-registration-with-code', 'async-invoice-registration-with-code',
#                               'sync-entity-registration', 'async-entity-registration',
#                               'async-bulk-registration-with-code',  'async-bulk-registration-without-codes'
#                             )  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#             """
#
#         finance_query = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url,
#                "# Successful", "Funding Ok %", "# Invoices Request", "% of Invoices Ok"
#             from
#                 financing_api_materialized_view
#             where
#                 api_url in ('syncFinancing', 'asyncFinancing') and
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         cancel_query = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url,
#                 "# Request", "# Successful"
#             from
#                 cancellation_api_materialized_view
#             where
#                 api_url in ('cancel', 'Cancellation API', 'Ledger Cancellation API', 'Invoice Cancellation API')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#             """
#
#         disburse_query = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url,
#                 "# of Request", "# of Updated"
#             from
#                 disbursement_api_materialized_view
#             where
#                 api_url in ('syncDisbursement', 'asyncDisbursement') AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#             """
#
#         repay_query = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url,
#                 "# of Request", "# of Updated"
#             from
#                 repayment_api_materialized_view
#             where
#                 api_url in ('syncRepayment', 'asyncRepayment') AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         status_check_query = """
#             select
#                 category, idp_name, idp_code, created_date, period, api_url,
#                 "# of Request", "% Success"
#             from
#                 status_check_api_materialized_view
#             where
#                 api_url in ('sync-ledger-status-check', 'async-ledger-status-check',
#                 'sync-invoice-status-check-with-code', 'async-invoice-status-check-with-code'
#                 'sync-invoice-status-check-without-code', 'async-invoice-status-check-without-code')  AND
#                 created_date between (:fromDate)::date and (:toDate)::date
#         """
#
#         from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
#         if from_date != "" and to_date != "":
#             from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#             to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
#
#             reg_result = db.execute(text(reg_query), [{'fromDate': from_date, 'toDate': to_date}])
#             finance_result = db.execute(text(finance_query), [{'fromDate': from_date, 'toDate': to_date}])
#             cancel_result = db.execute(text(cancel_query), [{'fromDate': from_date, 'toDate': to_date}])
#             disburse_result = db.execute(text(disburse_query), [{'fromDate': from_date, 'toDate': to_date}])
#             repay_result = db.execute(text(repay_query), [{'fromDate': from_date, 'toDate': to_date}])
#             status_check_result = db.execute(text(status_check_query), [{'fromDate': from_date, 'toDate': to_date}])
#         else:
#             reg_result = db.execute(text(reg_query))
#             finance_result = db.execute(text(finance_query))
#             cancel_result = db.execute(text(cancel_query))
#             disburse_result = db.execute(text(disburse_query))
#             repay_result = db.execute(text(repay_query))
#             status_check_result = db.execute(text(status_check_query))
#
#         logger.info(f"reg_result {reg_result}")
#         logger.info(f"finance_result {finance_result}")
#         logger.info(f"cancel_result {cancel_result}")
#         logger.info(f"disburse_result {disburse_result}")
#         logger.info(f"query3_result {repay_result}")
#         logger.info(f"query3_result {status_check_result}")
#
#         columns = reg_result.keys()
#         data1 = [dict(zip(columns, row)) for row in reg_result.fetchall()]
#         df1 = pd.DataFrame(data1)
#
#         columns = finance_result.keys()
#         data2 = [dict(zip(columns, row)) for row in finance_result.fetchall()]
#         df2 = pd.DataFrame(data2)
#
#         columns = cancel_result.keys()
#         data3 = [dict(zip(columns, row)) for row in cancel_result.fetchall()]
#         df3 = pd.DataFrame(data3)
#
#         columns = disburse_result.keys()
#         data4 = [dict(zip(columns, row)) for row in disburse_result.fetchall()]
#         df4 = pd.DataFrame(data4)
#
#         columns = repay_result.keys()
#         data5 = [dict(zip(columns, row)) for row in repay_result.fetchall()]
#         df5 = pd.DataFrame(data5)
#
#         columns = status_check_result.keys()
#         data6 = [dict(zip(columns, row)) for row in status_check_result.fetchall()]
#         df6 = pd.DataFrame(data6)
#
#         sheets = {
#             'registration': df1, 'finance': df2, 'cancel': df3,
#             'disburse': df4, 'repayment': df5, 'statusCheck': df6
#         }
#         return sheets


@router.post("/download-invoice-hub-mis-report/")
def download_invoice_hub_mis_report(request: GetInvoiceHubMisReportSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    report_type_list = ['finance', 'registration', 'cancellation', 'disbursement', 'repayment', 'statusCheck', 'misHub',
                        'directIBDIC', 'totalBusiness', 'summary', 'UsageMISforIDP', 'IdpWiseDailyTrend',
                        'IdpWise', 'consentData', 'gspApiCalls']
    try:
        response_data = {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(200)
        }
        # idp_id = request_data.get('idpId', '')
        # merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == idp_id).first()
        # merchant_key = merchant_details and merchant_details.merchant_key or None
        if request_data.get('filterType', '') == 'all':
            idp_ids = db.query(models.MerchantDetails.unique_id).all()
            idp_id = [unique_id for (unique_id,) in idp_ids]
            request_data.update({'idpId': idp_id})

        if request_data.get('reportType', '') not in report_type_list:
            return {
                "requestId": request_data.get('requestId'),
                **ErrorCodes.get_error_response(1132)
            }
        report_type = request_data.get('reportType', '')
        sheets = {}
        if report_type == 'finance':
            sheets = GetInvoiceHubMisReport.get_finance_data(request_data, db)
        elif report_type == 'registration':
            sheets = GetInvoiceHubMisReport.get_registration_data(request_data, db)
        elif report_type == 'cancellation':
            sheets = GetInvoiceHubMisReport.get_cancel_data(request_data, db)
        elif report_type == 'disbursement':
            sheets = GetInvoiceHubMisReport.get_disbursement_data(request_data, db)
        elif report_type == 'repayment':
            sheets = GetInvoiceHubMisReport.get_repayment_data(request_data, db)
        elif report_type == 'statusCheck':
            sheets = GetInvoiceHubMisReport.get_status_check_data(request_data, db)
        elif report_type == 'misHub':
            sheets = GetInvoiceHubMisReport.get_hub_mis_data(request_data, db)
        elif report_type == 'directIBDIC':
            sheets = GetInvoiceHubMisReport.get_direct_ibdic_data(request_data, db)
        elif report_type == 'totalBusiness':
            sheets = GetInvoiceHubMisReport.get_total_business_data(request_data, db)
        elif report_type == 'summary':
            sheets = GetInvoiceHubMisReport.get_total_business_data(request_data, db)
        elif report_type == 'UsageMISforIDP': #'IdpWiseBillingMis'
            sheets = GetInvoiceHubMisReport.get_idp_wise_billing_mis_data(request_data, db)
        elif report_type == 'IdpWiseDailyTrend':
            sheets = GetInvoiceHubMisReport.get_idp_wise_daily_trend(request_data, db)
        elif report_type == 'IdpWise':
            sheets = GetInvoiceHubMisReport.get_idp_wise_data(request_data, db)
        elif report_type == 'consentData':
            sheets = GetInvoiceHubMisReport.get_consent_data(request_data, db)
        elif report_type == 'gspApiCalls':
            sheets = GetInvoiceHubMisReport.get_gsp_api_calls(request_data, db)
        else:
            return {
                'code': 200,
                'message': 'invalid reportType'
            }
        # Create a BytesIO buffer to hold the Excel file
        output = BytesIO()
        if sheets:
            for sheet in sheets:
                value = sheets.get(sheet)
                df = pd.DataFrame(value)
                sheets[sheet] = df
            # Use ExcelWriter to write multiple sheets to the buffer
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                for sheet_name, df in sheets.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Set the buffer's position to the beginning
            output.seek(0)

            ## sample fileName :: # 29_May_2024_11:50_InvoiceData_invoice registred_10_May_2024to27_May_2024
            curr_datetime = dt.now(asia_kolkata).strftime("%d_%b_%Y_%H:%M")
            from_date = request_data.get("fromDate", '')
            to_date = request_data.get("toDate", '')
            if from_date != "" and to_date != "":
                from_date = dt.strptime(request_data.get("fromDate"), "%d/%m/%Y").strftime("%d_%b_%Y")
                to_date = dt.strptime(request_data.get("toDate"), "%d/%m/%Y").strftime("%d_%b_%Y")
                file_name = curr_datetime + '_' + report_type + '_' + from_date + 'to' + to_date
            else:
                file_name = curr_datetime + '_' + report_type

            headers = {'Content-Disposition': f'attachment; filename="{file_name}.xlsx"'}
            # Return the Excel file as a response
            return Response(
                content=output.getvalue(),
                media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                headers=headers
            )

        return {
            **ErrorCodes.get_error_response(500)
        }
        # query = download_mis_report_query(request_data)
        # from_date, to_date = request_data.get("fromDate"), request_data.get("toDate")
        # if from_date != "" and to_date != "":
        #     from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        #     to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
        #     result = db.execute(text(query), [{'fromDate': from_date, 'toDate': to_date}])
        # else:
        #     result = db.execute(text(query))
        #
        # logger.info(f"result {result}")
        #
        # columns = result.keys()
        # data = [dict(zip(columns, row)) for row in result.fetchall()]
        # df = pd.DataFrame(data)
        #
        # # Write DataFrame to an Excel file
        # # df.to_excel('data.xlsx', index=False)
        # from io import BytesIO
        # from fastapi import FastAPI, Response
        # output = BytesIO()
        # df.to_excel(output, index=False)
        # output.seek(0)
        #
        # # Return the Excel file as a response
        # return Response(content=output.getvalue(),
        #                 media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        #                 headers={'Content-Disposition': 'attachment; filename="data.xlsx"'})
        # from fastapi.responses import FileResponse
        # from openpyxl import Workbook
        # # Create a new workbook
        # wb = Workbook()
        #
        # # Select the active worksheet
        # ws = wb.active
        #
        # # Add headers from the first JSON object
        # # headers = list(json_data[0].keys())
        # headers = list(columns)
        # ws.append(headers)
        #
        # # Add data rows from JSON objects
        # for item in rows:
        #     row = [item[header] for header in headers]
        #     ws.append(row)
        #
        # # Save the workbook to a file
        # file_path = "data.xlsx"
        # wb.save(file_path)
        #
        #
        # # Return the file as a response
        # return FileResponse(file_path, filename="data.xlsx")
    except Exception as e:
        logger.exception(f"Exception download_invoice_hub_mis_report :: {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


class GetUserMisReport:

    @staticmethod
    def entity_registered_data(request_data, db):
        entity_registered_query = """
        select 
            e.id as entityId, 
            ec.entity_code as "Entity Code", 
            ( SELECT eil.entity_id_name FROM entity_identifier_line eil WHERE eil.entity_id=e.id and eil.entity_id_name is not null and eil.entity_id_name != '' order by id desc limit 1) AS "Entity Name", 
            to_date(to_char(ec.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as "Created on",
            md.name as "Created by", 
            'NA'::text as "Authorised by",
            ( SELECT
                CASE
                WHEN EXISTS (SELECT 1 FROM entity_identifier_line WHERE entity_id_type = 'gstin' and entity_id=e.id)
                THEN 'Y'
                ELSE 'N'
                END AS result
            ) as GSTIN,
            ( SELECT
                CASE
                WHEN EXISTS (SELECT 1 FROM entity_identifier_line WHERE entity_id_type = 'pan' and entity_id=e.id)
                THEN 'Y'
                ELSE 'N'
                END AS result
            ) as PAN,
            ( SELECT
                CASE
                WHEN EXISTS (SELECT 1 FROM entity_identifier_line WHERE is_active = true and entity_id=e.id)
                THEN 'Y'
                ELSE 'N'
                END AS result 
            ) as "Active Status"
        from 
            entity_combination ec
        left join entity e on e.id = ec.entity_id 
        inner join merchant_details md on md.id = ec.merchant_id 
        where 
            md.unique_id = ANY(:idpId) and
            ec.created_at::date between (:fromDate)::date and (:toDate)::date
        order by "Created on" desc
        """

        gstin_wise_entity_registered_query = """
        select 
            eil.entity_id_no as GSTIN,
            eil.entity_id_name AS "Entity Name",
            ec.entity_code as "Entity Code",
            e.id as entityId, 
            to_date(to_char(ec.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as "Created on",
            md.name as "Created by", 
            'NA'::text as "Authorised by"
        from 
            entity e 
        inner join merchant_details md on md.id = e.merchant_id 
        inner join entity_combination ec on ec.entity_id = e.id
        inner join entity_identifier_line eil on eil.entity_id = ec.entity_id
        where 
            md.unique_id = ANY(:idpId) and
            eil.entity_id_type ='gstin' and 
            ec.created_at::date between (:fromDate)::date and (:toDate)::date
        order by "Created on" desc
        ;
        """

        entity_id_with_identifiers_query = """
        select 
            data.*
        from 
        (
            select 
                to_date(to_char(ec.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as "Date of entry",	
                coalesce(ec.entity_code, '') as "Entity Code",
                e.id as "Entity Id", 
                eil.entity_id_name as "Entity Name",
                eil.entity_id_type as "Identifier type",
                eil.entity_id_no as "Identifier ID no",
                false as "Verified flag", 
                case when (eil.is_active = true) then 'Y' else 'N' end as "Active Status"
            from
                entity e 
            inner join merchant_details md on md.id = e.merchant_id 
            inner join entity_combination ec on ec.entity_id = e.id
            inner join entity_identifier_line eil on eil.entity_id = e.id
            where 
                md.unique_id = ANY(:idpId) and
                ec.created_at::date between (:fromDate)::date and (:toDate)::date
            union all
            select 
                to_date(to_char(e.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as "Date of entry",
                '-' as "Entity Code",
                e.id as "Entity Id", 
                eil.entity_id_name as "Entity Name",
                eil.entity_id_type as "Identifier type",
                eil.entity_id_no as "Identifier ID no",
                false as "Verified flag", 
                case when (eil.is_active = true) then 'Y' else 'N' end as "Active Status"
            from 
                entity e
            inner join merchant_details md on md.id = e.merchant_id
            left join entity_combination ec on ec.entity_id = e.id
            inner join entity_identifier_line eil on eil.entity_id = e.id
            where 
                md.unique_id = ANY(:idpId) and
                ec.entity_id is null and
                e.created_at::date between (:fromDate)::date and (:toDate)::date
        ) as data 
        order by 
            data."Date of entry" desc
        ;
        """

        entity_ids_having_multiple_gstin_query = """
        select 
            to_date(to_char(ec.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as "Date of entry",	
            ec.entity_code as "Entity Code",
            e.id as "Entity Id", 
            eil.entity_id_name as "Entity Name",
            eil.entity_id_no as "GST",
            false as "Verified flag", 
            case when (eil.is_active = true) then 'Y' else 'N' end as "Active Status"
        from
            entity e 
        inner join merchant_details md on md.id = e.merchant_id 
        inner join entity_combination ec on ec.entity_id = e.id
        inner join entity_identifier_line eil on eil.entity_id = ec.entity_id
        where 
            md.unique_id = ANY(:idpId) and
            eil.entity_id_type ='gstin' and
            ec.created_at::date between (:fromDate)::date and (:toDate)::date
        order by "Date of entry" desc
        ;
        """

        entity_ids_without_gstin_query = """
        select 
            to_date(to_char(ec.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as "Date of entry",    
            ec.entity_code as "Entity Code",
            e.id as "Entity Id", 
            eil.entity_id_name as "Entity Name",
            eil.entity_id_type as "Identifier type",
            eil.entity_id_no as "Identifier ID no",
            false as "Verified flag", 
            case when (eil.is_active = true) then 'Y' else 'N' end as "Active Status"
        from
            entity e 
        inner join entity_combination ec on ec.entity_id = e.id and ec.merchant_id = e.merchant_id
        inner join merchant_details md on md.id = ec.merchant_id
        inner join entity_identifier_line eil on eil.entity_id = ec.entity_id
        where 
            md.unique_id = ANY(:idpId) and
            ec.created_at::date between (:fromDate)::date and (:toDate)::date and
            not exists (
                select 1
                from entity_identifier_line sub_eil
                where sub_eil.entity_id = e.id
                and sub_eil.entity_id_type = 'gstin'
            )
        order by "Date of entry" desc;
        """

        entity_ids_without_pan_query = """
        select 
            to_date(to_char(ec.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as "Date of entry",    
            ec.entity_code as "Entity Code",
            e.id as "Entity Id", 
            eil.entity_id_name as "Entity Name",
            eil.entity_id_type as "Identifier type",
            eil.entity_id_no as "Identifier ID no",
            false as "Verified flag", 
            case when (eil.is_active = true) then 'Y' else 'N' end as "Active Status"
        from
            entity e 
        inner join entity_combination ec on ec.entity_id = e.id and ec.merchant_id = e.merchant_id
        inner join merchant_details md on md.id = ec.merchant_id
        inner join entity_identifier_line eil on eil.entity_id = ec.entity_id
        where 
            md.unique_id = ANY(:idpId) and
            ec.created_at::date between (:fromDate)::date and (:toDate)::date and
            not exists (
                select 1
                from entity_identifier_line sub_eil
                where sub_eil.entity_id = e.id
                and sub_eil.entity_id_type IN ('gstin', 'pan')
            )
        order by "Date of entry" desc;
        """

        query_mapper = {
            "entity_registered": entity_registered_query,
            "gstin_wise_entity_registered": gstin_wise_entity_registered_query,
            "entity_id_with_identifiers": entity_id_with_identifiers_query,
            "entity_ids_having_multiple_gstin": entity_ids_having_multiple_gstin_query,
            "entity_ids_without_gstin": entity_ids_without_gstin_query,
            "entity_ids_without_pan": entity_ids_without_pan_query
        }

        report_sub_type = request_data.get('reportSubType', '')
        query = query_mapper.get(report_sub_type)
        idp_id = request_data.get('idpId', '')
        from_date = request_data.get("fromDate")
        to_date = request_data.get("toDate")
        if from_date != "" and to_date != "":
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            result = db.execute(text(query), [{'idpId': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            result = db.execute(text(query))
        logger.info(f"result {result}")

        columns = result.keys()
        data = [dict(zip(columns, row)) for row in result.fetchall()]

        return data

    @staticmethod
    def invoice_data(request_data, db):
        invoices_registered_query = """
            select 
                i.invoice_no as invoiceNo,
                to_char(i.invoice_date::date,'DD/MM/YYYY') as invoiceDate,
                i.invoice_amt as invoiceAmount,
                to_char(i.invoice_due_date, 'DD/MM/YYYY') as invoiceDueDate,
                l.ledger_id as ledgerId,
                i.status as presentInvoiceStatus,
                case when l.status in ('non_funded','funded')
                     then '-'
                     when l.status in ('full_disbursed', 'full_paid', 'repaid')
                     then 'F'
                     when l.status in ('partial_disbursed', 'partial_repaid')
                     then 'P'
                     else '-'
                end as "P/F",
                coalesce( i.extra_data->>'buyer_gst', '') as buyerGST,
                coalesce( (select eil2.entity_id_name from entity e2 inner join entity_identifier_line eil2 on eil2.entity_id = e2.id where (e2.id)::text = (i.extra_data->>'buyer_entity_id') and eil2.entity_id_name is not null and eil2.entity_id_name != '' limit 1), '') as buyerName,
                coalesce( i.extra_data->>'seller_gst', '') as sellerGST,
                coalesce( (select eil3.entity_id_name from entity e3 inner join entity_identifier_line eil3 on eil3.entity_id = e3.id where (e3.id)::text = (i.extra_data->>'seller_entity_id') and eil3.entity_id_name is not null and eil3.entity_id_name != '' limit 1), '') as sellerName,
                to_char(i.created_at, 'DD/MM/YYYY') as createdOn,  
                md.name as createdBy,
                md.name as authorisedBy
            from
                invoice i
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where
                md.unique_id = ANY(:idpId) and
                i.created_at::date between (:fromDate)::date and (:toDate)::date 
            order by createdOn desc
        ;
        """

        ledger_funded_query = """
            select 
                l.ledger_id as ledgerId,
                ( select coalesce( sum(i.invoice_amt), 0)::text
                  from 
                    ledger l2 
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id 
                  inner join invoice i on i.id = ila.invoice_id 
                  where l2.ledger_id = l.ledger_id
                ) as ledgerAmount,
                l.status as ledgerStatus,
                case when l.status in ('funded')
                     then 'F'
                     when l.status in ('partial_funded')
                     then 'P'
                     else '-'
                end as fundingType,
                ( select coalesce( sum(i.funded_amt::numeric), 0::numeric)::text
                  from 
                    ledger l2 
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id 
                  inner join invoice i on i.id = ila.invoice_id 
                  where l2.ledger_id = l.ledger_id
                ) as fundedAmount,
                to_date(to_char(l.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as dateCreated,
                to_date(to_char(l.updated_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as dateOfFunding,
                md.name as fundedBy,
                md.name as authBy
            from 
                ledger l 
            inner join merchant_details md on md.id = l.merchant_id
            where 
                md.unique_id = ANY(:idpId) and
                l.status in ('funded', 'partial_funded') and 
                l.created_at::date between (:fromDate)::date and (:toDate)::date
            order by dateCreated desc
            ;
            """

        invoices_funded_with_invoice_details_query = """
            select 
                l.ledger_id as ledgerId,
                ( select coalesce( sum(i.invoice_amt), 0)::text
                  from 
                    ledger l2 
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id 
                  inner join invoice i on i.id = ila.invoice_id 
                  where l2.ledger_id = l.ledger_id
                ) as ledgerAmount,
                l.status as ledgerStatus,
                l.status as fundingType,
                i.invoice_no as invoiceNo,
                to_char(i.invoice_date::date, 'DD/MM/YYYY') as invoiceDate,
                cast(i.invoice_amt AS VARCHAR) as invoiceAmount,
                to_char(i.invoice_due_date, 'DD/MM/YYYY') as invoiceDueDate,
                coalesce(i.extra_data->'ledgerData'->>'fundingAmtFlag', '') as fundingType,
                i.funded_amt as fundedAmount,
                to_date(to_char(i.created_at::date, 'DD/MM/YYYY'), 'DD/MM/YYYY') as dateCreated,
                coalesce(i.extra_data->'ledgerData'->>'financeRequestDate', '') as dateOfFunding,
                md.name as fundedBy,
                md.name as authBy
            from 
                invoice i
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where
                md.unique_id = ANY(:idpId) and 
                i.status in ('funded', 'partial_funded') and
                i.created_at::date between (:fromDate)::date and (:toDate)::date
            order by i.created_at::date desc
            ;
            """

        invoices_cancelled_query = """
            select 
                l.ledger_id as ledgerId,
                ( select coalesce( sum(i.invoice_amt), 0)::text 
                  from 
                    ledger l2 
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id 
                  inner join invoice i on i.id = ila.invoice_id 
                  where l2.ledger_id = l.ledger_id
                ) as ledgerAmount,
                l.status as ledgerStatus,
                to_date(to_char(l.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as dateCreated,
                i.invoice_no as invoiceNo,
                to_char(i.invoice_date::date, 'DD/MM/YYYY') as invoiceDate,
                cast(i.invoice_amt AS VARCHAR) as invoiceAmount,
                to_char(i.invoice_due_date, 'DD/MM/YYYY') as invoiceDueDate,
                coalesce(i.extra_data->'ledgerData'->>'financeRequestDate', '') as dateOfFunding,
                coalesce(i.extra_data->'ledgerData'->>'fundingAmtFlag', '') as fundingType,
                i.funded_amt as fundedAmount,
                coalesce(l.extra_data->>'cancellationMessage', '') as reasonForCancellation,
                to_date(to_char(l.updated_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as dateOfCancellation,
                '-'::text as initiatedBy,
                md.name as authBy
            from 
                invoice i
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where
                md.unique_id = ANY(:idpId)
                and l.status in ('non_funded', 'cancel') 
                and i.fund_status = false 
                and i.created_at::date between (:fromDate)::date and (:toDate)::date
                and l.extra_data->>'cancellationMessage' is not null
            order by i.created_at::date desc
            ;
            """

        invoices_disbursed_query = """ 
            select 
                l.ledger_id as ledgerId,
                ( select coalesce( sum(i.invoice_amt), 0)::text 
                  from 
                    ledger l2 
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id 
                  inner join invoice i on i.id = ila.invoice_id 
                  where l2.ledger_id = l.ledger_id
                ) as ledgerAmount,
                l.status as ledgerStatus,
                i.invoice_no as invoiceNo,
                to_char(i.invoice_date::date, 'DD/MM/YYYY') as invoiceDate,
                cast(i.invoice_amt AS VARCHAR) as invoiceAmount,
                to_char(i.invoice_due_date, 'DD/MM/YYYY') as invoiceDueDate,
                coalesce(i.extra_data->'ledgerData'->>'fundingAmtFlag', '') as fundingType,
                i.funded_amt as fundedAmount,
                coalesce(i.extra_data->'ledgerData'->>'financeRequestDate', '') as dateOfFunding,
                coalesce(i.extra_data->>'disbursedAmt', '') as disbursedAmount,
                coalesce(i.extra_data->>'disbursedDate', '') as disbDate,
                '-'::text as initiatedBy,
                md.name as authBy
            from 
                invoice i
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where 
                md.unique_id = ANY(:idpId) and
                i.status in ('partial_disbursed', 'full_disbursed') and 
                i.created_at::date between (:fromDate)::date and (:toDate)::date
            order by i.created_at desc
            ;
            """

        ledger_with_partial_funding_query = """ 
            select 
                 l.ledger_id as ledgerId,
                 ( select coalesce( sum(i.invoice_amt), 0)::text
                  from
                    ledger l2
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id
                  inner join invoice i on i.id = ila.invoice_id
                  where 
                    l2.ledger_id = l.ledger_id
                ) as ledgerAmount,
                
                l.status as ledgerStatus,
                
                ( select count(i.id)
                  from
                    ledger l2
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id
                  inner join invoice i on i.id = ila.invoice_id
                  where
                        --i.status in ('non_funded') and
                        l2.ledger_id = l.ledger_id
                ) as noOfInvoicesRegistered,
                
                ( select coalesce( sum(i.invoice_amt), 0)::text
                  from
                    ledger l2
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id
                  inner join invoice i on i.id = ila.invoice_id
                  where
                        --i.status in ('non_funded') and
                        l2.ledger_id = l.ledger_id
                ) as amountOfInvoicesRegistered,
                
                ( select count(i.id)
                  from
                    ledger l2
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id
                  inner join invoice i on i.id = ila.invoice_id
                  where
                        i.status in ('funded', 'partial_funded', 'partial_disbursed', 'full_disbursed', 'repaid', 'partial_repaid', 'full_paid', 'partial_paid') and
                        l2.ledger_id = l.ledger_id
                ) as noOfInvoicesFunded,
                
                ( select coalesce( sum(i.invoice_amt), 0)::text
                  from
                    ledger l2
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id
                  inner join invoice i on i.id = ila.invoice_id
                  where
                        i.status in ('funded', 'partial_funded', 'partial_disbursed', 'full_disbursed', 'repaid', 'partial_repaid', 'full_paid', 'partial_paid') and
                        l2.ledger_id = l.ledger_id
                ) as amountOfFundedAmount,
                
                ( select count(i.id)
                  from
                    ledger l2
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id
                  inner join invoice i on i.id = ila.invoice_id
                  where
                        i.status in ('non_funded') and
                        l2.ledger_id = l.ledger_id
                ) as noOfUnfundedInvoices,
                
                ( select coalesce( sum(i.invoice_amt), 0)::text
                  from
                    ledger l2
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id
                  inner join invoice i on i.id = ila.invoice_id
                  where
                        i.status in ('non_funded') and
                        l2.ledger_id = l.ledger_id
                ) as amountOfUnfundedInvoices
            from 
                invoice i
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where 
                md.unique_id = ANY(:idpId) and
                l.status in ('partial_funded') and 
                l.updated_at::date between (:fromDate)::date and (:toDate)::date
            group by 
                l.ledger_id, l.status, l.updated_at::date
            order by 
                l.updated_at::date desc
            ;
            """

        invoices_with_partial_funding_query = """ 
        select 
            l.ledger_id as ledgerId,
            ( select coalesce( sum(i.invoice_amt), 0)::text 
                  from 
                    ledger l2 
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id 
                  inner join invoice i on i.id = ila.invoice_id 
                  where l2.ledger_id = l.ledger_id
                ) as ledgerAmount,
            l.status as ledgerStatus,
            i.invoice_no as invoiceNo,
            to_char(i.invoice_date::date, 'DD/MM/YYYY') as invoiceDate,
            cast(i.invoice_amt AS VARCHAR) as invoiceAmount,
            to_char(i.invoice_due_date, 'DD/MM/YYYY') as invoiceDueDate,
            coalesce(i.extra_data->'ledgerData'->>'fundingAmtFlag', '') as fundingType,
            i.funded_amt as fundedAmount,
            coalesce(i.extra_data->'ledgerData'->>'financeRequestDate', '') as dateOfFunding,
            '-'::text as initiatedBy,
            md.name as authBy
        from 
            invoice i
        inner join invoice_ledger_association ila on i.id = ila.invoice_id 
        inner join ledger l on l.id = ila.ledger_id
        inner join merchant_details md on md.id = l.merchant_id
        where 
            md.unique_id = ANY(:idpId) and
            i.status in ('partial_funded', 'partial_disbursed', 'partial_paid') and 
            i.created_at::date between (:fromDate)::date and (:toDate)::date
        order by i.created_at::date desc
        ;
        """

        invoices_repaid_query = """ 
            select 
                l.ledger_id as ledgerId,
                ( select coalesce( sum(i.invoice_amt), 0)::text 
                  from 
                    ledger l2 
                  inner join invoice_ledger_association ila on ila.ledger_id = l2.id 
                  inner join invoice i on i.id = ila.invoice_id 
                  where l2.ledger_id = l.ledger_id
                ) as ledgerAmount,
                l.status as ledgerStatus,
                i.invoice_no as invoiceNo,
                to_char(i.invoice_date::date, 'DD/MM/YYYY') as invoiceDate,
                cast(i.invoice_amt AS VARCHAR) as invoiceAmount,
                to_char(i.invoice_due_date, 'DD/MM/YYYY') as invoiceDueDate,
                coalesce(i.extra_data->'ledgerData'->>'fundingAmtFlag', '') as fundingType,
                i.funded_amt as fundedAmount,
                coalesce(i.extra_data->'ledgerData'->>'financeRequestDate', '') as dateOfFunding,
                coalesce(i.extra_data->>'disbursedAmt', '') as disbursedAmount,
                coalesce(i.extra_data->>'disbursedDate', '') as disbDate,
                coalesce( ( select rh.extra_data->>'repaymentAmt' from repayment_history rh where rh.invoice_id =i.id order by rh.id desc limit 1), '0') as repaymentAmount,
                coalesce( ( select rh.extra_data->>'repaymentDate' from repayment_history rh where rh.invoice_id =i.id order by rh.id desc limit 1), '0') as repaymentDate,
                coalesce( ( select rh.extra_data->>'dueAmt' from repayment_history rh where rh.invoice_id =i.id order by rh.id desc limit 1), '0') as outstandingAmount,
                md.name as initiatedBy,
                md.name as authBy
            from 
                invoice i
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where 
                md.unique_id = ANY(:idpId) and
                i.status in ('repaid', 'partial_repaid') and 
                i.created_at::date between (:fromDate)::date and (:toDate)::date
            order by i.created_at::date desc
            ;
            """

        funding_requests_rejected_for_reason_already_funded_query = """ 
            select 
                data.*
            from
            (
            select 
                arl.request_id as reqId,
                to_date(to_char(i.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as requestDate,
                i.invoice_no as invoiceNo,
                cast(i.invoice_amt AS VARCHAR) as invoiceAmount,
                coalesce(i.extra_data->>'buyer_gst', '') as buyerGST,
                coalesce( (select eil2.entity_id_name from entity e2 inner join entity_identifier_line eil2 on eil2.entity_id = e2.id where (e2.id)::text = (i.extra_data->>'buyer_entity_id') and eil2.entity_id_name is not null and eil2.entity_id_name != '' limit 1), '') as buyerName,  
                coalesce(i.extra_data->>'seller_gst', '') as sellerGST,
                coalesce( (select eil3.entity_id_name from entity e3 inner join entity_identifier_line eil3 on eil3.entity_id = e3.id where (e3.id)::text = (i.extra_data->>'seller_entity_id') and eil3.entity_id_name is not null and eil3.entity_id_name != '' limit 1), '') as sellerName,
                to_date(to_char( i.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as registeredByUsOn
            from
                api_request_log arl
            cross join lateral jsonb_array_elements(arl.request_data->'ledgerData') AS requested_ledger_data
            inner join invoice i on i.invoice_no::text = requested_ledger_data->>'invoiceNo'::text
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where 
                arl.api_url in ( 'syncFinancing', 'asyncFinancing') 
                and md.unique_id = ANY(:idpId) 
                and i.created_at::date between (:fromDate)::date and (:toDate)::date
                and arl.response_data->>'code'::text in ('1004')
                
            union all
            
            select 
                ppr.request_extra_data->>'request_id' as reqId,
                to_date(to_char(i.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as requestDate,
                i.invoice_no as invoiceNo,
                cast(i.invoice_amt AS VARCHAR) as invoiceAmount,
                coalesce(i.extra_data->>'buyer_gst', '') as buyerGST,
                coalesce( (select eil2.entity_id_name from entity e2 inner join entity_identifier_line eil2 on eil2.entity_id = e2.id where (e2.id)::text = (i.extra_data->>'buyer_entity_id') and eil2.entity_id_name is not null and eil2.entity_id_name != '' limit 1), '') as buyerName,  
                coalesce(i.extra_data->>'seller_gst', '') as sellerGST,
                coalesce( (select eil3.entity_id_name from entity e3 inner join entity_identifier_line eil3 on eil3.entity_id = e3.id where (e3.id)::text = (i.extra_data->>'seller_entity_id') and eil3.entity_id_name is not null and eil3.entity_id_name != '' limit 1), '') as sellerName,
                to_date(to_char( i.created_at, 'DD/MM/YYYY'), 'DD/MM/YYYY') as registeredByUsOn
            from
                post_processing_request ppr
            cross join lateral jsonb_array_elements(ppr.request_extra_data->'ledgerData') AS requested_ledger_data
            inner join invoice i on i.invoice_no::text = requested_ledger_data->>'invoiceNo'::text
            inner join invoice_ledger_association ila on i.id = ila.invoice_id 
            inner join ledger l on l.id = ila.ledger_id
            inner join merchant_details md on md.id = l.merchant_id
            where 
                ppr.type in ('asyncFinancing') 
                and md.unique_id = ANY(:idpId)
                and i.created_at::date between (:fromDate)::date and (:toDate)::date
                and ppr.webhook_response->>'code'::text in ('1004')
            ) as data
            order by 
                data.requestDate desc
            ;
            """

        query_mapper = {
            "invoices_registered": invoices_registered_query,
            "ledger_funded": ledger_funded_query,
            "invoices_funded_with_invoice_details": invoices_funded_with_invoice_details_query,
            "invoices_cancelled": invoices_cancelled_query,
            "invoices_disbursed": invoices_disbursed_query,
            "ledger_with_partial_funding": ledger_with_partial_funding_query,
            "invoices_with_partial_funding": invoices_with_partial_funding_query,
            "invoices_repaid": invoices_repaid_query,
            "funding_requests_rejected_for_reason_already_funded": funding_requests_rejected_for_reason_already_funded_query
        }

        report_sub_type = request_data.get('reportSubType', '')
        query = query_mapper.get(report_sub_type)
        idp_id = request_data.get('idpId', '')
        from_date = request_data.get("fromDate")
        to_date = request_data.get("toDate")
        if from_date != "" and to_date != "":
            from_date = dt.strptime(from_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            to_date = dt.strptime(to_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            result = db.execute(text(query), [{'idpId': idp_id, 'fromDate': from_date, 'toDate': to_date}])
        else:
            result = db.execute(text(query))
        logger.info(f"result {result}")

        columns = result.keys()
        data = [dict(zip(columns, row)) for row in result.fetchall()]

        return data


@router.post("/get-user-mis-report/")
def get_user_mis_report(request: GetUserMisReportSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    report_type_list = ["entity_registration_data", "invoice_data", "api_wises_success_failure_summary"]

    try:
        response_data = {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(200)
        }
        idp_id = request_data.get('idpId', '')
        # merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == idp_id).first()
        # merchant_key = merchant_details and merchant_details.merchant_key or None

        # api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'get-mis-report', merchant_key)
        # if api_request_log_res.get("code") != 200:
        #     return {
        #         "requestId": request_data.get('requestId'),
        #         **api_request_log_res
        #     }

        report_type = request_data.get('reportType', '')
        if report_type not in report_type_list:
            return {
                "requestId": request_data.get('requestId'),
                **ErrorCodes.get_error_response(1132)
            }

        data = []
        if report_type == 'entity_registration_data':
            report_sub_type_list = ["entity_registered", "gstin_wise_entity_registered",
                                    "entity_id_with_identifiers", "entity_ids_having_multiple_gstin",
                                    "entity_ids_without_gstin", "entity_ids_without_pan"]

            if request_data.get('reportSubType', '') not in report_sub_type_list:
                return {
                    "requestId": request_data.get('requestId'),
                    **ErrorCodes.get_error_response(1132)
                }
            data = GetUserMisReport.entity_registered_data(request_data, db)
            # if report_sub_type_list == 'gstin_wise_entity_registered':
            #     pass
            # if report_sub_type_list == 'entity_id_with_identifiers':
            #     pass
            # if report_sub_type_list == 'entity_ids_having_multiple_gstin':
            #     pass
            # if report_sub_type_list == 'entity_ids_without_gstin':
            #     pass
            # if report_sub_type_list == 'entity_ids_without_pan':
            #     pass
        elif report_type == 'invoice_data':
            report_sub_type_list = ["invoices_registered", "ledger_funded", "invoices_funded_with_invoice_details",
                                    "invoices_cancelled", "invoices_disbursed", "ledger_with_partial_funding",
                                    "invoices_with_partial_funding", "invoices_repaid",
                                    "funding_requests_rejected_for_reason_already_funded"]

            if request_data.get('reportSubType', '') not in report_sub_type_list:
                return {
                    "requestId": request_data.get('requestId'),
                    **ErrorCodes.get_error_response(1132)
                }
            data = GetUserMisReport.invoice_data(request_data, db)
        else:
            return {
                'code': 200,
                'message': 'invalid reportType'
            }

        response_data.update({'data': data})
        return response_data
    except Exception as e:
        logger.exception(f"Exception get_user_mis_report :: {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }


@router.post("/download-user-mis-report/")
def get_user_mis_report(request: GetUserMisReportSchema, db: Session = Depends(get_db)):
    request_data = jsonable_encoder(request)
    report_type_list = ["entity_registration_data", "invoice_data", "api_wises_success_failure_summary"]

    try:
        response_data = {
            'requestId': request_data.get('requestId'),
            **ErrorCodes.get_error_response(200)
        }
        idp_id = request_data.get('idpId', '')
        # merchant_details = db.query(MerchantDetails).filter(MerchantDetails.unique_id == idp_id).first()
        # merchant_key = merchant_details and merchant_details.merchant_key or None

        # api_request_log_res = views.create_request_log(db, request_data.get('requestId'), request_data, '', 'request', 'get-mis-report', merchant_key)
        # if api_request_log_res.get("code") != 200:
        #     return {
        #         "requestId": request_data.get('requestId'),
        #         **api_request_log_res
        #     }

        report_type = request_data.get('reportType', '')
        report_sub_type = request_data.get('reportSubType', '')
        if report_type not in report_type_list:
            return {
                "requestId": request_data.get('requestId'),
                **ErrorCodes.get_error_response(1132)
            }

        data = []
        if report_type == 'entity_registration_data':
            report_sub_type_list = ["entity_registered", "gstin_wise_entity_registered",
                                    "entity_id_with_identifiers", "entity_ids_having_multiple_gstin",
                                    "entity_ids_without_gstin", "entity_ids_without_pan"]

            if report_sub_type not in report_sub_type_list:
                return {
                    "requestId": request_data.get('requestId'),
                    **ErrorCodes.get_error_response(1132)
                }
            data = GetUserMisReport.entity_registered_data(request_data, db)
        elif report_type == 'invoice_data':
            report_sub_type_list = ["invoices_registered", "ledger_funded", "invoices_funded_with_invoice_details",
                                    "invoices_cancelled", "invoices_disbursed", "ledger_with_partial_funding",
                                    "invoices_with_partial_funding", "invoices_repaid",
                                    "funding_requests_rejected_for_reason_already_funded"]

            if report_sub_type not in report_sub_type_list:
                return {
                    "requestId": request_data.get('requestId'),
                    **ErrorCodes.get_error_response(1132)
                }
            data = GetUserMisReport.invoice_data(request_data, db)
        else:
            response_data = {
                "requestId": request_data.get('requestId'),
                **ErrorCodes.get_error_response(1132)
            }
            response_data.update({'data': data})
            return response_data

        # ### Write DataFrame to an Excel file
        df = pd.DataFrame(data)
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)
        curr_datetime = dt.now(asia_kolkata).strftime("%d_%b_%Y_%H:%M")
        from_date = dt.strptime(request_data.get("fromDate"), "%d/%m/%Y").strftime("%d_%b_%Y")
        to_date = dt.strptime(request_data.get("toDate"), "%d/%m/%Y").strftime("%d_%b_%Y")
        # 29_May_2024_11:50_InvoiceData_invoice registred_10_May_2024to27_May_2024
        file_name = curr_datetime + '_' + report_sub_type + '_' + from_date + 'to' + to_date
        # Return the Excel file as a response
        headers = {'Content-Disposition': f'attachment; filename="{file_name}.xlsx"'}
        return Response(
            content=output.getvalue(),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers=headers)

    except Exception as e:
        logger.exception(f"Exception get_mis_report :: {e}")
        return {
            **ErrorCodes.get_error_response(500)
        }

