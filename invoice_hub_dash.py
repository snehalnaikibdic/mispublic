import logging
import time
import redis
import ast
import json
from fastapi import HTTPException
from typing import Annotated
from fastapi import APIRouter, Depends, Request
from decouple import config as dconfig
from fastapi.encoders import jsonable_encoder

import models
from aes_encryption_decryption import AESCipher
from cygnet_api import CygnetApi
from database import SessionLocal
from sqlalchemy.orm import Session
import utils
from gspi_api import get_token, consent_verify_ewb_credentials
from models import MerchantDetails
from routers.auth import get_current_merchant_active_user, User
from schema import InvoiceRequestSchema, FinanceSchema, CancelLedgerSchema, CheckStatusSchema, CheckEnquirySchema, AsyncInvoiceRegistrationWithCodeSchema, AsyncValidationServiceWithCodeSchema, AsyncValidationServiceWithoutCodeSchema, GSPUserDetailSchema
import views
from errors import ErrorCodes
from utils import get_financial_year, check_invoice_date, check_invoice_due_date
from utils import generate_key_secret, create_post_processing, validate_signature
from config import webhook_task, create_auth_org
from enquiry_view import AsyncValidationServiceWithCode, AsyncValidationServiceWithoutCode
from database import get_db
from sqlalchemy import desc, text
from models import MerchantDetails, APIRequestLog
from schema import GetGspUserListSchema, GSPUserDeleteSchema, IdpListSchema, GSPUserCreateSchema, GenerateOtpSchema, CorporateLoginSchema
from routers.send_mail import CORPORATE_GENERATE_OTP_BODY_TEXT
from config import async_sftp_send_email
from utils import GenerateToken
from utils import OTPGenerateVerify

logger = logging.getLogger(__name__)

router = APIRouter()
r = redis.Redis(host=dconfig('REDIS_HOST'), port=6379, decode_responses=True)


class TagsMetadata:
    gsp_user_info= {
        "name": "GSP User Info",
        "description": "**Aim** : Registration of new gsp user consent \n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- GSTIN under PAN number validation \n"
                       "- new consent GSTIN, PAN number,Username, password should be unique.\n"
                       "- Corporate name should be unique\n"
                       "\n **Table** : GSPUserDetails, GSPAPIRequestLog\n"
                       "\n **Function** : decrypt_key_using_private_key, aes_cbc_256_bit_decrypt, validate_gst_pan, create_consent_auth_token \n",
                        "vayana_encryption, consent_verify_ewb_credentials, cygnet_create_customer, cygnet_consent_verify_ewb_credentials, \n"
                        "gsp_password_encryption, gsp_user_name_phone_no\n"
        "summary": "Summary of GSP User"
    }
    consent_user_info= {
        "name": "new consent user",
        "description": "**Aim** : Registration of new user consent.\n"
                       "\n**Validation** : \n"
                       "- Date format validation\n"
                       "- GSTIN under PAN number validation n"
                       "- new consent GSTIN, PAN number,Username, password should be unique.\n"
                       "- Corporate name should be uniques\n"
                       "\n **Table** : GSPUserDetails, GSPAPIRequestLog\n"
                       "\n **Function** : decrypt_key_using_private_key, aes_cbc_256_bit_decrypt, validate_gst_pan, create_consent_auth_token \n",
                        "vayana_encryption, consent_verify_ewb_credentials, cygnet_create_customer, cygnet_consent_verify_ewb_credentials, \n"
                        "gsp_password_encryption, gsp_user_name_phone_no\n"
        "summary": "Summary of consent User"
    }

# class TagsMetadata:
#     async_validation_service_with_code = {
#         "name": "async validation service with entity code",
#         "description": "**Aim** : Before establishing the invoice registration service with the entity code called,"
#                        "this API will be made available to verify whether the format of all requested data is correct"
#                        "or not in the validation service with the entity code format via webhook.\n"
#                        "\n**Validation** : \n"
#                        "- GST format validation\n"
#                        "- Date format validation\n"
#                        "- IDP ID and invoice data combo uniqueness validation\n"
#                        "- Invoice date should not be greater than current date\n"
#                        "\n **Table** : APIRequestLog, PostProcessingRequest, Invoice, Ledger, MerchantDetails, EntityIdentifierLine, EntityCombination\n"
#                       "\n **Function** :validate_signature, create_request_log, create_post_processing, \n"
#                       "validation_service_with_code, check_entity_code_gst_validation, webhook_task",
#         "summary": "Summary of asyncValidationServiceWithCode"
#     }


@router.post("/GSPUser/", description=TagsMetadata.gsp_user_info.get('description'))
async def gsp_user(request: Request, db: Session = Depends(get_db)):
    try:
        header_dict = dict(request.headers)
        logger.info(f"{header_dict}")
        aes_key = header_dict.get('session-key', '')
        aes_iv = header_dict.get('session-iv', '')
        data = await request.json()
        encrypted_request_data = data.get('data')
        aes_encryption = AESCipher()
        raw_aes_key = aes_encryption.decrypt_key_using_private_key(aes_key)
        logger.info(f"raw aes key ::  {raw_aes_key}")
        request_data = aes_encryption.aes_cbc_256_bit_decrypt(raw_aes_key, aes_iv, encrypted_request_data)
        request_data = json.loads(request_data)
        request_data.update({"requestId": data.get("requestId")})
        logger.info(f"Request after decryption  :: {type(request_data)}")
        logger.info(f"{request_data}")
        try:
            reg_pay_load = GSPUserCreateSchema(**request_data)
            request_data = jsonable_encoder(reg_pay_load)
        except HTTPException as httpExc:
            logger.info(f"httpExc:: {httpExc.detail}")
            return {"code": 400, "message": str(httpExc.detail)}
        except Exception as e:
            logger.exception(f"getting error {e}")
            return {"code": 500, "message": str(e)}

        derived_pan_response = utils.validate_gst_pan(request_data)
        logger.info(f" ################# getting derived pan response {derived_pan_response} #################")
        if derived_pan_response:
             return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1146)}

        extra_data = {
            "requestId": request_data.get('requestId'),
            "ref": "ibdic_login"
        }
        gsp_user_req = db.query(
            models.GSPUserDetails
        ).filter(
            models.GSPUserDetails.gstin == request_data.get('gstin'),
            models.GSPUserDetails.gsp == request_data.get('gsp'),
            models.GSPUserDetails.username == request_data.get('username'),
            models.GSPUserDetails.password == request_data.get('password')
        ).first()

        corporate_user_obj = db.query(
            models.CorporateUserDetails
        ).filter(
            models.CorporateUserDetails.email_id == request_data.get('email')
        ).first()

        # logger.info(f"getting gst user password {gsp_user_req.password} and {request_data.get('password')}")
        if request_data.get('gsp').lower() == 'vayana':
            logger.info(f" >>>>>>>>>>>>>>>>>>>> getting inside vayana consent api >>>>>>>>>>>>>> ")
            cache_org_id_key = f"org_id@{request_data.get('username')}"
            cache_token_key = f"token@{request_data.get('username')}"
            auth_token = r.get(cache_token_key)

            if not auth_token:
                try:
                    gsp_response, response_data = create_consent_auth_token(request_data)
                    if gsp_response.status_code != 200:
                        return response_data
                except Exception as e:
                    logger.info(f"getting error {e}")

            auth_token = r.get(cache_token_key)
            org_id = r.get(cache_org_id_key)
            request_data['encrypted_rek'] = ""
            request_data['encrypted_password'] = ""
            if not gsp_user_req:
                aes_response = aes_encryption.vayana_encryption(request_data.get('password'))
                request_data['encrypted_rek'] = aes_response.get('X-FLYNN-S-REK')
                request_data['encrypted_password'] = aes_response.get('X-FLYNN-S-DATA')

                consent_api_response = consent_verify_ewb_credentials(
                    request_data,
                    'vay',
                    org_id,
                    auth_token
                )
                if consent_api_response.status_code != 200:
                    return {**ErrorCodes.get_error_response(1071)}
        else:
            f_name = request_data.get('name')[0:9]
            l_name = request_data.get('name')[9:18]
            cygnet_api = CygnetApi()
            create_customer_request_data = {
                "data": {
                    
                    "f_name": f_name,
                    "l_name": l_name,
                    "lgnm": request_data.get('name'),
                    "email": request_data.get('emailId'),
                    "mo_num": request_data.get('mobileNumber'),
                    "pan": request_data.get('pan'),
                    "ct": "1"
                }
            }

            create_cust_response = cygnet_api.cygnet_create_customer(create_customer_request_data)
            logger.info(f" >>>>>>>> getting create_cust_response response {create_cust_response} >>>>>>>>> ")

            consent_request_data = {
                "data": {
                    "username": request_data.get('username'),
                    "password": request_data.get('password'),
                    "gstin": request_data.get('gstin')
                }
            }
            consent_response = cygnet_api.cygnet_consent_verify_ewb_credentials(consent_request_data)

            logger.info(f" ################ getting consent response {consent_response} #####################")
            if not consent_response.get('ResponseData'):
                return consent_response

            extra_data.update({
                "cygnet_ewaybill_token": consent_response.get('ResponseData').get('authToken'),
                "cygnet_ewaybill_sek": consent_response.get('ResponseData').get('sek')
            })

        if not gsp_user_req:
            secret_key = request_data.get('gstin') + request_data.get('mobileNumber')
            encrypted_pass = aes_encryption.gsp_password_encryption(request_data.get('password'), secret_key)
        else:
            encrypted_pass = gsp_user_req.password

        if gsp_user_req:
            # duplicate check corporate name and number
            gsp_user_duplicate_response = utils.gsp_user_name_phone_no(
                db,
                request_data
            )
            if gsp_user_duplicate_response.get("code") != 200:
                return {
                    "requestId": request_data.get('requestId'),
                    **gsp_user_duplicate_response
                }
            gsp_user_req.gsp = request_data.get('gsp')
            gsp_user_req.username = request_data.get('username')
            gsp_user_req.password = encrypted_pass
            gsp_user_req.name = request_data.get('name')
            gsp_user_req.pan = request_data.get('pan')
            gsp_user_req.email = request_data.get('emailId')
            gsp_user_req.mobile_number = request_data.get('mobileNumber')
            gsp_user_req.extra_data = extra_data
            gsp_user_req.created_by_id = corporate_user_obj.id
            db.commit()
            db.refresh(gsp_user_req)
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1136)}
        else:
            # duplicate check corporate name and number
            gsp_user_duplicate_response = utils.gsp_user_name_phone_no(
                db,
                request_data
            )
            if gsp_user_duplicate_response.get("code") != 200:
                return {
                    "requestId": request_data.get('requestId'),
                    **gsp_user_duplicate_response
                }
            # duplicate gstin found while create gsp user
            gsp_gstin = db.query(
                models.GSPUserDetails
            ).filter(
                models.GSPUserDetails.gstin == request_data.get('gstin')
            ).first()

            if gsp_gstin and str(gsp_gstin.gstin) == request_data.get('gstin'):
                response_data = {
                    "requestId": request_data.get('requestId'),
                    **ErrorCodes.get_error_response(1149)
                }
                return response_data
            logger.info(f"duplcate gstin number not found")

            gsp_user_req_created = models.GSPUserDetails(
                gstin=request_data.get('gstin'),
                gsp=request_data.get('gsp'),
                username=request_data.get('username'),
                password=encrypted_pass,
                name=request_data.get('name'),
                pan=request_data.get('pan'),
                email=request_data.get('emailId'),
                mobile_number=request_data.get('mobileNumber'),
                extra_data=extra_data,
                created_by_id=corporate_user_obj.id if corporate_user_obj else None
            )
            db.add(gsp_user_req_created)
            db.commit()
            db.refresh(gsp_user_req_created)
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(200)}
    except Exception as e:
        logger.error(f"getting error while creating GSP USER CREATE log >>>>>>>>>>>>>>> {e}")
        return {**ErrorCodes.get_error_response(500)}


def create_consent_auth_token(gsp_user_details):
    logger.info(f" >>>>>>>>>>>>>>>>>>>> getting inside create_consent_auth_token >>>>>>>>>>>>>> ")
    # ALL REQUEST PACKET WILL COME FROM TABLE NEED TO INTEGRATE HERE
    cache_token_key = f"token@{gsp_user_details.get('username')}"
    cache_org_id_key = f"org_id@{gsp_user_details.get('username')}"
    request_data = {
        "handle": dconfig('VAYANA_HANDLE', ''),
        "password": dconfig('VAYANA_PASSWORD', ''),
        "handleType": "email",
        "tokenDurationInMins": 360
    }
    gsp_response, response_data = get_token(request_data, 'vayana')
    if gsp_response.status_code != 200:
        return gsp_response

    auth_token = str(response_data.get('data').get('token'))
    org_id = str(response_data.get('data').get('associatedOrgs')[0].get('organisation').get('id'))
    r.set(cache_token_key, auth_token)
    r.set(cache_org_id_key, org_id)
    r.expire(cache_token_key, 21600)
    r.expire(cache_org_id_key, 21600)
    logger.info(f"getting gst_response {gsp_response} and response data {response_data}")
    gsp_response, response_data


@router.post("/getGspUserList/")
def get_gsp_user_list(request: GetGspUserListSchema,
                      db: Session = Depends(get_db)):

    try:
        gsp_user_ids = []
        gsp_user_obj = db.query(
            models.GSPUserDetails
        ).all()
        for user_data in gsp_user_obj:
            # logger.info(f"gsp user_data {user_data}")
            if user_data:
                history_obj = {
                    "requestId": user_data.extra_data.get('requestId'),
                    "gspUserData": user_data
                    # "gspUserData": {
                    #     "username": user_data.username,
                    #     "id": user_data.id,
                    #     "name": user_data.name,
                    #     "email": user_data.email,
                    #     "mobile_number": user_data.mobile_number,
                    #     "gstin": user_data.gstin,
                    #     "gsp": user_data.gsp,
                    #     "pan": user_data.pan,
                    #     "created_at": user_data.created_at,
                    #     "updated_at": user_data.updated_at,
                    #     "is_active": user_data.is_active
                    # }
                }
            else:
                history_obj = ""
            gsp_user_ids.append(history_obj)

        return {
            "requestId": request.requestId,
            **ErrorCodes.get_error_response(200),
            "gspUserList": gsp_user_ids
        }
    except Exception as e:
        logger.error(f"getting error while gst GSP user List {e}")
        return {**ErrorCodes.get_error_response(500)}


@router.post("/gspUserDelete/")
def gsp_user_delete(request: GSPUserDeleteSchema,
                      db: Session = Depends(get_db)):

    try:
        request = jsonable_encoder(request)
        if not request.get('gspUserId'):
            return {"requestId": request.get('requestId'), **ErrorCodes.get_error_response(1135)}
        gsp_user_del = db.query(models.GSPUserDetails).filter(
            models.GSPUserDetails.id == request.get('gspUserId')
        ).first()

        if gsp_user_del:
            db.delete(gsp_user_del)
            db.commit()
            return {"requestId": request.get('requestId'), **ErrorCodes.get_error_response(200)}
        else:
            return {'requestId': request.get('requestId'), **ErrorCodes.get_error_response(1134)}

    except Exception as e:
        logger.error(f"getting error while gst GSP user delete {e}")
        return {**ErrorCodes.get_error_response(500)}


@router.post("/gspUserdetail/")
def gsp_user_detail(request: GSPUserDetailSchema,
                      db: Session = Depends(get_db)):

    try:
        request_data = jsonable_encoder(request)
        if not request_data.get('gspUserId'):
            return {"requestId":  request.requestId, **ErrorCodes.get_error_response(1138)}

        gsp_user_obj = db.query(
            models.GSPUserDetails
        ).filter(
            models.GSPUserDetails.id == request_data.get('gspUserId')
        ).first()
        if gsp_user_obj:
            return {
                "requestId": request.requestId,
                **ErrorCodes.get_error_response(200),
                "gspUserDetail": gsp_user_obj
            }
        else:
            return {
                "requestId": request.requestId,
                **ErrorCodes.get_error_response(1137),
            }

    except Exception as e:
        logger.error(f"getting error while gst GSP user Detail {e}")
        return {**ErrorCodes.get_error_response(500)}


@router.post("/getIdpList/")
def idp_list(request: IdpListSchema, db: Session = Depends(get_db)):

    try:
        gsp_user_ids = []
        gsp_user_obj = db.query(
            models.MerchantDetails
        ).all()
        for user_data in gsp_user_obj:
            if user_data:
                idp_data = {
                    "idpId": user_data.id,
                    "idpCode": user_data.unique_id if user_data.unique_id else '',
                    "idpName": user_data.name
                }
            else:
                idp_data = ""
            gsp_user_ids.append(idp_data)

        return {
            "requestId": request.requestId,
            **ErrorCodes.get_error_response(200),
            "idpData": gsp_user_ids
        }
    except Exception as e:
        logger.error(f"getting error while IDP ID List {e}")
        return {**ErrorCodes.get_error_response(500)}


@router.post("/generateOtp/")
def generate_otp(request: GenerateOtpSchema):
    import random
    import string
    from utils import GeneratetransactionRef, OTPCache
    try:
        request_data = jsonable_encoder(request)

        otp = ''.join(random.choices(string.digits, k=6))  # Generate a 6-digit OTP
        logger.info(f"otp_storage >> {otp}")
        # return otp
        if request_data.get('otpType') == 'mobile_otp':
            reference_id = GeneratetransactionRef().get_transaction_ref(request_data.get('mobileNo'))
            logger.info(f"OTP Reference ID Mobile No: {reference_id} OTP: {otp}")
            otp_cache_obj = OTPCache(mobile_no=request_data.get('mobileNo'), reference_id=reference_id, otp_to_send=otp)
            otp_cache_obj.set()
            return_response = {
                'requestId': request_data.get('requestId'),
                'phoneNumber': request_data.get('mobileNo'),
                'referenceId': reference_id,
                **ErrorCodes.get_error_response(200)
            }
        else:
            reference_id = GeneratetransactionRef().get_transaction_ref(request_data.get('emailId'))
            logger.info(f"OTP Reference ID Email Id: {reference_id} OTP: {otp}")
            otp_cache_obj = OTPCache(mobile_no=request_data.get('emailId'), reference_id=reference_id, otp_to_send=otp)
            otp_cache_obj.set()
            return_response = {
                'requestId': request_data.get('requestId'),
                'emailId': request_data.get('emailId'),
                'referenceId': reference_id,
                **ErrorCodes.get_error_response(200)
            }
            # send otp to mail template
            if return_response.get('code') == 200:
                user_email = request_data.get('emailId') or ''
                subject = f"Corporate User Generate OTP"
                email_to = user_email
                body_data = {
                    "email": email_to,
                    "otp": otp
                }
                async_sftp_send_email.delay(subject, email_to, body_data, body_text=CORPORATE_GENERATE_OTP_BODY_TEXT)
            else:
                return {**ErrorCodes.get_error_response(500)}
        return return_response
    except Exception as e:
        logger.error(f"getting error while generate otp {e}")
        return {**ErrorCodes.get_error_response(500)}


@router.post("/corporateUserLogin/")
def corporate_user_login(request: CorporateLoginSchema, db: Session = Depends(get_db)):
    try:
        request_data = jsonable_encoder(request)
        cop_user_req = db.query(
            models.CorporateUserDetails
        ).filter(
            models.CorporateUserDetails.email_id == request_data.get('emailId')
        ).first()

        otp_verification_response = OTPGenerateVerify.verify_otp(
            request_data.get('emailId').strip(),
            request_data.get('referenceId').strip(),
            request_data.get('emailIdOtp').strip()
        )

        logger.info(f'otp_verification_response email>>> {otp_verification_response} ')
        if int(otp_verification_response.get('code')) != 200:
            return otp_verification_response

        if not cop_user_req:
            cop_user_created = models.CorporateUserDetails(
                email_id=request_data.get('emailId'),
                mobile_no=request_data.get('mobileNo'),
                pan_number=request_data.get('panNumber'),
                role=1
            )
            db.add(cop_user_created)
            db.commit()
            db.refresh(cop_user_created)
            # generate_token = GenerateToken.create_token(db, cop_user_created.id)
            role_obj = db.query(
                models.Role
            ).filter(
                models.Role.id == cop_user_created.role
            ).first()
            # logger.info(f"role >>>> {role_obj.role_name}")
            data = {
                "corporateUserId": cop_user_created.id or '',
                "emailId": cop_user_created.email_id or '',
                "mobileNo": cop_user_created.mobile_no or '',
                "panNumber": cop_user_created.pan_number,
                "role": role_obj.role_name
            }
            return {
                "requestId": request_data.get('requestId'),
                # "token": generate_token,
                "data": data,
                **ErrorCodes.get_error_response(200)
            }
        else:
            role_obj = db.query(
                models.Role
            ).filter(
                models.Role.id == cop_user_req.role
            ).first()
            data = {
                "corporateUserId": cop_user_req.id or '',
                "emailId": cop_user_req.email_id or '',
                "mobileNo": cop_user_req.mobile_no or '',
                "panNumber": cop_user_req.pan_number,
                "role": role_obj.role_name
            }
            return {"requestId": request_data.get('requestId'),
                    "data": data,
                    **ErrorCodes.get_error_response(200)}
            # return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1145)}
    except Exception as e:
        logger.error(f"getting error while creating Corporate User Login log >>>>>>>>>>>>>>> {e}")
        return {**ErrorCodes.get_error_response(500)}


# def check_merchant_key(func, db):
#     # @wraps(func)
#     async def wrapper(merchant_key: str, *args, **kwargs):
#         # Check if the merchant key exists in the database
#         # merchant = db.query(Merchant).filter(Merchant.key == merchant_key).first()
#         if not merchant:
#             # raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Merchant key not found")
#         # If merchant key exists, execute the original function
#         return await func(merchant_key, *args, **kwargs)
#     return wrapper


@router.post("/ConsentUser/{merchant_key}", description=TagsMetadata.consent_user_info.get('description'))
async def consent_user(merchant_key: str, request: Request, db: Session = Depends(get_db)):
    try:
        header_dict = dict(request.headers)
        logger.info(f"{header_dict}")
        aes_key = header_dict.get('session-key', '')
        aes_iv = header_dict.get('session-iv', '')
        data = await request.json()
        encrypted_request_data = data.get('data')
        aes_encryption = AESCipher()
        raw_aes_key = aes_encryption.decrypt_key_using_private_key(aes_key)
        logger.info(f"raw aes key ::  {raw_aes_key}")
        request_data = aes_encryption.aes_cbc_256_bit_decrypt(raw_aes_key, aes_iv, encrypted_request_data)
        request_data = json.loads(request_data)
        request_data.update({"requestId": data.get("requestId")})
        logger.info(f"Request after decryption  :: {type(request_data)}")
        logger.info(f"{request_data}")

        try:
            reg_pay_load = GSPUserCreateSchema(**request_data)
            request_data = jsonable_encoder(reg_pay_load)
        except Exception as e:
            logger.info(f"getting error {e}")
            return {"code": 500}

        derived_pan_response = utils.validate_gst_pan(request_data)
        logger.info(f" ################# getting derived pan response {derived_pan_response} #################")
        if derived_pan_response:
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1146)}

        merchant_details = db.query(MerchantDetails).filter(MerchantDetails.merchant_key == merchant_key).first()
        if merchant_details:
            if merchant_details.unique_id != request_data.get('idpId'):
                return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1150)}

            extra_data = {
                "requestId": request_data.get('requestId'),
                "ref": "bank"
            }

            gsp_user_req = db.query(
                models.GSPUserDetails
            ).filter(
                models.GSPUserDetails.gstin == request_data.get('gstin'),
                models.GSPUserDetails.gsp == request_data.get('gsp'),
                models.GSPUserDetails.username == request_data.get('username'),
                models.GSPUserDetails.password == request_data.get('password')
            ).first()

            # logger.info(f"getting gst user password {gsp_user_req.password} and {request_data.get('password')}")
            if request_data.get('gsp').lower() == 'vayana':
                logger.info(f" >>>>>>>>>>>>>>>>>>>> getting inside vayana consent api >>>>>>>>>>>>>> ")
                cache_org_id_key = f"org_id@{request_data.get('username')}"
                cache_token_key = f"token@{request_data.get('username')}"
                auth_token = r.get(cache_token_key)

                if not auth_token:
                    try:
                        gsp_response, response_data = create_consent_auth_token(request_data)
                        if gsp_response.status_code != 200:
                            return response_data
                    except Exception as e:
                        logger.info(f"getting error {e}")

                auth_token = r.get(cache_token_key)
                org_id = r.get(cache_org_id_key)
                request_data['encrypted_rek'] = ""
                request_data['encrypted_password'] = ""
                if not gsp_user_req:
                    aes_response = aes_encryption.vayana_encryption(request_data.get('password'))
                    request_data['encrypted_rek'] = aes_response.get('X-FLYNN-S-REK')
                    request_data['encrypted_password'] = aes_response.get('X-FLYNN-S-DATA')

                    consent_api_response = consent_verify_ewb_credentials(
                        request_data,
                        'vay',
                        org_id,
                        auth_token
                    )
                    if consent_api_response.status_code != 200:
                        return {**ErrorCodes.get_error_response(1071)}
            else:
                f_name = request_data.get('name')[0:9]
                l_name = request_data.get('name')[9:18]
                cygnet_api = CygnetApi()
                create_customer_request_data = {
                    "data": {
                        "f_name": f_name,
                        "l_name": l_name,
                        "lgnm": request_data.get('name'),
                        "email": request_data.get('emailId'),
                        "mo_num": request_data.get('mobileNumber'),
                        "pan": request_data.get('pan'),
                        "ct": "1"
                    }
                }

                create_cust_response = cygnet_api.cygnet_create_customer(create_customer_request_data)
                logger.info(f" >>>>>>>> getting create_cust_response response {create_cust_response} >>>>>>>>> ")
                consent_request_data = {
                    "data": {
                        "username": request_data.get('username'),
                        "password": request_data.get('password'),
                        "gstin": request_data.get('gstin')
                    }
                }

                cygnet_api = CygnetApi()
                consent_response = cygnet_api.cygnet_consent_verify_ewb_credentials(consent_request_data)
                if not consent_response.get('ResponseData'):
                    return consent_response

                extra_data.update({
                    "cygnet_ewaybill_token": consent_response.get('ResponseData').get('authToken'),
                    "cygnet_ewaybill_sek": consent_response.get('ResponseData').get('sek')
                })

            if not gsp_user_req:
                secret_key = request_data.get('gstin') + request_data.get('mobileNumber')
                encrypted_pass = aes_encryption.gsp_password_encryption(request_data.get('password'), secret_key)
            else:
                encrypted_pass = gsp_user_req.password

            if gsp_user_req:
                # duplicate check corporate name and number
                gsp_user_duplicate_response = utils.gsp_user_name_phone_no(
                    db,
                    request_data
                )
                if gsp_user_duplicate_response.get("code") != 200:
                    return {
                        "requestId": request_data.get('requestId'),
                        **gsp_user_duplicate_response
                    }
                gsp_user_req.gsp = request_data.get('gsp')
                gsp_user_req.username = request_data.get('username')
                gsp_user_req.password = encrypted_pass
                gsp_user_req.name = request_data.get('name')
                gsp_user_req.pan = request_data.get('pan')
                gsp_user_req.email = request_data.get('emailId')
                gsp_user_req.mobile_number = request_data.get('mobileNumber')
                gsp_user_req.extra_data = extra_data
                db.commit()
                db.refresh(gsp_user_req)
                return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1136)}
            else:
                # duplicate check corporate name and number
                gsp_user_duplicate_response = utils.gsp_user_name_phone_no(
                    db,
                    request_data
                )
                if gsp_user_duplicate_response.get("code") != 200:
                    return {
                        "requestId": request_data.get('requestId'),
                        **gsp_user_duplicate_response
                    }
                # duplicate gstin found while create gsp user
                gsp_gstin = db.query(
                    models.GSPUserDetails
                ).filter(
                    models.GSPUserDetails.gstin == request_data.get('gstin')
                ).first()

                if gsp_gstin and str(gsp_gstin.gstin) == request_data.get('gstin'):
                    response_data = {
                        "requestId": request_data.get('requestId'),
                        **ErrorCodes.get_error_response(1149)
                    }
                    return response_data
                logger.info(f"duplcate gstin number not found")

                gsp_user_req_created = models.GSPUserDetails(
                    gstin=request_data.get('gstin'),
                    gsp=request_data.get('gsp'),
                    username=request_data.get('username'),
                    password=encrypted_pass,
                    name=request_data.get('name'),
                    pan=request_data.get('pan'),
                    email=request_data.get('emailId'),
                    mobile_number=request_data.get('mobileNumber'),
                    extra_data=extra_data
                )
                db.add(gsp_user_req_created)
                db.commit()
                db.refresh(gsp_user_req_created)
                return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(200)}

        else:
            return {"requestId": request_data.get('requestId'), **ErrorCodes.get_error_response(1002)}

    except Exception as e:
        logger.info(f"getting error {e}")





