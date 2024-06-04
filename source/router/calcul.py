import pandas as pd
import requests
from io import StringIO
from fastapi import Request
from fastapi import APIRouter
from pydantic import BaseModel
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.staticfiles import StaticFiles
from utils.process_open_banking_mono import get_auth, get_statement, open_banking_mono_calcul_score, open_banking_mono_from_to, open_banking_mono_transaction_labelisation
from utils.utils import OPEN_BANKING_MONO_COLUMNS_MAPPING, load_config, calcul_score, read_data, get_files, main_process, read_json
from utils.process_access_bank import *
from utils.process_first_bank import * 
from utils.process_sterling_bank import *
from utils.process_wema_bank import * 
from utils.process_union_bank import * 
from utils.process_fcmb_bank import * 
from utils.process_zenith_bank import *
from utils.process_eco_bank import * 
from utils.standard_func import *
from utils.utils import ACCESS_BANK_COLUMNS_MAPPING, FIRST_BANK_COLUMNS_MAPPING,STERLING_BANK_COLUMNS_MAPPING
from utils.utils import WEMA_BANK_COLUMNS_MAPPING, UNION_BANK_COLUMNS_MAPPING, FCMB_BANK_COLUMNS_MAPPING, ZENITH_BANK_COLUMNS_MAPPING 
from utils.utils import ECO_BANK_COLUMNS_MAPPING 


# path file config
CONFIG_PATH = "config/config.json"
# load config
_, CONFIG_MAPPING, CONFIG_SECURITY = load_config(CONFIG_PATH)


router = APIRouter()
security = HTTPBasic()


class FileUrl(BaseModel):
    file_url: str


# Fonction pour verifier les informations d'authentification
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = CONFIG_SECURITY["LOGIN_ENDPT"]
    correct_password = CONFIG_SECURITY["PWD_ENDPT"]
    if credentials.username == correct_username and credentials.password == correct_password:
        return credentials.username
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Mauvaises informations d'identification",
        headers={"WWW-Authenticate": "Basic"},
    )


@router.post("/calcul")
async def calcul(request: Request, username: str = Depends(verify_credentials)):#, username: str = Depends(verify_credentials)
    try:
        input_json = await request.json()
        return main_process(input_json)
    except Exception as ex:
        print(f"[ERROR][calcul.calcul.RequestOrHost] {ex}")
        return add_status_output(calcul_score(pd.DataFrame()), "errors", ["impossible de faire l'analyse"])
    
@router.post("/calcul_open_banking")  
async def calcul_open_banking(code : str, username: str = Depends(verify_credentials)):
    id_account = get_auth(code)
    input_json = get_statement(id_account)
    # print(input_json)
    data = read_json(input_json)
    data = standardize_column_name(data,OPEN_BANKING_MONO_COLUMNS_MAPPING)
    data = parse_date(data, "Date") # create new column Datetime from column Date
    data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance

    data = open_banking_mono_transaction_labelisation(data,"")
    data = open_banking_mono_from_to(data)
    # print(data)

    return open_banking_mono_calcul_score(data)


# oem_http
@router.post("/unit-test")
async def calcul(file: FileUrl, bank: str, password:str,  username: str = Depends(verify_credentials)):

    """
    Pour test
    {
      "file_url": "data/8705791_2022-12-26_2023-04-13.csv"
      "file_url": "data/acces-bank-6521.pdf" ==> ACCESS BANK
      "file_url": "data/first-bank-3815.pdf" ==> FIRST BANK
    }
    """
    file_url = file.file_url
    print(file_url, bank)
    if bank == "MOOV":
        data = read_data(file_url)
        return calcul_score(data)

    elif bank== "ACCESS BANK":
        data = access_bank_pdf_reader(file_url, password)
        data = standardize_column_name(data, ACCESS_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date") # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = access_bank_transaction_labelisation(data, "")
        data = access_bank_from_to(data)
        out = access_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "FIRST BANK":
        data = first_bank_pdf_reader(file_url, password)    # pdf reader
        data = standardize_column_name(data, FIRST_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date") # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = first_bank_transaction_labelisation(data, "")
        data = first_bank_from_to(data)
        out = first_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "STERLING BANK":
        data = sterling_bank_pdf_reader(file_url, password)    # pdf reader
        data = standardize_column_name(data, STERLING_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date") # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = sterling_bank_transaction_labelisation(data, "")
        data = sterling_bank_from_to(data)
        out = sterling_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "WEMA BANK":
        data = wema_bank_pdf_reader(file_url, password)    # pdf reader
        data = standardize_column_name(data, WEMA_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = wema_bank_parse_date(data, "Date") # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = wema_bank_transaction_labelisation(data, "")
        data = wema_bank_from_to(data)
        out = wema_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "UNION BANK":
        data = union_bank_pdf_reader(file_url, password)    # pdf reader
        data = standardize_column_name(data, STERLING_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = union_parse_date(data, "Date", list_formats=["%d/%m/%Y"]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = union_bank_transaction_labelisation(data, "")
        data = union_bank_from_to(data)
        out= union_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "FIRST CITY MUNAMENT BANK":
        data = fcmb_bank_pdf_reader(file_url, password)    # pdf reader
        data = standardize_column_name(data, FCMB_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = fcmb_parse_date(data, "Date", list_formats=["%d-%b-%Y"]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = fcmb_bank_transaction_labelisation(data, "")
        data = fcmb_bank_from_to(data)
        out= fcmb_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "ZENITH BANK":
        data = zenith_bank_pdf_reader(file_url, password)    # pdf reader
        data = standardize_column_name(data, ZENITH_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = zenith_bank_parse_date(data, "Date", list_formats=[]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = zenith_bank_transaction_labelisation(data, "")
        data = zenith_bank_from_to(data)
        out= zenith_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "ECO BANK":
        data = eco_bank_pdf_reader(file_url, password)    # pdf reader
        data = standardize_column_name(data, ECO_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date", list_formats=[]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = eco_bank_transaction_labelisation(data, "")
        data = eco_bank_from_to(data)
        out= eco_bank_calcul_score(data)
        return add_status_output(out, status, errors)