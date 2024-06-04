import time
import os
import json
import csv
import copy
import tabula
import datetime
from io import StringIO
import pandas as pd
import numpy as np
import math
import urllib.request
import requests
from utils.process_access_bank import *
from utils.process_first_bank import *
from utils.process_sterling_bank import *
from utils.process_wema_bank import *
from utils.process_union_bank import *
from utils.process_fcmb_bank import *
from utils.standard_func import *
from utils.process_zenith_bank import *
from utils.process_eco_bank import *

MAPPING = {}
VALUE_MAPPING = {}
CATEGORY = {}
CONFIG_SECURITY = {}

ACCESS_BANK_COLUMNS_MAPPING = {
            "Date" : ["Tran. date", "DATE"],
            "TransDet" : ["Transaction details", "TRANSACTION"],
            "Debit" : ["Debit", "WITHDRAWALS"],
            "Credit" : ["Credit", "LODGEMENTS"],
            "Solde" : ["Balance", "BALANCE"],
            "Reference" : ["Reference"]
        }

FIRST_BANK_COLUMNS_MAPPING = {
            "Date" : ["Tran Date"],
            "TransDet" : ["Narration"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "Solde" : ["Balance"]
        }


STERLING_BANK_COLUMNS_MAPPING = {
            "Date" : ["Tran. date"],
            "TransDet" : ["Transaction details"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "Solde" : ["Balance"]
        }

FCMB_BANK_COLUMNS_MAPPING = {
            "Date" : ["Tran. date"],
            "TransDet" : ["Transaction details"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "Solde" : ["Balance"]
        }

UNION_BANK_COLUMNS_MAPPING = {
            "Date" : ["Tran. date"],
            "TransDet" : ["Transaction details"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "Solde" : ["Balance"]
        }

WEMA_BANK_COLUMNS_MAPPING = {
            "Date" : ["Tran. date"],
            "TransDet" : ["Transaction details"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "Solde" : ["Balance"]
        }

# COLUMNS ZENITH
ZENITH_BANK_COLUMNS_MAPPING = {
            "Date" : ["DATE"],
            "TransDet" : ["DESCRIPTION"],
            "Debit" : ["DEBIT"],
            "Credit" : ["CREDIT"],
            "Solde" : ["BALANCE"]
        }

# COLUMNS ECOBANK
ECO_BANK_COLUMNS_MAPPING ={
            "Date" : ["Tran Date"],
            "TransDet" : ["Narration"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "Solde" : ["Balance"]
        }

# OPEN BANKING MONO
OPEN_BANKING_MONO_COLUMNS_MAPPING = {
            "Date" : ["date"],
            "TransDet" : ["narration"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "TypeTrans" : ["type"],
            "Solde" : ["balance"],
}
        
############################## CONFIG READER ####################################

def load_json_file(file):
    # open json file
    with open(file, 'r') as json_file:
        data = json.load(json_file)
    return data


def load_config(json_path):
    global MAPPING
    global VALUE_MAPPING
    global CATEGORY
    global CONFIG_SECURITY

    # load config from json file
    CONFIG_UVICORN = load_json_file(json_path)["UVICORN"]
    CONFIG_MAPPING = load_json_file(json_path)["MAPPING"]
    CONFIG_SECURITY = load_json_file(json_path)["SECURITY"]
    VALUE_MAPPING = load_json_file(json_path)["VALUE_MAPPING"]
    CATEGORY  = load_json_file(json_path)["CATEGORY"]
    MAPPING = CONFIG_MAPPING

    return CONFIG_UVICORN, CONFIG_MAPPING, CONFIG_SECURITY


############################## PDF REDAER ######################################


def read_data_pdf(document):
    tableaux = tabula.read_pdf(document, pages = 'all',encoding = 'cp1252')

    data = pd.DataFrame(columns=tableaux[len(tableaux)-1].columns)

    i=0
    while i<len(tableaux):
        if (tableaux[i].shape[1]==10):
            data=pd.concat([data,tableaux[i]])
        i=i+1

    return  data


############################### STANDARDIZE COLUMN NAME #######################

def standardize_name(df, mapping={}):
    """
    Renomme les colonnes du DataFrame `df` en utilisant la configuration de renommage `mapping`.

    :param df: DataFrame à renommer.
    :param mapping: Dictionnaire de configuration de renommage.
    :return: DataFrame avec les colonnes renommées.
    """
    try:
        # Renommer les colonnes selon la configuration
        for standard_name, name_specifics in mapping.items():
            for name_specific in name_specifics:
                if name_specific in df.columns:
                    df = df.rename(columns={name_specific: standard_name})
                    break
        return df
    except Exception as e:
        print(f"[ERROR][utils.utils.standardize_name] {e}")
        return df 

    
############################# DATA VALIDATION ###################################
def transforme_montant(row, liste_trans_types, list_transfert_value, msisdn):
    if row['TransType'] in liste_trans_types and row['Montant'] >= 0:
        row['Montant'] = -row['Montant']

    if row['TransType'] in list_transfert_value and row['ToMSISDN'] != msisdn and msisdn is not None  and row['Montant'] >= 0:
        row['Montant'] = -row['Montant']
    return row


def standardize_data(data):
    status = "success"
    list_error = []
    try:
        # check columns, create column with null value if not exist
        noms_colonnes = list(MAPPING.keys())
        for nom_colonne in noms_colonnes:
            if nom_colonne not in data.columns:
                data[nom_colonne] = [None] * len(data.index)
        # find msisdn acount proprietary
        msisdn= None
        data['TransType'] = data['TransType'].str.lower() 
        try:
            msisdn=data[data['TransType'].isin([element.lower() for element in VALUE_MAPPING["credit"]])]['ToMSISDN'].iloc[0]
            if msisdn == "" or pd.isna(msisdn):
                msisdn=data[data['TransType'].isin([element.lower() for element in VALUE_MAPPING["debit"]])]['FromMSISDN'].iloc[0]
                if msisdn == "" or pd.isna(msisdn):
                    msisdn = None 
        except:
            pass
        # standardize amount values
        data = data.apply(transforme_montant, args=([element.lower() for element in VALUE_MAPPING["debit"]],
                                                    [element.lower() for element in VALUE_MAPPING["transfert"]],
                                                    msisdn), axis=1)
        return data, status
    except Exception as e:
        print(f"[ERROR][utils.utils.standardize_data] {e}")
        status = "error"
        return data, status


############################## DATA REDAER ######################################
def autoDetectSep(fichier):
    sep = ";"
    try:
        with open(fichier, 'r') as file:
            sample = file.read(1024)  # Lisez un échantillon du fichier
            dialect = csv.Sniffer().sniff(sample)
            sep = dialect.delimiter
    except Exception as e:
        print(e)
        sep = ";"
    return sep 

def read_csv(file, sep=";", encoding='ISO-8859-1'):
    data = pd.read_csv(file, sep=sep, encoding=encoding)
    if len(data.columns) < 2:
        data = pd.read_csv(file, sep=",", encoding=encoding)
        return data
    return data

def read_data(file_path_url, _type_='csv', filestream=True):
    try:
        if str(_type_).lower() == "csv":
            if filestream:
                df = pd.read_csv(copy.copy(file_path_url), sep=";", encoding='ISO-8859-1')
                if len(df.columns) < 2:
                    df = pd.read_csv(file_path_url, sep=",", encoding='ISO-8859-1')
                    data = standardize_name(df, MAPPING)
                    data, status = standardize_data(data)
                    return data
                data = standardize_name(df, MAPPING)
                data, status = standardize_data(data)
                return data
            else:
                data = read_csv(file_path_url)
                data = standardize_name(data, MAPPING)
                data, status = standardize_data(data)
            if status == "error":
                return pd.DataFrame()
            return data
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"[ERROR][utils.utils.read_data] {e}")
        return pd.DataFrame()
    
def read_json(input_json):
    try:
        df = pd.DataFrame(input_json)
        # Créer une nouvelle colonne "Debit" avec les valeurs de la colonne "amount" lorsque le type est "debit", sinon 0
        df['Debit'] = df.apply(lambda row: row['amount'] if row['type'] == 'debit' else 0, axis=1)
        # Créer une nouvelle colonne "Credit" avec les valeurs de la colonne "amount" lorsque le type est "credit", sinon 0
        df['Credit'] = df.apply(lambda row: row['amount'] if row['type'] == 'credit' else 0, axis=1)

        df['Debit'] = df['Debit'].map('{:,.2f}'.format)
        df['Credit'] = df['Credit'].map('{:,.2f}'.format)
        df['balance'] = df['balance'].map('{:,.2f}'.format)
        return df

    except Exception as e:
        print(f"[ERROR][utils.utils.read_json] {e}")
        return pd.DataFrame()

def get_file_extension(file_path):
    # Obtenir l'extension du fichier
    _, extension = os.path.splitext(file_path)
    return extension.lower().replace('.', '')


def get_files(input_json, domain=""):
    try:
        print("input_json", input_json)
        path_list = []
        if isinstance(input_json, dict):
            if 'files' in input_json.keys():
                files_list = input_json["files"]
                if  isinstance(files_list, list):
                    for json_file in files_list:
                        if 'file' in json_file.keys() and 'institution' in json_file.keys():
                            jsn = json_file['file']
                            institution = json_file['institution']
                            employer = []
                            if "employerNames" in json_file.keys():
                                if isinstance(json_file['employerNames'], list):
                                    employer = json_file['employerNames']
                            if 'path' in jsn.keys():
                                if 'baseUrl' in jsn.keys() and jsn["baseUrl"] is not None and str(jsn["baseUrl"]) != "":
                                    dict_files = {
                                        "baseUrl":str(jsn["baseUrl"]),
                                        "path" : str(jsn["path"]),
                                        "ext" : get_file_extension(str(jsn["path"])),
                                        "code" : str(jsn["accessCode"]),
                                        "bank" : str(institution["name"]),
                                        "employer" : employer
                                    }
                                    path_list.append(dict_files)
                                    print("baseUrl", str(jsn["baseUrl"]))
                                else:    
                                    print("No baseUrl")
                                    
            return path_list
        else:
            return []
    except Exception as e:
        pass
############################ DATE TOOLS ########################################
def week_of_month(date):
    first_day = date.replace(day=1)
    days_until_first_day = (date - first_day).days
    if days_until_first_day < 7:
        return 1
    elif days_until_first_day < 14:
        return 2
    elif days_until_first_day < 21:
        return 3
    elif days_until_first_day < 28:
        return 4
    else:
        return 5

def process_top_3(data, transCol):
    try:
        data = data.loc[data[transCol] != '']
        data = data.groupby(transCol)["Montant"].sum()
        data = data.reset_index()
        head = data.sort_values(by="Montant", ascending=True).head(3)
        return list(head[transCol].tolist())
    except Exception as e:
        print(e)
        return []
    
def search_recurring_cat(data, column_rec, monthPeriod):
    print("===================================================================>", str(monthPeriod),  str(monthPeriod*0.3), column_rec)
    df_rec = data.loc[data['Montant'] < 0][['mois', column_rec]].groupby(['mois', column_rec]).count().reset_index()
    df_rec = df_rec.groupby(column_rec)['mois'].count().reset_index()
    print(df_rec)
    df_rec = df_rec.loc[df_rec['mois']>monthPeriod*0.3]
    liste = [x.lower() for x in df_rec[column_rec].tolist()]
    liste = list(set(liste) - set([x.lower() for x in VALUE_MAPPING['debit_not_expense']]))
    liste = list(set(liste) - set(['']))  
    return liste


def attribuer_plage(valeur_min, valeur_max, plage=10000):
    # Déterminez dans quelle plage de 10 000 se trouvent les valeurs
    plage_debut = (valeur_min // plage) * plage
    nouvelle_valeur_min = plage_debut
    nouvelle_valeur_max = plage_debut + plage

    return nouvelle_valeur_min, nouvelle_valeur_max
############################### CACUL ##########################################

def calcul_score(data):
    tps1 = time.time()

    try:
        #format='mixed', 
        data['Datetime'] = pd.to_datetime(data['Date'], format='%d/%m/%y %H:%M', infer_datetime_format=True)
        print(data['Datetime'])
        data['Date']=data['Datetime'].dt.date
        data['Heure']=data['Datetime'].dt.time
        min_date=data['Date'].min()
        max_date=data['Date'].max()
        nb_jour_activity=data['Date'].nunique()
        nb_jour_total=(max_date - min_date).days
        liste_date_total=pd.date_range(min_date, periods=nb_jour_total+1).strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.Datetime/Date/Heure] {e}")
        data['Datetime'] = None
        data['Date']=None
        data['Heure']=None
        min_date=None
        max_date=None
        nb_jour_activity=None
        nb_jour_total=None
        liste_date_total=None

    # =============================================================================
    # Cashflow Analysis
    # =============================================================================

    
    try:    
        averageCredits=data.loc[data['Montant'] >0, 'Montant'].mean()
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.averageCredits] {e}")
        averageCredits=None

    try:
        averageDebits=data.loc[data['Montant'] <0, 'Montant'].abs().mean()
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.averageDebits] {e}")
        averageDebits=None

    try:    
        initialBalance=data[data['Datetime'] == data['Datetime'].min()]["Solde"].values[0] # plus heure
        initialBalance = int(initialBalance)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.initialBalance] {e}")
        initialBalance=None

    try:    
        closingBalance=data[data['Datetime'] == data['Datetime'].max()]["Solde"].values[0] # plus  heure
        closingBalance = int(closingBalance)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.closingBalance] {e}")
        closingBalance=None

    try:
        firstDay=min_date.strftime("%Y-%m-%d")
        lastDay=max_date.strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.firstDay/lastDay] {e}")
        firstDay=None
        lastDay=None       

    try:
        #monthPeriod=int(nb_jour_total/30)+1
        #monthPeriod = int(monthPeriod)
        monthPeriod = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month)
        monthPeriod = int(monthPeriod) + 1
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.monthPeriod] {e}")
        monthPeriod=None

    try:
        dayPeriode=int(nb_jour_total)
        noOfTransactingDays=int(nb_jour_activity)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.noOfTransactingDays/dayPeriode] {e}")
        dayPeriode=None
        noOfTransactingDays=None

    try:
        noOfTransactingMonths=pd.DatetimeIndex(data['Date']).month.nunique()
        noOfTransactingMonths = int(noOfTransactingMonths)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.noOfTransactingMonths] {e}")
        noOfTransactingMonths=None


    try:
        totalCreditTurnover=data.loc[data['Montant'] >0, 'Montant'].sum()
        totalCreditTurnover = int(totalCreditTurnover)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.totalCreditTurnover] {e}")
        totalCreditTurnover=None
    
    try:
        nbTotalCreditTurnover=data.loc[data['Montant'] >0, 'Montant'].count()
        nbTotalCreditTurnover = int(nbTotalCreditTurnover)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.nbTotalCreditTurnover] {e}")
        nbTotalCreditTurnover=None

    try:
        nbTotalDebitTurnover=data.loc[data['Montant'] <0, 'Montant'].count()
        nbTotalDebitTurnover = int(nbTotalDebitTurnover)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.nbTotalCreditTurnover] {e}")
        nbTotalDebitTurnover=None

    try:
        totalDebitTurnover=data.loc[data['Montant'] <0, 'Montant'].abs().sum()
        totalDebitTurnover = int(totalDebitTurnover)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.totalDebitTurnover] {e}")
        totalDebitTurnover=None

    try:
        credit_debit =  totalCreditTurnover / totalDebitTurnover
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.credit_debit] {e}")
        credit_debit=None


    try:
        yearInStatement=pd.DatetimeIndex(data['Date']).year.unique()
        yearInStatement = int(yearInStatement[0])
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.yearInStatement] {e}")
        yearInStatement=None    

    try:
        accountActivity=nb_jour_activity/nb_jour_total
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.accountActivity] {e}")
        accountActivity=None    

    # averageBalance
    try:
        liste_date_df = pd.DataFrame(liste_date_total,columns =['Date'])
        liste_date_df["Date"]=liste_date_df["Date"].astype(str)

        balance=data[["Date","Heure","Solde"]]
        
        date_heure_max=data[["Date","Heure"]].groupby(['Date']).max().reset_index()
        balance= pd.merge(date_heure_max,balance,left_on=['Date','Heure'] ,right_on=['Date','Heure'])
        balance["Date"]=balance["Date"].astype(str)
        all_balance= pd.merge(liste_date_df,balance[["Date","Solde"]], on="Date",how="left").fillna(method='ffill')
        
        data["mois"]=pd.DatetimeIndex(data['Date']).month
        nb_month_activity = data['mois'].nunique()
        mois_max=data[["mois","Date"]].groupby(['mois']).max().reset_index()
        balance= pd.merge(date_heure_max,balance,left_on=['Date','Heure'] ,right_on=['Date','Heure'])
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.accountActivity] {e}")
        liste_date_df = None
        balance=None        
        date_heure_max=None
        all_balance= None
        mois_max=None
        nb_month_activity = None

    try:
        # groupena dtejour, ze max heure iany no raisina - (moyenne max journaliere) 
        averageBalance=all_balance["Solde"].mean()
        print("Solde moyen", data["Solde"].mean(), all_balance["Solde"].mean()) 
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.averageBalance] {e}")
        averageBalance=None


    try:
        df = data.copy()
        df['mois'] = df['Datetime'].dt.to_period('M')
        averageMonthlyCredit = df.loc[df['Montant'] >0].groupby('mois')['Montant'].sum().mean()
        averageMonthlyDebit = abs(df.loc[df['Montant'] <0].groupby('mois')['Montant'].sum().mean())
        nbAverageMonthlyCredit = int(df.loc[df['Montant'] >0].groupby('mois')['Montant'].count().mean())
        nbAverageMonthlyDebit = int(df.loc[df['Montant'] <0].groupby('mois')['Montant'].count().mean())
        netCashFlow = (abs(averageMonthlyCredit)-abs(averageMonthlyDebit))
        if netCashFlow < 0:
            netCashFlow = 0
        MaxMonthlyRepayment = netCashFlow * 0.6
        print("netCashFlow", netCashFlow, MaxMonthlyRepayment)
        del df
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.averageMonthlyCredit] {e}")
        averageMonthlyCredit=None
        averageMonthlyDebit=None
        nbAverageMonthlyCredit = None    
        nbAverageMonthlyDebit = None
        netCashFlow = 0
        MaxMonthlyRepayment = 0

    """
    try:
        MaxMonthlyRepayment=(abs(averageMonthlyCredit)-abs(averageMonthlyDebit))*0.4 
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.MaxMonthlyRepayment] {e}")
        MaxMonthlyRepayment=None    
    """

    try:
        totalMonthlyCredit=int(data.loc[data['Montant'] >0, 'Montant'].sum())
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.totalMonthlyCredit] {e}")
        totalMonthlyCredit=None    

    
    
    # =============================================================================
    # Spend Analysis
    # =============================================================================
    try:
        data['TransTypeDet'] = data['TransTypeDet'].fillna('')
        data['To Name'] = data['To Name'].fillna('')
        data['TransType'] = data['TransType'].fillna('')
        data['ProvidCat'] = data['ProvidCat'].fillna('')
        data['categorisation'] = data['To Name'] + data['TransTypeDet'] +  data['ProvidCat']
        
        data['categorisation'] = data['categorisation'].fillna('')
        data['categorisation'] = data['categorisation'].str.lower()
        data['TRANS'] = data['TransType'] + data['TransTypeDet']
        data['TRANS'] =data['TRANS'].fillna('')
        data['TRANS'] =data['TRANS'].str.lower()
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.combineToNameAndTypeTransDet] {e}")

    # Airtime
    try:
        #liste_airtime = ["BUY AIRTIME", "Airtime_For_You"]
        liste_airtime = CATEGORY["airtime"]
        liste_airtime = [x.lower() for x in liste_airtime]
        airtime=data.loc[data['categorisation'].str.contains('|'.join(liste_airtime)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.airtime] {e}")
        airtime=0

    # Bundle
    try:
        #liste_bundle = ["Bundle_For_You"]
        liste_bundle = CATEGORY["bundle"]
        liste_bundle = [x.lower() for x in liste_bundle]
        data_spt = data.loc[data['Montant'] < 0]
        bundle=data_spt.loc[data_spt['categorisation'].str.contains('|'.join(liste_bundle)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.bundle] {e}")
        bundle=0
        
    # Facture
    try:
        #liste_bills = ["OVA_SBEE", "OVA_OPENSIUTIL"]
        liste_bills = CATEGORY["bills"]
        liste_bills = [x.lower() for x in liste_bills]
        data_spt = data.loc[data['Montant'] < 0]
        bills=data_spt.loc[data_spt['categorisation'].str.contains('|'.join(liste_bills)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.bills] {e}")
        bills=None

    # Forfaits Internet
    try:
        #liste_webSpend = ["OVA_CIS", "BUY DATA", "Bundle_For_You"]
        liste_webSpend = CATEGORY["webspend"]
        liste_webSpend = [str(x).lower() for x in liste_webSpend]
        data_spt = data.loc[data['Montant'] < 0]
        webSpend=data_spt.loc[data_spt['categorisation'].str.contains('|'.join(liste_webSpend)), 'Montant'].abs().sum()/noOfTransactingMonths
        webSpend=0
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.webSpend] {e}")
        webSpend=0

    # Point de vente
    try:
        #liste_pos = ["PAYMENT"]
        liste_pos = CATEGORY["posspend"]
        liste_pos = [x.lower() for x in liste_pos]
        data_spt = data.loc[data['Montant'] < 0]
        posSpend=data_spt.loc[data_spt['categorisation'].str.contains('|'.join(liste_pos)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.posSpend] {e}")
        posSpend=0


    try:
        data_spt = data.loc[data['Montant'] < 0]
        bankCharges=data_spt['FromFee'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.bankCharges] {e}")
        bankCharges=0

    try:
        #liste_spt = ["Transfert_Regional_MTN_CI"]
        liste_spt = CATEGORY["spendontransfert"]
        liste_spt = [x.lower() for x in liste_spt]
        data_spt = data.loc[data['Montant'] < 0]
        spendOnTransfers=data_spt.loc[data_spt['TransType'].str.contains('|'.join(liste_spt)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.spendOnTransfers] {e}")
        spendOnTransfers=0

    try:
        #liste_enter = ["OVA_CANAL"]
        liste_enter = CATEGORY["entertainment"]
        liste_enter = [x.lower() for x in liste_enter]
        data_spt = data.loc[data['Montant'] < 0]
        entertainment=data_spt.loc[data_spt['categorisation'].str.contains('|'.join(liste_enter)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.entertainment] {e}")
        entertainment=0

    try:
        #liste_dab = ["CASH_OUT"]
        liste_dab = CATEGORY["dab"]
        liste_dab = [str(x).lower() for x in liste_dab]        
        data_spt = data.loc[data['Montant'] < 0]
        data_spt['ToUsrNameLower'] = data_spt['To Name'].str.lower()
        dab=data_spt.loc[data_spt['ToUsrNameLower'].str.contains('|'.join(liste_dab)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.dab] {e}")
        dab=0

    try:
        gambling=0 # differenciation gambling?
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.gambling] {e}")
        gambling=0

    try:
        #liste = ["OVA_CANAL", "OVA_OPENSIUTIL", "OVA_SBEE", "OVA_CIS"]
        #liste = CATEGORY["reccuring"]
        #liste = [x.lower() for x in liste]
        """
        column_rec = "TransTypeDet"
        if data['TransTypeDet'].isna().all() and not data['TransTypeDet'].eq('').any():
            column_rec = "TransType"
        """
        column_rec = "TransTypeDet"

        liste = search_recurring_cat(data, column_rec, monthPeriod)
        if len(liste) == 0:
            column_rec = "ProvidCat"
            liste = search_recurring_cat(data, column_rec, monthPeriod)

        liste_reccuring_trans = liste
        print(liste)
        data_reccuring=data.loc[data['categorisation'].str.contains('|'.join(liste))]
        data_reccuring['Montant'] = data_reccuring['Montant'].abs()
        averageRecurringExpense=data_reccuring.groupby('mois')['Montant'].sum().mean()
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.averageRecurringExpense] {e}")
        averageRecurringExpense=None
        liste_reccuring_trans = []

    hasRecurringExpense=None
    try:
        if averageRecurringExpense is not None and averageRecurringExpense > 0:
            hasRecurringExpense=True
        else:
            hasRecurringExpense=False
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.hasRecurringExpense] {e}")
        hasRecurringExpense=False


    try:

        liste_spt = CATEGORY["international"]
        liste_spt = [x.lower() for x in liste_spt]
        data_spt = data.loc[data['Montant'] < 0]
        internationalTransactionsSpend=data_spt.loc[data['TRANS'].str.contains('|'.join(liste_spt)), 'Montant'].abs().sum()/noOfTransactingMonths
        #internationalTransactionsSpend=0 # differenciation internationalTransactionsSpend?
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.internationalTransactionsSpend] {e}")
        internationalTransactionsSpend=0


    try:
        liste_ussd = CATEGORY["ussd"] # identification/differenciation ussdTransactions?
        ussdTransactions=data.loc[data['categorisation'].str.contains('|'.join(liste_ussd)), 'Montant'].abs().sum()/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.ussdTransactions] {e}")
        ussdTransactions=0


    try:
        liste_spt = VALUE_MAPPING["debit_not_expense"]
        liste_spt = [x.lower() for x in liste_spt]
        data_spt = data.loc[data['Montant'] < 0]
        totalExpenses=data_spt.loc[~data['TRANS'].str.contains('|'.join(liste_spt)), 'Montant'].abs().sum()/noOfTransactingMonths
        #totalExpenses=totalDebitTurnover/noOfTransactingMonths
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.totalExpenses] {e}")
        totalExpenses=None

    try:
        data["ProvidCat"] = data["ProvidCat"].fillna('')
        data["trans_temp_salary"] = data["ProvidCat"].str.lower()
        liste = CATEGORY["salary"] 
        liste = [str(x).lower() for x in liste]
        data_salary = data.loc[data['trans_temp_salary'].str.contains('|'.join(liste))]
        data_salary["mois_salary"]=pd.DatetimeIndex(data_salary['Date']).month
        data_salary["Montant"] = data_salary["Montant"].abs()
        nb_month_unique_salary = data_salary['mois_salary'].nunique()
        print(data_salary)
        print(data_salary["Montant"])
        print(nb_month_unique_salary)
    except:
        pass


    try:
        numberOfSalaryPayments=data_salary.loc[data_salary['Montant'] > 0, 'Montant'].count() 
        numberOfSalaryPayments = int(numberOfSalaryPayments)
        print("numberOfSalaryPayments", numberOfSalaryPayments)
        #numberOfSalaryPayments=0
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.numberOfSalaryPayments] {e}")
        numberOfSalaryPayments=0

    try:
        averageOtherIncome=0 # pas d'information permetant de differencier les autres revenus 
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.averageOtherIncome] {e}")
        averageOtherIncome=0


    try:
        averageSalary=data_salary.loc[data_salary['Montant'] > 0, 'Montant'].abs().sum()/numberOfSalaryPayments
        averageSalary = float(averageSalary)
        print("averageSalary", averageSalary)
        #averageSalary = 0 
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.averageSalary] {e}")
        averageSalary=0


    try:
        std = data_salary.loc[data_salary['Montant'] > 0, 'Montant'].abs().std()
        stability = (std/averageSalary)
        freq = 1 /nb_month_unique_salary
        confidenceIntervalOnSalaryDetection = 1 - stability - min([stability, freq]) # pas d'information permetant de differencier les salaires
        confidenceIntervalOnSalaryDetection = float(confidenceIntervalOnSalaryDetection) * 100
        print("confidenceIntervalOnSalaryDetection", confidenceIntervalOnSalaryDetection)
        if confidenceIntervalOnSalaryDetection > 100:
            confidenceIntervalOnSalaryDetection = 100
        print("confidenceIntervalOnSalaryDetection", confidenceIntervalOnSalaryDetection)
        #confidenceIntervalOnSalaryDetection = None
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.confidenceIntervalOnSalaryDetection] {e}")
        confidenceIntervalOnSalaryDetection=None


    try:
        data_salary['Jour'] = data_salary['Datetime'].dt.day
        mode_day = data_salary['Jour'].mode().values[0]
        print("mode_day", mode_day)
        expectedSalaryDay=None 
        if mode_day == 1:
            # Si le mode est 1, calculez le 75ème percentile des jours de paiement.
            percentile_75 = data_salary['Jour'].quantile(0.75)
            # Trouvez l'indice du jour le plus proche du 75ème percentile.
            closest_index = (data_salary['Jour'] - percentile_75).abs().idxmin()
            # Obtenez la date correspondante à l'indice.
            expectedSalaryDay = data_salary['Date'][closest_index].strftime("%d")
            expectedSalaryDay = int(expectedSalaryDay)
        else:
            expectedSalaryDay = int(mode_day)
        print("expectedSalaryDay", expectedSalaryDay)
        #expectedSalaryDay=None
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.expectedSalaryDay] {e}")
        expectedSalaryDay=None


    try:
        print('lastSalaryDate', data_salary["Date"].max())
        lastSalaryDate=data_salary["Date"].max().strftime("%Y-%m-%d") # pas d'information permetant de differencier les salaires 
        print("lastSalaryDate", lastSalaryDate)
        #lastSalaryDate=None
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.lastSalaryDate] {e}")
        lastSalaryDate=None


    try:
        medianIncome=data_salary.loc[data_salary['Montant'] > 0, 'Montant'].abs().median() # pas d'information permetant de differencier les salaires et des autres revenus
        medianIncome = int(medianIncome)
        print("medianIncome", medianIncome)
        #medianIncome=None
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.medianIncome] {e}")
        medianIncome=None


    try:
        numberOtherIncomePayments=0 # pas d'information permetant de differencier les salaires et des autres revenus
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.numberOtherIncomePayments] {e}")
        numberOtherIncomePayments=0




    try:
        if numberOfSalaryPayments is not None and numberOfSalaryPayments > 0:
            salaryEarner=True
        else:
            salaryEarner=False
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.salaryEarner] {e}")
        salaryEarner=False


    try:
        salaryFrequency=data_salary.loc[data_salary['Montant'] > 0].groupby('mois_salary')['Montant'].count().mean() # pas d'information permetant de differencier les salaires et des autres revenus
        print("salaryFrequency", salaryFrequency)
        #salaryFrequency=None
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.salaryFrequency] {e}")
        salaryFrequency=None


    try:
        if averageOtherIncome is not None and averageSalary is not None and ((averageOtherIncome + averageSalary)) > 0: 
            netAverageMonthlyEarnings=averageOtherIncome + averageSalary - totalExpenses
            if netAverageMonthlyEarnings < 0:
                netAverageMonthlyEarnings = 0
        else:
            netAverageMonthlyEarnings = 0
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.netAverageMonthlyEarnings] {e}")
        netAverageMonthlyEarnings=0


    try:   
        # inflowOutflowRate
        group_mois = data.groupby('mois')
        debit_mois=pd.Series(group_mois.apply(lambda x: x[x['Montant'] >0]['Montant'].sum()),name="credit").to_frame()
        credit_mois=pd.Series(group_mois.apply(lambda x: x[x['Montant'] <0]['Montant'].sum()),name="debit").to_frame()
        
        inflowOutflowRate=pd.concat([debit_mois,credit_mois], axis=1)
        inflowOutflowRate["ratio"]=inflowOutflowRate["debit"]+inflowOutflowRate["credit"]
        inflowOutflowRate["month_status"]=np.select([inflowOutflowRate['ratio'] > 0, inflowOutflowRate['ratio'] <0], ['positive_months', 'negative_months'], default='low')
        occurence=inflowOutflowRate["month_status"].value_counts()
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.airtime/totalExpenses/webSpend/etc...] {e}")
        # inflowOutflowRate
        group_mois = None
        debit_mois=None
        credit_mois=None
        inflowOutflowRate=None
        occurence=None


    try:
        occurence_pos=occurence["positive_months"].sum()
    except:
        occurence_pos=0
        
    try:
        occurence_neg=occurence["negative_months"].sum()
    except:
        occurence_neg=0    
    
    if occurence_pos>occurence_neg:
        inflowOutflowRate="Positive Cash Flow"
    elif occurence_pos<occurence_neg:
        inflowOutflowRate="Negative Cash Flow"
    else:
        inflowOutflowRate="Neutral Cash Flow"
    
    try:
        # Most occuring name
        liste = VALUE_MAPPING["transfert"]
        liste = [x.lower() for x in liste]
        data["TRANS_TEMP"] = data["TransType"].str.lower()
        data_top_transfert=data.loc[data["TRANS_TEMP"].str.contains('|'.join(liste))]        
        
        # Création d'une nouvelle colonne "Account" en fonction de vos critères.
        data_top_transfert['From Name'] = data_top_transfert['From Name'].fillna('').astype(str)
        data_top_transfert['To Name'] = data_top_transfert['To Name'].fillna('').astype(str)
        data_top_transfert['FromAccount'] = data_top_transfert['FromAccount'].fillna('').astype(str)
        data_top_transfert['ToAccount'] = data_top_transfert['ToAccount'].fillna('').astype(str)
        data_top_transfert['FromMSISDN'] = data_top_transfert['FromMSISDN'].fillna('').astype(str)
        data_top_transfert['ToMSISDN'] = data_top_transfert['ToMSISDN'].fillna('').astype(str)
        data_top_transfert['FromMSISDN'] = data_top_transfert['FromMSISDN'].str.replace('.0', '')
        data_top_transfert['ToMSISDN'] = data_top_transfert['ToMSISDN'].str.replace('.0', '')
        
        data_top_transfert['FromAccountName'] = data_top_transfert.apply(lambda row: row['From Name'] +' '+row['FromAccount']+' '+ row['FromMSISDN'] if (
            (row['From Name'] is not None) and
            (row['From Name'] != '') and
            (not row['From Name'].isdigit())
        ) else row['FromAccount']+' '+row['FromMSISDN'], axis=1)
        
        data_top_transfert['FromAccountName'] = data_top_transfert['FromAccountName'].str.strip() # supprimer les espaces au debut et a la fin
        
        data_top_transfert['ToAccountName'] = data_top_transfert.apply(lambda row: row['To Name']+' '+row['ToAccount'] +' '+ row['ToMSISDN'] if (
            (row['To Name'] is not None) and
            (row['To Name'] != '') and
            (not row['To Name'].isdigit())
        ) else row['ToAccount']+' '+row['ToMSISDN'], axis=1)
        
        data_top_transfert['ToAccountName'] = data_top_transfert['ToAccountName'].str.strip() # supprimer les espaces au debut et a la fin
        
    except:
        pass 

    try:
        data_incoming = data_top_transfert.loc[data_top_transfert['Montant'] > 0]
        data_incoming = data_incoming.groupby("FromAccountName")["Montant"].sum()
        data_incoming = data_incoming.reset_index()
        head = data_incoming.sort_values(by="Montant", ascending=False).head(3) 
        topIncomingTransferAccount = head["FromAccountName"].tolist()[0]
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.topIncomingTransferAccount] {e}")
        topIncomingTransferAccount=None

    try:
        data_incoming = data_top_transfert.loc[data_top_transfert['Montant'] < 0]
        data_incoming = data_incoming.groupby("ToAccountName")["Montant"].sum()
        data_incoming = data_incoming.reset_index()
        head = data_incoming.sort_values(by="Montant", ascending=True).head(3) 
        topTransferRecipientAccount = head["ToAccountName"].tolist()[0]
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.topTransferRecipientAccount] {e}")
        topTransferRecipientAccount=None


    try: 
        lastDateOfCredit=data.loc[data['Montant'] >0]["Date"].max().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.lastDateOfCredit] {e}")
        lastDateOfCredit=None

    try:
        lastDateOfDebit=data.loc[data['Montant'] <0]["Date"].max().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.lastDateOfDebit] {e}")
        lastDateOfDebit=None




    try:
        accountSweep=False # differenciation accountSweep?
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.accountSweep] {e}")
        accountSweep=False

    try:
        gamblingRate=None # differenciation gamblingRate?
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.gamblingRate] {e}")
        gamblingRate=None


    try:
        loanAmount=None # differenciation loanAmount?
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.loanAmount] {e}")
        loanAmount=None

    try:
        loanInflowRate=None # differenciation loanInflowRate?
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.loanInflowRate] {e}")
        loanInflowRate=None


    try:
        loanRepaymentInflowRate=None # differenciation loanRepaymentInflowRate?
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.loanRepaymentInflowRate] {e}")
        loanRepaymentInflowRate=None

    try:
        loanRepayments=None # differenciation loanRepayments?
    except Exception as e:
        #print(f"[ERROR][utils.utils.calcul_score.loanRepayments] {e}")
        loanRepayments=None


    # =============================================================================
    # Transaction patterns Analysis
    # =============================================================================

        
    try:        
        #data['MoisStr'] = data['Datetime'].dt.strftime('%B') 
        #data['Week'] = data['Datetime'].dt.isocalendar().week
        index_max = data.loc[data['Montant'] >0, 'Montant'].idxmax()
        #mois_max = data.loc[index_max, 'MoisStr']
        #semaine_max = data.loc[index_max, 'Week']
        #highestMAWOCredit={"month" : mois_max, "week" : int(semaine_max)}
        highestMAWOCredit=week_of_month(data.loc[index_max, 'Datetime'])
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.highestMAWOCredit] {e}")
        #highestMAWOCredit={"month" : None, "week" : None}
        highestMAWOCredit=None

    try:        
        #data['MoisStr'] = data['Datetime'].dt.strftime('%B') 
        #data['Week'] = data['Datetime'].dt.isocalendar().week
        index_max = data.loc[data['Montant'] <0, 'Montant'].idxmin()
        #mois_max = data.loc[index_max, 'MoisStr']
        #semaine_max = data.loc[index_max, 'Week']
        #highestMAWODebit={"month" : mois_max, "week" : int(semaine_max)}
        highestMAWODebit=week_of_month(data.loc[index_max, 'Datetime'])
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.highestMAWODebit] {e}")
        #highestMAWODebit={"month" : None, "week" : None}
        highestMAWODebit=None

    try:        
        #data['MoisStr'] = data['Datetime'].dt.strftime('%B') 
        #data['Week'] = data['Datetime'].dt.isocalendar().week
        #index_max = data.loc[data['Solde'] == 0, 'Solde'].idxmin()
        #mois_max = data.loc[index_max, 'MoisStr']
        #semaine_max = data.loc[index_max, 'Week']
        #MAWWZeroBalanceInAccount={"month" : mois_max, "week" : int(semaine_max)}
        #MAWWZeroBalanceInAccount=week_of_month(data.loc[index_max, 'Datetime'])
        ############################# NEW DEF ###################################
        df_zero_solde = data[data['Solde'] <= 0]
        occurrences_par_jour = df_zero_solde.groupby(df_zero_solde['Date']).size().reset_index(name='occurrences')
        print(occurrences_par_jour)
        jour_plus_occurrence = occurrences_par_jour[occurrences_par_jour['occurrences'] == occurrences_par_jour['occurrences'].max()]
        print(jour_plus_occurrence)
        jour_max_occurrence_date = pd.to_datetime(jour_plus_occurrence['Date'].iloc[0])
        MAWWZeroBalanceInAccount=week_of_month(jour_max_occurrence_date)
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.MAWWZeroBalanceInAccount] {e}")
        #MAWWZeroBalanceInAccount={"month" : None, "week" : None}
        MAWWZeroBalanceInAccount=None

    try:        
        valeur_min = data['Solde'].min()
        valeur_max = data['Solde'].max()
        frequence = 1
        while (frequence > 0 and (((valeur_max - valeur_min) / frequence) > 10)):
            frequence = frequence * 10
        if frequence > 10000:
            frequence = 10000
        print('frequence', frequence)
        
        b_min, _ = attribuer_plage(valeur_min, valeur_min, plage=10000)
        _, b_max = attribuer_plage(valeur_max, valeur_max, plage=10000)
        print('b_min, b_max',b_min, b_max)
        intervalle = pd.interval_range(start=b_min, end=b_max,freq=frequence,  closed='right')
        data['Intervalle'] = pd.cut(data['Solde'], bins=intervalle)
        occurrences = data['Intervalle'].value_counts()
        intervalle_plus_frequent = occurrences.idxmax()
        intervalle_min = intervalle_plus_frequent.left
        intervalle_max = intervalle_plus_frequent.right
        nombre_occurrences_max = occurrences.max()
        

        #intervalle_min, intervalle_max = attribuer_plage(intervalle_min, intervalle_max, plage=10000)

        mostFrequentBalanceRange = {"min":int(intervalle_min),"max": int(intervalle_max),"count": int(nombre_occurrences_max)}
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.mostFrequentBalanceRange] {e}")
        mostFrequentBalanceRange={"min":None,"max": None,"count": None}

    try:        
        valeur_min = data['Montant'].min()
        valeur_max = data['Montant'].max()
        frequence = 1
        while (frequence > 0 and (((valeur_max - valeur_min) / frequence) > 10)):
            frequence = frequence * 10
        if frequence > 10000:
            frequence = 10000

        b_min, _ = attribuer_plage(valeur_min, valeur_min, plage=10000)
        _, b_max = attribuer_plage(valeur_max, valeur_max, plage=10000)
        print('b_min, b_max',b_min, b_max)

        intervalle = pd.interval_range(start=b_min, end=b_max,freq=frequence, closed='right')
        data['Intervalle'] = pd.cut(data['Montant'].abs(), bins=intervalle)
        occurrences = data['Intervalle'].value_counts()
        intervalle_plus_frequent = occurrences.idxmax()
        intervalle_min = intervalle_plus_frequent.left
        intervalle_max = intervalle_plus_frequent.right
        nombre_occurrences_max = occurrences.max()
        mostFrequentTransactionRange = {"min":int(intervalle_min),"max": int(intervalle_max),"count": int(nombre_occurrences_max)}
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.mostFrequentTransactionRange] {e}")
        mostFrequentTransactionRange={"min":None,"max": None,"count": None}


    try:        
        df_filtre = data[data['Solde'] < 5000]
        NODWBalanceLess5000 = df_filtre.groupby(df_filtre['Date'])['Date'].nunique().count()
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.NODWBalanceLess5000] {e}")
        NODWBalanceLess5000=None



    try:        
        df_filtre = data[(data['Montant'].abs() >= 100000) & (data['Montant'].abs() <= 500000)]
        transactionsBetween100000And500000 = df_filtre.shape[0]
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.transactionsBetween100000And500000] {e}")
        transactionsBetween100000And500000=0

    try:        
        df_filtre = data[(data['Montant'].abs() >= 10000) & (data['Montant'].abs() <= 100000)]
        transactionsBetween10000And100000 = df_filtre.shape[0]
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.transactionsBetween10000And100000] {e}")
        transactionsBetween10000And100000=0


    try:        
        df_filtre = data[(data['Montant'].abs() > 500000)]
        transactionsGreater500000 = df_filtre.shape[0]
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.transactionsGreater500000] {e}")
        transactionsGreater500000=0

    recurringExpense = []
    try:
        #liste = CATEGORY["reccuring"]
        #liste = [x.lower() for x in liste]
        liste = liste_reccuring_trans
        print("liste_reccuring_trans", liste)
        data['categorisation_temp'] = data[column_rec].str.lower()
        data_reccuring=data.loc[data['categorisation_temp'].str.contains('|'.join(liste))]
        data_debit = data_reccuring.loc[data_reccuring['Montant'] < 0]

        top3 = process_top_3(data_debit, "TransTypeDet")
        if len(top3) == 0:
            top3 = process_top_3(data_debit, column_rec)
        recurringExpense = top3
    except Exception as e:
        print(f"[ERROR][utils.utils.calcul_score.recurringExpense] {e}")
        recurringExpense=[]


    dict_output = {
                "credit_debit" : credit_debit,
                "nbTotalCreditTurnover" : nbTotalCreditTurnover,
                "nbTotalDebitTurnover" : nbTotalDebitTurnover,
                "nbAverageMonthlyCredit" : nbAverageMonthlyCredit,    
                "nbAverageMonthlyDebit" : nbAverageMonthlyDebit,
                "nb_month_activity" : nb_month_activity,
                "accountActivity":accountActivity,
                "averageBalance":averageBalance,
                "averageCredits":averageCredits,
                "averageDebits":averageDebits,
                "initialBalance" : initialBalance,
                "closingBalance":closingBalance,
                "firstDay":firstDay,
                "lastDay":lastDay,
                "monthPeriod":monthPeriod,
                "noOfTransactingMonths":noOfTransactingMonths,
                "totalCreditTurnover":totalCreditTurnover,
                "totalDebitTurnover":totalDebitTurnover,
                "yearInStatement":yearInStatement,
                "netAverageMonthlyEarnings": netAverageMonthlyEarnings,
                "airtime":airtime,
                "totalExpenses":totalExpenses,
                "webSpend":webSpend,
                "bundleSpend":bundle,
                "entertainment": entertainment,
                "inflowOutflowRate":inflowOutflowRate,
                "topIncomingTransferAccount":topIncomingTransferAccount,
                "topTransferRecipientAccount":topTransferRecipientAccount,
                "lastDateOfCredit":lastDateOfCredit,
                "lastDateOfDebit":lastDateOfDebit,
                "averageOtherIncome" : averageOtherIncome,
                "averageSalary" : averageSalary,
                "netCashFlow" : netCashFlow,
                "MaxMonthlyRepayment" : MaxMonthlyRepayment,
                "averageMonthlyCredit" : averageMonthlyCredit,
                "averageMonthlyDebit" : averageMonthlyDebit,
                "totalMonthlyCredit" : totalMonthlyCredit,
                "noOfTransactingDays" : noOfTransactingDays,
                "dayPeriode" : dayPeriode,
                "confidenceIntervalOnSalaryDetection" : confidenceIntervalOnSalaryDetection,
                "expectedSalaryDay" : expectedSalaryDay,
                "lastSalaryDate" : lastSalaryDate,
                "medianIncome" : medianIncome,
                "numberOtherIncomePayments" : numberOtherIncomePayments,
                "numberOfSalaryPayments" : numberOfSalaryPayments,
                "salaryEarner" : salaryEarner,
                "salaryFrequency" : salaryFrequency,
                "averageRecurringExpense" : averageRecurringExpense,
                "bankCharges" : bankCharges,
                "bills": bills,
                "gambling" : gambling,
                "dab": dab,
                "hasRecurringExpense" : hasRecurringExpense,
                "internationalTransactionsSpend" : internationalTransactionsSpend,
                "posSpend" : posSpend,
                "spendOnTransfers": spendOnTransfers,
                "ussdTransactions" : ussdTransactions,
                "accountSweep" : accountSweep,
                "gamblingRate" : gamblingRate,
                "loanAmount" : loanAmount,
                "loanInflowRate" : loanInflowRate,
                "loanRepaymentInflowRate" : loanRepaymentInflowRate,
                "loanRepayments" : loanRepayments,
                "highestMAWOCredit" : highestMAWOCredit,
                "highestMAWODebit": highestMAWODebit,
                "MAWWZeroBalanceInAccount" : MAWWZeroBalanceInAccount,
                "mostFrequentBalanceRange" : mostFrequentBalanceRange,
                "mostFrequentTransactionRange" : mostFrequentTransactionRange,
                "NODWBalanceLess5000" : NODWBalanceLess5000,
                "recurringExpense" : recurringExpense,
                "transactionsBetween100000And500000" : transactionsBetween100000And500000,
                "transactionsBetween10000And100000" : transactionsBetween10000And100000,
                "transactionsGreater500000" : transactionsGreater500000
                }

    # transformation pd.NA en None pourque FastAPI puisse le serealiser
    for cle, valeur in dict_output.items():
        if cle != "recurringExpense":
            try:
                if pd.isna(valeur):
                    dict_output[cle] = None
                if isinstance(valeur, np.uint32):
                    dict_output[cle] = int(valeur)
                    if np.isinf(valeur):
                        dict_output[cle] = None
                if isinstance(valeur, np.int64):
                    dict_output[cle] = int(valeur)
                    if np.isinf(valeur):
                        dict_output[cle] = None
                if isinstance(valeur, np.float64):
                    if np.isinf(valeur):
                        dict_output[cle] = None
                    dict_output[cle] = float(valeur)
                if isinstance(valeur, np.float32):
                    if np.isinf(valeur):
                        dict_output[cle] = None
                    dict_output[cle] = float(valeur)
                if isinstance(valeur, float):
                     if valeur == float('inf') or valeur == float('-inf') or math.isnan(valeur):
                        dict_output[cle] = None
            except:
                pass 

    print(dict_output)
    tps2 = time.time()
    print(str((tps2 - tps1)/1000)+" ms")

    return dict_output

    
    
    
def download_pdf(url, bank, code, save_directory="data"):
    print(url, bank, code, save_directory)
    # Vérifier si le répertoire de sauvegarde existe, sinon le créer
    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    # Extraire le nom du fichier du lien
    file_name = url.split("/")[-1]

    # Chemin complet du fichier local
    local_path = os.path.join(save_directory, file_name)
    print("local_path ==> ",local_path)
    
    """
    try:
        # Télécharger le fichier
        response = requests.get(url)
        with open(local_path, 'wb') as file:
            file.write(response.content)
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")
    """
    
    try:
        with urllib.request.urlopen(url) as response, open(local_path, 'wb') as file:
            file.write(response.read())
        print("Téléchargement réussi.")
    except Exception as e:
        print(f"Erreur lors du téléchargement : {e}")
    try:
        if bank== "ACCESS BANK":
            return access_bank_pdf_reader(local_path, code), "success_password"
        if bank== "FIRST BANK":
            return first_bank_pdf_reader(local_path, code), "success_password"
        if bank== "STERLING BANK":
            return sterling_bank_pdf_reader(local_path, code), "success_password"
        if bank== "WEMA BANK":
            return wema_bank_pdf_reader(local_path, code), "success_password"
        if bank== "UNION BANK":
            return union_bank_pdf_reader(local_path, code), "success_password"
        if bank== "FIRST CITY MUNAMENT BANK":
            return fcmb_bank_pdf_reader(local_path, code), "success_password"
        if bank== "ZENITH BANK":
            return zenith_bank_pdf_reader(local_path, code), "success_password"
        if bank== "ECO BANK":
            return eco_bank_pdf_reader(local_path, code), "success_password"
        return pd.DataFrame(), "success_password"
    except Exception as e:
        if "password is incorrect" in str(e).lower():
            return pd.DataFrame(), "error_password"
        else:
            return pd.DataFrame(), "success_password"


def download_csv(url):
    try:
        response = requests.get(url)
        data = StringIO(response.text)
        df = read_data(data)
        return df
    except Exception as e:
        print(f"[ERROR][utils.utils.download_csv] {e}")
        return pd.DataFrame()


def main_process(input_json):
    
    files = get_files(input_json, domain=CONFIG_SECURITY["FILEHOST"])
    data_frames = []
    ext = ""
    bank = ""
    employer = []
    status_passwword = ""
    print("files ===> ",files)
    try:
        for file in files:
            ext = str(file["ext"]) if ext == "" else ext
            bank = str(file["bank"]) if bank == "" else bank
            url = str(file["baseUrl"])+str(file["path"])
            try:
                for emp in file["employer"]: 
                    if emp is not None and emp != "":
                        employer.append(emp)
            except:
                pass 
            if str(file["ext"]) == ext and str(file["ext"]) == "csv" and str(file["bank"]) == bank:
                df = download_csv(url)
                if  not df.empty:
                    data_frames.append(df)
            elif str(file["ext"]) == ext and str(file["ext"]) == "pdf" and str(file["bank"]) == bank:
                df, status_pwd = download_pdf(url, str(file["bank"]), str(file["code"]))
                if status_passwword == "":
                    status_passwword = status_pwd
                if  not df.empty:
                    data_frames.append(df)

    except Exception as e:
        print(f"[ERROR][utils.utils.main_process.get_files] {e}")
    try:
        final_df = pd.concat(data_frames, axis=0, ignore_index=True)
    except Exception as e:
        print(f"[ERROR][utils.utils.main_process.concat_dataframe] {e}")
        final_df = pd.DataFrame()

    print("bank ===>", bank, "** status_passwword ==>",status_passwword)
    if status_passwword == "error_password":
        print("mot de passe incorrect")
        return add_status_output({}, "errors", [{'code':'WRONG_PASSWORD'}])
    
    
    if bank == "MOOV":
        status, errors = file_validation(final_df)
        out = calcul_score(final_df)
        return add_status_output(out, status, errors)
    
    elif bank == "MTN":
        status, errors = file_validation(final_df)
        print("status, errors ==>",status, errors)
        out = calcul_score(final_df)
        return add_status_output(out, status, errors)

    elif bank == "ACCESS BANK":
        data = standardize_column_name(final_df, ACCESS_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date", list_formats=[]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = access_bank_transaction_labelisation(data, employer)
        data = access_bank_from_to(data)
        out = access_bank_calcul_score(data)
        return add_status_output(out, status, errors)

    elif bank == "FIRST BANK":
        data = standardize_column_name(final_df, FIRST_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date", list_formats=[]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = first_bank_transaction_labelisation(data, employer)
        data = first_bank_from_to(data)
        out = first_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank == "STERLING BANK":
        data = standardize_column_name(final_df, STERLING_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date", list_formats=[]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = sterling_bank_transaction_labelisation(data, employer)
        data = sterling_bank_from_to(data)
        out = sterling_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "WEMA BANK":
        data = standardize_column_name(final_df, WEMA_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = wema_bank_parse_date(data, "Date", list_formats=[]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = wema_bank_transaction_labelisation(data, employer)
        data = wema_bank_from_to(data)
        out = wema_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "UNION BANK":
        data = standardize_column_name(final_df, UNION_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = union_parse_date(data, "Date", list_formats=["%d/%m/%Y"]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = union_bank_transaction_labelisation(data, employer)
        data = union_bank_from_to(data)
        out = union_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank== "FIRST CITY MUNAMENT BANK":
        data = standardize_column_name(final_df, FCMB_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = fcmb_parse_date(data, "Date", list_formats=["%d-%b-%Y"]) # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = fcmb_bank_transaction_labelisation(data, employer)
        data = fcmb_bank_from_to(data)
        out = fcmb_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank == "ZENITH BANK":
        data = standardize_column_name(final_df, ZENITH_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date") # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = zenith_bank_transaction_labelisation(data,employer)
        data = zenith_bank_from_to(data, categories=LABELISATION)
        out = zenith_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank == "ECO BANK":
        data = standardize_column_name(final_df, ECO_BANK_COLUMNS_MAPPING) # rename column name as standard name
        data = parse_date(data, "Date") # create new column Datetime from column Date
        data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
        status, errors = file_validation(data)
        data = eco_bank_transaction_labelisation(data,employer)
        data = eco_bank_from_to(data, categories=LABELISATION)
        out= eco_bank_calcul_score(data)
        return add_status_output(out, status, errors)
    elif bank == "" or bank is None:
        out = calcul_score(final_df)        
        return add_status_output(out, "errors", [{'code':'UNKNOWN_BANK'}])

