import pandas as pd
import time
import re
from tabula import read_pdf
from utils.standard_func import parse_date, standardize_column_name
from utils.standard_func import montant_validation
from utils.standard_func import process_account_activity
from utils.standard_func import process_average_balance
from utils.standard_func import process_average_credit
from utils.standard_func import process_average_debit
from utils.standard_func import process_closing_balance
from utils.standard_func import process_month_period
from utils.standard_func import process_net_average_monthly_earnings
from utils.standard_func import process_year_in_statement
from utils.standard_func import process_credit_debit
from utils.standard_func import process_total_debit_turnover
from utils.standard_func import process_nb_total_debit_turnover
from utils.standard_func import process_nb_total_credit_turnover
from utils.standard_func import process_total_credit_turover
from utils.standard_func import process_number_of_transacting_month
from utils.standard_func import process_max_monthly_repayement
from utils.standard_func import process_total_monthly_credit_debit
from utils.standard_func import process_average_other_income
from utils.standard_func import process_average_salary
from utils.standard_func import process_confidence_interval
from utils.standard_func import process_salary_day
from utils.standard_func import process_last_salary_date
from utils.standard_func import process_spend
from utils.standard_func import process_reccuring_expense
from utils.standard_func import process_account_sweep
from utils.standard_func import process_gambling_rate
from utils.standard_func import process_inflow_outflow_rate
from utils.standard_func import process_loan
from utils.standard_func import process_top_transfert_account
from utils.standard_func import process_high
from utils.standard_func import process_last_date
from utils.standard_func import process_balance
from utils.standard_func import process_frequent_transaction_range
from utils.standard_func import process_transaction
from utils.standard_func import process_transaction_range
from utils.standard_func import process_total_expense
from utils.standard_func import process_median_income
from utils.standard_func import serialize_dict
from utils.standard_func import add_employer_labelisation
from utils.standard_func import del_employer_labelisation

# Chemin vers le fichier PDF
pdf_path = 'FIRST BANK-3815.pdf'

# Mot de passe du fichier PDF
password = '3815'


COLUMNS_MAPPING = {
            "Date" : ["Tran Date"],
            "TransDet" : ["Narration"],
            "Debit" : ["Debit"],
            "Credit" : ["Credit"],
            "Solde" : ["Balance"]
        }


LABELISATION = {
            "transfert" : {"keywords":["trfcheersfrm","transfer","trf", "trsf", "tfr", "nip", "transf", "etz", 'trffrm', 'trftransferfrm', 'tran', 'fip', 'from', 'frm'],"montant" : "all"},
            "atm-spend" : {"keywords":["atm"],"montant" : "negative"},
            "web-spend" : {"keywords":["flutter wave", "monnify", "paystack", "interswitch", "web buy", "vervecard"],"montant" : "negative"},
            "pos-spend" : {"keywords":["pos", "baxi"],"montant" : "negative"},
            "ussd-spend" : {"keywords":["ussd","901", "qs894"],"montant" : "negative"},
            "mobile-spend" : {"keywords":["mobile"],"montant" : "negative"},
            "spend-on-transfert" : {"keywords":["transfer","trf", "trsf", "tfr", "nip", "transf", "etz", 'trffrm', 'trftransferfrm', 'tran', 'fip', 'from', 'frm'],"montant" : "negative"},
            "international-spend" : {"keywords":["international", "visa fee", "school fees", "tuition", "travel consultancy"],"montant" : "negative"},
            "bills-spend" : {"keywords":["bills", "ikeja", "afm", "nepa", "ikeja electric", "lawma", "eko electric", "netflix"],"montant" : "negative"},
            "entertainment-spend" : {"keywords":["canal","cinema", "concert", "events", "shows", "refreshments", "clubs", "bars"],"montant" : "negative"},
            "waste-spend" : {"keywords":["waste", "garbage", "disposal", "recycling", "cleanup"],"montant" : "negative"},
            "water-spend" : {"keywords":["water"],"montant" : "negative"},
            "electricity-spend" : {"keywords":["electricity"],"montant" : "negative"},
            "savingsinvestments-spend" : {"keywords":["fof", "funds of funds", "cowry wise", "piggyvest", "carbon", "investment one", "kuda", "alat", "get equity", "bamboo"],"montant" : "negative"},
            "gambling-spend" : {"keywords":["gambling", "gamble", "mssport", "betking", "1xbet", "bet9ja", "22bet", "betway", "sportybet", "bet winner", "nairabet", "netbet", "naijabet", "msport"],"montant" : "negative"},
            "airtime-spend" : {"keywords":["airtime"],"montant" : "negative"},
            "bankcharges-spend" : {"keywords":["charge", "levy", "debitsessioncharge", "vat", "insurance"],"montant" : "negative"},
            "bundle-spend" : {"keywords":["bundle"],"montant" : "negative"},
            "loan" : {"keywords":["dsbrsal", "disbursement", "loan"],"montant" : "positive"},
            "loan-rpymnt" : {"keywords":["intrst","rpymnt", "penalty", "repayment", "credit", "payment"],"montant" : "negative"},
            "other-income" : {"keywords":["commision", "bonuses"],"montant" : "positive"},
            "slr-earner" : {"keywords":["salary", "commision", "bonuses"],"montant" : "positive"},
            "salary" : {"keywords":["salary"],"montant" : "positive"}
        }

KEYWORDS = ['debitsession' ,'topup', 'qtm', 'hbr', 'ustmtn', 'psd', 'fgn', 
            'nxg', 'fee', 'alert', 'sms', 'sac', 'cwu', 'wdl', "hyd", 
            "airtime", "topup", "qtm","ustmtn", "ust", "mtn",
            "nnl", "fip", "mb", "gtb", ":", "charges", "withdrawal", "fbn", 
            "withdrawal-fbn", "plp", "mmb", "nib", "acc", "cdl", "uba", "zib",
            "ceva", "abr", "pcm", "ubn", "kmb"]


def first_bank_pdf_reader(pdf_path, password):

    # Initialiser une liste pour stocker les DataFrames de chaque page
    data_frames = []

    # Utiliser tabula pour extraire les DataFrames de chaque page
    all_pages = read_pdf(pdf_path,password = password,pages="all",multiple_tables=True,lattice=True)

    # Boucler sur chaque DataFrame de page
    for page_number, df in enumerate(all_pages[2:], start=1):
        
        # Ajouter le DataFrame à la liste
        data_frames.append(df)

    # Concaténer tous les DataFrames en un seul
    final_df = pd.concat(data_frames, axis=0, ignore_index=True)
    return final_df


def first_bank_classify_transaction(row, categories):
    default = 'cashin' if row['Montant'] > 0 else 'cashout'
    labelisation = ""

    for label, details in categories.items():
        keywords = details["keywords"]
        montant_condition = details["montant"]
        
        if label == "salary":
            if any((keyword.lower()) in row['TransDet'].lower() for keyword in keywords):
                if montant_condition == "all":
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "negative" and row['Montant'] < 0:
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "positive" and row['Montant'] >= 0:
                    labelisation += "|" + label if labelisation else label
        else: 
            if any((" " + keyword.lower()) in row['TransDet'].lower() or (keyword.lower() + " ") in row['TransDet'].lower() 
                   or ("/" + keyword.lower()) in row['TransDet'].lower() or (keyword.lower() + "/") in row['TransDet'].lower()
                   or ("-" + keyword.lower()) in row['TransDet'].lower() or (keyword.lower() + "-") in row['TransDet'].lower() 
                   or (":" + keyword.lower()) in row['TransDet'].lower() or (keyword.lower() + ":") in row['TransDet'].lower()
                   or ("|" + keyword.lower()) in row['TransDet'].lower() or (keyword.lower() + "|") in row['TransDet'].lower()
                   or ("_" + keyword.lower()) in row['TransDet'].lower() or 
                   (keyword.lower() + "_") in row['TransDet'].lower() for keyword in keywords):
                if montant_condition == "all":
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "negative" and row['Montant'] < 0:
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "positive" and row['Montant'] >= 0:
                    labelisation += "|" + label if labelisation else label

        """
        if label == "loan" or label == "loan-rpymnt":
            if any((keyword.lower()) in row['TransDet'].lower() for keyword in keywords):
                if montant_condition == "all":
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "negative" and row['Montant'] < 0:
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "positive" and row['Montant'] >= 0:
                    labelisation += "|" + label if labelisation else label
                print("loan===>")
        else: 
            if any((" " + keyword.lower()) in row['TransDet'].lower() or
                   (keyword.lower() + " ") in row['TransDet'].lower() or ("_" + keyword.lower()) in row['TransDet'].lower() or 
                   (keyword.lower() + "_") in row['TransDet'].lower() for keyword in keywords):
                if montant_condition == "all":
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "negative" and row['Montant'] < 0:
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "positive" and row['Montant'] >= 0:
                    labelisation += "|" + label if labelisation else label
        """
    return labelisation if labelisation else default


def first_bank_transaction_labelisation(data, new_key_salary="", categories=LABELISATION):
    try:
        categories = add_employer_labelisation(categories, new_key_salary)
        data['TransDetOrigin'] = data['TransDet'].copy()
        data['TransDetOrigin'] = data['TransDetOrigin'].str.lower()
        
        # preprocess replace char speci to ' '
        caracteres_speciaux = ['.', ':', '|', '-', '_', '\r', '\n','*']
        for char in caracteres_speciaux:
            data['TransDet'] = data['TransDet'].str.replace(char, ' ')
        # process labelisation
        data['LABEL'] = data.apply(lambda row: first_bank_classify_transaction(row, categories), axis=1)
        categories = del_employer_labelisation(categories)
        return data
    except Exception as e:
        print(f"[ERROR][utils.process_first_bank.labelisation] {e}")
        data['LABEL'] = ""
        return data


def extract_from_and_to(text, all_keywords):
    from_text = ""
    to_text = ""
    text = text.replace('\n', ' ').replace('\r', ' ')
    
    # FIP:MB:(name of the bank)/name of the platform/
    if "fip:mb:" in text: # on remarque que c'est toujours une transaction de debit    
        list_words = text.split('/')
        if len(list_words) > 2:
            to_text = list_words[2]
        elif len(list_words) > 0:
            to_text = list_words[1]
    # FIP/ZIB/name of the sender
    if "fip:zib" in text: # on remarque que c'est toujours une transaction de credit
        list_words = text.split('/')
        if len(list_words) > 1:
            from_text = list_words[1]
    # ETZ (ref number of transaction) NIP or TRFFROM (name or ref number of the sender)
    if "etz:" in text: # on remarque que c'est toujours une transaction de credit
        found = False
        for key in ['from', 'frm', 'trffrm', 'trftransferfrm']:
            if found == False:
                list_words = text.split(key)
                if len(list_words) > 1:
                    from_text = list_words[1]
                    found = True
    # FIP:USSD:ACC/name of the receiver
    if "fip:ussd:acc" in text: # on remarque que c'est toujours une transaction de debit
            list_words = text.split('/')
            if len(list_words) > 1:
                to_text = list_words[1]
    # CEVA:ABR:TRF/name of the sender/name of the receiver  
    if "ceva:abr:trf" in text: # on remarque que c'est toujours une transaction de credit
            list_words = text.split('/')
            if len(list_words) > 2:
                from_text = list_words[1]
                to_text = list_words[2]
    # FIP:GTB/name of sender or name of plateform
    if "fip:gtb" in text: # on remarque que c'est toujours une transaction de credit
            list_words = text.split('/')
            if len(list_words) > 1:
                from_text = list_words[1]
    # FIP:NIB/name of the POS platform/name of sender
    if "fip:nib:" in text: # on remarque que c'est toujours une transaction de credit 
        list_words = text.split('/')
        if len(list_words) > 2:
            from_text = list_words[2]
        elif len(list_words) > 0:
            from_text = list_words[1]

    # POS Tran (name of the platform you send the money from)
    if "pos tran" in text: # on remarque que c'est toujours une transaction de debit
            list_words = text.split('-')
            if len(list_words) > 1:
                to_text = list_words[1]
    # FIP:PLP/Palmpay/(Name of the sender)
    if "fip:plp" in text: # on remarque que c'est toujours une transaction de credit 
        list_words = text.split('/')
        if len(list_words) > 2:
            from_text = list_words[2]

    # FIP/PCM/(name of the sender)/Transfer from to (name of the receiver)
    if "fip:pcm" in text: # on remarque que c'est toujours une transaction de credit 
        list_words = text.split('/')
        if len(list_words) > 2:
            from_text = list_words[1]
            to_text = list_words[2].replace('transfer from to', '')
    
    # FIP:MMB/(name of the platform)_TRSF_(ref number)
    if "fip:mmb" in text: # on remarque que c'est toujours une transaction de credit 
        list_words = text.split('/')
        if len(list_words) > 1:
            from_text = list_words[1]

    # FIP:MB:ACC/(name of the receiver) 
    if "fip:mb:acc" in text: # on remarque que c'est toujours une transaction de debit
            list_words = text.split('/')
            if len(list_words) > 1:
                to_text = list_words[1]

    # FIP:FID/name of the sender
    if "fip:mmb" in text: # on remarque que c'est toujours une transaction de credit 
        list_words = text.split('/')
        if len(list_words) > 1:
            from_text = list_words[1]

    # FIP:FID/name of the sender
    if "fip:/" in text: # on remarque que c'est toujours une transaction de credit 
        list_words = text.split('/')
        if len(list_words) > 1:
            from_text = list_words[1]


    from_text = ' '.join([word for word in from_text.split() if word not in all_keywords])
    to_text = ' '.join([word for word in to_text.split() if word not in all_keywords])

    # if simple transfert trf name od sender / name of receiver    
    if "/" in text: 
        words = text.lower().split("/")
        words_candidate = []
        if len(words) > 0:
            for w in words:
                if w not in all_keywords: 
                    for key in all_keywords:
                        w=str(w).lower().replace('\r', ' ')
                        w = str(w).lower().replace(key, '').strip()
                    if w is not None and w != '' and len(w) > 3:
                        words_candidate.append(w)
        
        if from_text == "":
            if len(words_candidate) > 0:
                from_text = words_candidate[0]
            else:
                from_text = ""
            
        if to_text == "":
            if len(words_candidate) > 1:
                to_text = words_candidate[-1]
            else:
                to_text = ""

    from_text, to_text = from_text.strip(), to_text.strip()

    return from_text, to_text


def first_bank_from_to(data, categories=LABELISATION, KEYWORDS=KEYWORDS):
    try:
        all_keywords = [keyword.lower() for details in categories.values() for keyword in details["keywords"]]
        # Récupérer les mots-clés de la catégorie "salary" qui  sont inserer via front employer-name
        salary_keywords = [keyword.lower() for keyword in LABELISATION["salary"]["keywords"] if keyword != "salary"]

        # Filtrer les mots-clés pour enlever ceux sont inserer via front employer-name
        all_keywords = [keyword for keyword in all_keywords if keyword not in salary_keywords]
        all_keywords = all_keywords + KEYWORDS
        data[['FROM', 'TO']] = data['TransDetOrigin'].apply(lambda x: extract_from_and_to(x, all_keywords)).apply(pd.Series)
        return data
    except Exception as e:
        print(f"[ERROR][utils.process_first_bank.first_bank_from_to] {e}")
        data['FROM'] = ""
        data['TO'] = ""
        return data


def first_bank_calcul_score(data):
    tps1 = time.time()
    accountActivity =  process_account_activity(data)
    averageBalance = process_average_balance(data)
    averageCredits = process_average_credit(data)
    averageDebits = process_average_debit(data)
    closingBalance, initialBalance = process_closing_balance(data)
    firstDay,lastDay, monthPeriod = process_month_period(data)
    totalCreditTurnover = process_total_credit_turover(data)
    totalDebitTurnover = process_total_debit_turnover(data)
    nbTotalCreditTurnover = process_nb_total_credit_turnover(data)
    nbTotalDebitTurnover = process_nb_total_debit_turnover(data)
    noOfTransactingMonths, noOfTransactingDays, dayPeriode = process_number_of_transacting_month(data)
    credit_debit = process_credit_debit(totalCreditTurnover, totalDebitTurnover)
    yearInStatement = process_year_in_statement(data)
    averageOtherIncome, numberOtherIncomePayments = process_average_other_income(data)
    averageSalary, numberOfSalaryPayments, salaryEarner, salaryFrequency = process_average_salary(data)
    confidenceIntervalOnSalaryDetection = process_confidence_interval(data, averageSalary)
    expectedSalaryDay = process_salary_day(data)
    lastSalaryDate = process_last_salary_date(data) 
    medianIncome = process_median_income(data)
    dab = process_spend(data, "atm-spend", noOfTransactingMonths)
    webSpend = process_spend(data, "web-spend", noOfTransactingMonths)
    posSpend = process_spend(data, "pos-spend", noOfTransactingMonths)
    ussdTransactions = process_spend(data, "ussd-spend", noOfTransactingMonths)
    mobileSpend = process_spend(data, "mobile-spend", noOfTransactingMonths)
    spendOnTransfers = process_spend(data, "spend-on-transfert", noOfTransactingMonths)
    internationalTransactionsSpend = process_spend(data, "international-spend", noOfTransactingMonths)
    bills = process_spend(data, "bills-spend", noOfTransactingMonths)
    entertainment = process_spend(data, "entertainment-spend", noOfTransactingMonths)
    savingsAndInvestments = process_spend(data, "savingsinvestments-spend", noOfTransactingMonths)
    gambling = process_spend(data, "gambling-spend", noOfTransactingMonths)
    airtime = process_spend(data, "airtime-spend", noOfTransactingMonths)
    bankCharges = process_spend(data, "bankcharges-spend", noOfTransactingMonths)
    bundle = process_spend(data, "bundle-spend", noOfTransactingMonths)
    averageRecurringExpense, hasRecurringExpense, recurringExpense = process_reccuring_expense(data, monthPeriod, LABELISATION["bills-spend"]["keywords"])
    totalExpenses = process_total_expense(data, noOfTransactingMonths)
    netAverageMonthlyEarnings = process_net_average_monthly_earnings(data, averageOtherIncome, averageSalary, totalExpenses)
    averageMonthlyCredit, averageMonthlyDebit, nbAverageMonthlyCredit, nbAverageMonthlyDebit, netCashFlow, MaxMonthlyRepayment = process_max_monthly_repayement(data)
    totalMonthlyCredit, totalMonthlyDebit = process_total_monthly_credit_debit(data)
    accountSweep = process_account_sweep(data)
    gamblingRate = process_gambling_rate(data)
    inflowOutflowRate = process_inflow_outflow_rate(data)
    loanAmount, loanInflowRate, loanRepaymentInflowRate, loanRepayments = process_loan(data)
    topIncomingTransferAccount, topTransferRecipientAccount = process_top_transfert_account(data)
    highestMAWOCredit , highestMAWODebit = process_high(data)
    lastDateOfCredit, lastDateOfDebit = process_last_date(data)
    MAWWZeroBalanceInAccount, mostFrequentBalanceRange, NODWBalanceLess5000 = process_balance(data)
    mostFrequentTransactionRange = process_frequent_transaction_range(data)
    transactionsBetween100000And500000, transactionsBetween10000And100000, transactionsGreater500000, transactionsLess10000 = process_transaction(data)
    transactionRanges = process_transaction_range(data)
    
    dict_output = {
                "credit_debit" : credit_debit,
                "nbTotalCreditTurnover" : nbTotalCreditTurnover,
                "nbTotalDebitTurnover" : nbTotalDebitTurnover,
                "nbAverageMonthlyCredit" : nbAverageMonthlyCredit,    
                "nbAverageMonthlyDebit" : nbAverageMonthlyDebit,
                "nb_month_activity" : noOfTransactingMonths,
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
                "savingsAndInvestments" : savingsAndInvestments,
                "mobileSpend" : mobileSpend,
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
                "totalMonthlyDebit" : totalMonthlyDebit,
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
                "transactionsGreater500000" : transactionsGreater500000,
                "transactionRanges" : transactionRanges
                }


    dict_output = serialize_dict(dict_output)
    print(dict_output)
    
    tps2 = time.time()
    print(str((tps2 - tps1)/1000)+" ms")
    return dict_output

"""
if __name__ == '__main__':
    
    data = first_bank_pdf_reader(pdf_path, password)    # pdf reader
    print(data.columns)
    data = standardize_column_name(data, COLUMNS_MAPPING) # rename column name as standard name

    data = parse_date(data, "Date") # create new column Datetime from column Date

    data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
    
    data = first_bank_transaction_labelisation(data)

    data = first_bank_from_to(data, categories=LABELISATION)

    print(data[["FROM", "TO"]])
    first_bank_calcul_score(data)

"""