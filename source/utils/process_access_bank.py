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
pdf_path = 'ACCESS BANK-6521.pdf'

# Mot de passe du fichier PDF
password = '6521'

# Chemin vers le fichier PDF
pdf_path = 'ACCESS BANK GAMBLE.pdf'

# Mot de passe du fichier PDF
password = ''


COLUMNS_MAPPING = {
            "Date" : ["Tran. date", "Date", "DATE"],
            "TransDet" : ["Transaction details", "TRANSACTION"],
            "Debit" : ["Debit", "Withdrawals", "WITHDRAWALS"],
            "Credit" : ["Credit", "Lodgements", "LODGEMENTS"],
            "Solde" : ["Balance", "BALANCE"],
            "Reference" : ["Reference"]
        }


LABELISATION = {
            "transfert" : {"keywords":["trfcheersfrm","transfer","trf", "trsf", "tfr", "nip", "transf"],"montant" : "all"},
            "atm-spend" : {"keywords":["atm"],"montant" : "negative"},
            "web-spend" : {"keywords":["flutter wave", "monnify", "paystack", "interswitch", "web buy", "vervecard"],"montant" : "negative"},
            "pos-spend" : {"keywords":["pos"],"montant" : "negative"},
            "ussd-spend" : {"keywords":["ussd","901", "qs894"],"montant" : "negative"},
            "mobile-spend" : {"keywords":["mobile"],"montant" : "negative"},
            "spend-on-transfert" : {"keywords":["transfer","trf", "trsf", "tfr", "nip", "transf"],"montant" : "negative"},
            "international-spend" : {"keywords":["international", "visa fee", "school fees", "tuition", "travel consultancy"],"montant" : "negative"},
            "bills-spend" : {"keywords":["bills", "ikeja", "afm", "nepa", "ikeja electric", "lawma", "eko electric", "netflix"],"montant" : "negative"},
            "entertainment-spend" : {"keywords":["canal","cinema", "concert", "events", "shows", "refreshments", "clubs", "bars"],"montant" : "negative"},
            "waste-spend" : {"keywords":["waste", "garbage", "disposal", "recycling", "cleanup"],"montant" : "negative"},
            "water-spend" : {"keywords":["water"],"montant" : "negative"},
            "electricity-spend" : {"keywords":["electricity"],"montant" : "negative"},
            "savingsinvestments-spend" : {"keywords":["fof", "funds of funds", "cowry wise", "piggyvest", "carbon", "investment one", "kuda", "alat", "get equity", "bamboo"],"montant" : "negative"},
            "gambling-spend" : {"keywords":["gambling",  "gamble", "mssport", "betking", "1xbet", "bet9ja", "22bet", "betway", "sportybet", "bet winner", "nairabet", "netbet", "naijabet", "msport"],"montant" : "negative"},
            "airtime-spend" : {"keywords":["airtime"],"montant" : "negative"},
            "bankcharges-spend" : {"keywords":["charge", "levy", "debitsessioncharge","vat", "insurance"],"montant" : "negative"},
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


def access_bank_gamble_pdf_reader(pdf_path, password):
    if password is not None and password != '':
        # Utiliser tabula pour extraire les DataFrames de chaque page
        all_pages = read_pdf(pdf_path,password = password, pages="all", multiple_tables=True, guess=False, lattice=True)
    else:
        all_pages = read_pdf(pdf_path, pages="all", multiple_tables=True, guess=False, lattice=True)
    
    # Initialiser une liste pour stocker les DataFrames de chaque page
    data_frames = []

    # Boucler sur chaque DataFrame de page
    for df in all_pages:
        # Supprimer les colonnes avec des valeurs NaN
        df = df.dropna(axis=1, how='all')
        print(df.columns)
        # Renommer les colonnes avec des noms spécifiques
        df.columns = ['Tran. date', 'Transaction details', 'Reference', 'Value Date', 'Debit', 'Credit', 'Balance'][:df.shape[1]]
        
        # Ajouter le DataFrame à la liste
        data_frames.append(df)

    # Concaténer tous les DataFrames en un seul
    final_df = pd.concat(data_frames, axis=0, ignore_index=True)
    final_df['DATE'] = final_df['Tran. date']
    # Convertir la première colonne en format datetime et supprimer les lignes non convertibles
    final_df['DATE'] = pd.to_datetime(final_df['DATE'], errors='coerce')
    final_df = final_df[final_df['DATE'].notna()]

    # Réindexer le DataFrame après la suppression des lignes
    final_df = final_df.reset_index(drop=True)
    return final_df


def access_bank_pdf_reader(pdf_path, password):

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

    if all(colonne not in df.columns for colonne in ["Tran. date", "Date"]):
        return access_bank_gamble_pdf_reader(pdf_path, password)

    return final_df





def access_bank_classify_transaction(row, categories):
    default = 'cashin' if row['Montant'] > 0 else 'cashout'
    labelisation = ""

    for label, details in categories.items():
        keywords = details["keywords"]
        montant_condition = details["montant"]
        if label == "salary":
            if any((keyword.lower()) in row['TransDetLabel'].lower() for keyword in keywords):
                if montant_condition == "all":
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "negative" and row['Montant'] < 0:
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "positive" and row['Montant'] >= 0:
                    labelisation += "|" + label if labelisation else label
        else: 
            if any((" " + keyword.lower()) in row['TransDetLabel'].lower() or (keyword.lower() + " ") in row['TransDetLabel'].lower() 
                   or ("/" + keyword.lower()) in row['TransDetLabel'].lower() or (keyword.lower() + "/") in row['TransDetLabel'].lower()
                   or ("-" + keyword.lower()) in row['TransDetLabel'].lower() or (keyword.lower() + "-") in row['TransDetLabel'].lower() 
                   or (":" + keyword.lower()) in row['TransDetLabel'].lower() or (keyword.lower() + ":") in row['TransDetLabel'].lower()
                   or ("|" + keyword.lower()) in row['TransDetLabel'].lower() or (keyword.lower() + "|") in row['TransDetLabel'].lower()
                   or ("_" + keyword.lower()) in row['TransDetLabel'].lower() or 
                   (keyword.lower() + "_") in row['TransDetLabel'].lower() for keyword in keywords):
                if montant_condition == "all":
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "negative" and row['Montant'] < 0:
                    labelisation += "|" + label if labelisation else label
                elif montant_condition == "positive" and row['Montant'] >= 0:
                    labelisation += "|" + label if labelisation else label
        """
        if any((" " + keyword.lower()) in str(row['TransDetLabel']).lower() or
               (keyword.lower() + " ") in str(row['TransDetLabel']).lower() or ("/" + keyword.lower()) in str(row['TransDetLabel']).lower() or
               (keyword.lower() + "/") in str(row['TransDetLabel']).lower()   for keyword in keywords):
            if montant_condition == "all":
                labelisation += "|" + label if labelisation else label
            elif montant_condition == "negative" and row['Montant'] < 0:
                labelisation += "|" + label if labelisation else label
            elif montant_condition == "positive" and row['Montant'] >= 0:
                labelisation += "|" + label if labelisation else label
        """
    return labelisation if labelisation else default



def access_bank_transaction_labelisation(data, new_key_salary="", categories=LABELISATION):
    try:
        categories = add_employer_labelisation(categories, new_key_salary)
        if "Reference" not in data.columns:
            data["Reference"] = ""
        data["TransDetLabel"] = data["TransDet"] + data["Reference"] 
 
        # preprocess replace char speci to ' '
        caracteres_speciaux = ['.', ':', '|', '-', '_', '\r', '\n','*']
        for char in caracteres_speciaux:
            data['TransDetLabel'] = data['TransDetLabel'].str.replace(char, ' ')
        
        data['TransDetLabel'] =data['TransDetLabel'].fillna('')

        data['LABEL'] = data.apply(lambda row: access_bank_classify_transaction(row, categories), axis=1)
        categories = del_employer_labelisation(categories)
        return data
    except Exception as e:
        print(f"[ERROR][utils.process_access_bank.labelisation] {e}")
        data['LABEL'] = ""
        return data


def extract_from_and_to(text, all_keywords):
    words = text.lower().split()

    from_index = -1
    to_index = -1
    for_index = -1

    for i, word in enumerate(words):
        word=word.replace('\r', ' ')
        if (word == "from" and i < len(words) - 1) or (word == "frm" and i < len(words) - 1) or ("from" in word and i < len(words) - 1) or ("frm" in word and i < len(words) - 1):
            from_index = i
        elif word == "to" and i < len(words) - 1:
            to_index = i
        elif word == "for" and i < len(words) - 1 and all(ch not in text.lower() for ch in ['levy', 'charge', 'vat']):
            for_index = i

    if from_index != -1 and to_index != -1 and from_index < to_index:
        from_text = ' '.join(words[from_index + 1:to_index])
    elif from_index != -1 and to_index == -1:
        from_text = ' '.join(words[from_index + 1:])
    elif for_index != -1 and from_index < for_index:
        from_text = ' '.join(words[from_index + 1:for_index])
    else:
        from_text = ""

    if to_index != -1 and to_index < len(words) - 1:
        to_text = ' '.join(words[to_index + 1:])
    elif for_index != -1 and for_index < len(words) - 1:
        to_text = ' '.join(words[for_index + 1:])
    else:
        to_text = ""
    from_text = ' '.join([word for word in from_text.split() if word not in all_keywords])
    to_text = ' '.join([word for word in to_text.split() if word not in all_keywords])

    from_text, to_text = from_text.strip(), to_text.strip()
    
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
    
    return from_text.strip(), to_text.strip()


def exp_dest_name(df):

    df['FROM'] = None
    df['TO'] = None
    
    for index, row in df.iterrows():
        trans_det = row['TransDet']
        
        if "from" in trans_det.lower() and "to" in trans_det.lower():
            match = re.search(r'from\s+([\w\s]+)\s+to\s+([\w\s]+)', trans_det, re.IGNORECASE)
            if match:
                df.at[index, 'FROM'] = match.group(1)
                df.at[index, 'TO'] = match.group(2)
        elif "FROM" in trans_det:
            match = re.search(r'FROM\s+(\w+(?:\s+\w+)*)\s*(?:IS\s+(\w+(?:\s+\w+)*)|$)', trans_det)
            if match:
                df.at[index, 'FROM'] = match.group(1)
                df.at[index, 'TO'] = match.group(2)
        elif "from" in trans_det:
            match = re.search(r'from\s+(\w+(?:\s+\w+)*)\s*(?:IS\s+(\w+(?:\s+\w+)*)|$)', trans_det)
            if match:
                df.at[index, 'FROM'] = match.group(1)
                df.at[index, 'TO'] = match.group(2)
        elif "NIP" in trans_det:
            match = re.search(r'NIP\s+(\w+(?:\s+\w+)*)', trans_det)
            if match:
                df.at[index, 'FROM'] = match.group(1)
        elif "QTM TRSF" in trans_det:
            # Utilisation de re.split pour diviser la chaîne de caractères
            segments = re.split("/", trans_det)

            # Récupération de l'FROM
            df.at[index, 'FROM'] = segments[1]

            # Récupération du TO
            df.at[index, 'TO'] = segments[-1]
        elif "ITO" in trans_det and "-" in trans_det:
            mots = trans_det.split()
            if len(mots) >= 4 and mots[-2] == '-':
                df.at[index, 'FROM'] = ' '.join(mots[1:-2]) 
                df.at[index, 'TO'] = None
            else:
                df.at[index, 'FROM'] = None
                df.at[index, 'TO'] = None
        elif "NXG TRF" in trans_det:
            match = re.search(r'NXG TRF/([^/]+)/(?:FRM\s+([^/]+)\s+TO\s+(.+)|(.+))', trans_det)
            
            if match:
                df.at[index, 'FROM'] = match.group(2).strip() if match.group(2) else match.group(1).strip()
                df.at[index, 'TO'] = match.group(3).strip() if match.group(3) else None
    
    return df


def access_bank_from_to(data, categories=LABELISATION, KEYWORDS=KEYWORDS):
    try:
        all_keywords = [keyword.lower() for details in categories.values() for keyword in details["keywords"]]
        # Récupérer les mots-clés de la catégorie "salary" qui  sont inserer via front employer-name
        salary_keywords = [keyword.lower() for keyword in LABELISATION["salary"]["keywords"] if keyword != "salary"]

        # Filtrer les mots-clés pour enlever ceux sont inserer via front employer-name
        all_keywords = [keyword for keyword in all_keywords if keyword not in salary_keywords]

        all_keywords = all_keywords + KEYWORDS
        data[['FROM', 'TO']] = data['TransDet'].apply(lambda x: extract_from_and_to(x, all_keywords)).apply(pd.Series)
        return data
    except Exception as e:
        print(f"[ERROR][utils.process_access_bank.access_bank_from_to] {e}")
        data['FROM'] = ""
        data['TO'] = ""
        return data


def access_bank_calcul_score(data):
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
    
    data = access_bank_pdf_reader(pdf_path, password)    # pdf reader
    print(data.columns)
    data = standardize_column_name(data, COLUMNS_MAPPING) # rename column name as standard name
    print(data.columns)
    
    data = parse_date(data, "Date") # create new column Datetime from column Date

    data = montant_validation(data) # create 2 new column Montant & Solde and validate value credit/debit/balance
    
    data = access_bank_transaction_labelisation(data)
    
    data.to_csv('AccessBankGamble-webspend'+'.csv', index=False, sep=";", quoting=1)

    #data = access_bank_from_to(data, categories=LABELISATION)
    
    #data = exp_dest_name(data)

    #print(data[["FROM", "TO"]])
    #access_bank_calcul_score(data)
"""
