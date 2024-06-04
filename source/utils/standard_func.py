import pandas as pd 
import time 
import numpy as np
import math


def find_nan(dataframe, nom_colonne):
    # Vérifier les NaN dans la colonne spécifiée
    masque_nan = dataframe[nom_colonne].isna()

    # Obtenir les numéros de ligne où les NaN sont présents
    lignes_nan = masque_nan[masque_nan].index.tolist()

    return lignes_nan


def file_validation(data):

    status = "success"
    errors = []
    try:
        if not isinstance(data, pd.DataFrame):
            status = "errors"
            obj = {'code':'FILE_READ_ERROR'}
            errors.append(obj)
            #errors.append("impossible de lire le fichier")


        if data.empty:
            status = "errors"
            obj = {'code':'FILE_READ_ERROR'}
            errors.append(obj)
            #errors.append("impossible de lire les données dans le fichier")

        cols = ['Date', 'Solde']
        

        for col in cols:
            if col not in data.columns:
                status = 'errors'
                obj = {'code':'FILE_READ_ERROR'}
                errors.append(obj)

            liste_nan = find_nan(data, col)
            if len(liste_nan) > 0:
                status = 'errors'
                for num_row in liste_nan:     
                    try:
                        nrow = int(num_row) + 1       
                        obj = {'code':'PARTIAL_DATA_READ_ERROR'}
                        if len(errors) == 0:
                            errors.append(obj)
                        #errors.append("impossible de lire la valeur de la colonne "+str(col)+" à la ligne "+str(nrow))
                    except:
                        pass
    except Exception as e:
        print(f"[ERROR][file_validation] {e}")
        status = 'errors'
        obj = {'code':'FILE_READ_ERROR'}
        errors.append(obj)
    return status, errors


def add_status_output(out, status, errors):
    if not isinstance(out, dict):
        return {"status" : "errors", "errors": ["impossible de trouver la banque correspondant"]}
    else:
        out["status"] = status
        out["errors"] = errors
        return out


def add_employer_labelisation(LABELISATION, new_salary_keys):
    if "salary" in LABELISATION:
        for new_key in new_salary_keys:
            if new_key is not None and new_key != "":
                # Ajouter le nouveau mot-clé à la liste existante
                LABELISATION["salary"]["keywords"].append(str(new_key).lower())
    print("LABELISATION[salary][keywords] ==> ",LABELISATION["salary"]["keywords"])
    return LABELISATION

def del_employer_labelisation(LABELISATION):
    if "salary" in LABELISATION:
        LABELISATION["salary"]["keywords"] = ["salary"]
    return LABELISATION
    

def parse_date(df, colomn_date, list_formats=[]):
    
    try:
        # Création de la nouvelle colonne 'Date_Parse'
        df['Datetime'] = pd.to_datetime(df[colomn_date], errors='coerce', infer_datetime_format=True)
        df['Datetime'] = df['Datetime'].fillna(method='ffill')
        df['Date']=df['Datetime'].dt.date
        df['Heure']=df['Datetime'].dt.time
        return df 
    except Exception as e:
        print(f"[ERROR][standard_func.parse_date] {e}")
        return df

def standardize_column_name(df, mapping={}):
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
        print(f"[ERROR][standard_func.standardize_column_name] {e}")
        return df


def extract_last_line(text):
    lines = str(text).split('\r') # extration montant pour le bug d'un texte invisible qui superpose les données dans le PDF
    return lines[-1] if lines else text


def montant_validation(df):
    try:
        df['Credit'] = df['Credit'].apply(extract_last_line)
        df['Debit'] = df['Debit'].apply(extract_last_line)
        
        df['Credit'] = pd.to_numeric(df['Credit'].str.replace(',', ''), errors='coerce')
        df['Debit'] = pd.to_numeric(df['Debit'].str.replace(',', ''), errors='coerce')
        df['Solde'] = pd.to_numeric(df['Solde'].str.replace(',', ''), errors='coerce')
        df['Montant'] = df['Solde'] - df['Solde'].shift(1) # On ne calcul plusle solde en bas et ceci remplace le calcul Montant en bas
        df['Montant'] = np.where(df['Montant'].isna(), df['Credit'].fillna(df['Debit']), df['Montant'])


        # Ajout de la colonne "Solde_Verification"
        df['Solde_calcul'] = df['Solde'].shift(1) + df['Credit'] - df['Debit']

        # Ajout des colonnes "Credit_True" et "Debit_True"
        df['Credit_calcul'] = df['Credit'].where((df['Credit'].notna() | (df['Credit'] != 0)), df['Solde'] - df['Solde'].shift(1)) 
        df['Credit_calcul'] = df['Credit_calcul'].abs()
        df['Debit_calcul'] = df['Debit'].where((df['Debit'].notna() | (df['Debit'] != 0)), df['Solde'] - df['Solde'].shift(1))
        df['Debit_calcul'] = df['Debit_calcul'].abs()

        #df['Montant'] = df['Credit_calcul'].where((df['Credit_calcul'].notna() & df['Credit_calcul'] > 0), -1 * df['Debit'])
        df['Credit'] = df.apply(lambda row: row['Credit_calcul'] if pd.notna(row['Credit']) and not isinstance(row['Credit'], (int, float))  and pd.notna(row['Credit_calcul']) and isinstance(row['Credit_calcul'], (int, float)) and row['Credit_calcul'] > 0 else row['Credit'], axis=1)
        df['Debit'] = df.apply(lambda row: row['Debit_calcul'] if pd.notna(row['Debit']) and not isinstance(row['Debit'], (int, float))  and pd.notna(row['Debit_calcul']) and isinstance(row['Debit_calcul'], (int, float)) and row['Debit_calcul'] > 0 else row['Debit'], axis=1)
        #df['Solde'] = df.apply(lambda row: row['Solde_calcul'] if pd.notna(row['Solde']) and not isinstance(row['Solde'], (int, float))  and pd.notna(row['Solde_calcul']) and isinstance(row['Solde_calcul'], (int, float)) and row['Solde_calcul'] > 0 else row['Solde'], axis=1)

        return df
    except Exception as e:
        print(f"[ERROR][standard_func.montant_validation] {e}")
        return df



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
        data = data.groupby(transCol)["Montant"].count()
        data = data.reset_index()
        head = data.sort_values(by="Montant", ascending=True).head(3)
        return list(head[transCol].tolist())
    except Exception as e:
        print(e)
        return []

import pandas as pd


def labels_reguliers(df, column_rec, seuil=0.3):
    # Assurez-vous que la colonne datetime est de type datetime
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    
    # Extrait le jour de la semaine et le jour du mois
    df['day_of_week'] = df['Datetime'].dt.day_name()
    df['day_of_month'] = df['Datetime'].dt.day
    
    # Calcul du nombre total de semaines et de mois dans le DataFrame
    nb_semaines = len(df['Datetime'].dt.isocalendar().week.unique())
    nb_mois = len(df['Datetime'].dt.to_period('M').unique())
    
    # Trouve les labels qui reviennent chaque semaine avec une proportion minimale
    labels_semaine = df[column_rec][df.groupby('day_of_week')[column_rec].transform('nunique') >= len(df[column_rec].unique()) * seuil].unique()
    
    # Trouve les labels qui reviennent chaque mois avec une proportion minimale
    labels_mois = df[column_rec][df.groupby('day_of_month')[column_rec].transform('nunique') >= len(df[column_rec].unique()) * seuil].unique()
    
    # Combine les labels qui reviennent chaque semaine ou chaque mois
    labels_reguliers_ = list(set(labels_semaine) | set(labels_mois))
    labels_reguliers_ = [label for label in labels_reguliers_ if label != ""]
    print("labels_reguliers ==> ", labels_reguliers_)    
    return labels_reguliers_


def search_recurring_cat(data, column_rec, monthPeriod, list_debit_not_expense):
    df_rec = data.loc[data['Montant'] < 0]
    df_rec['mois'] = df_rec['Datetime'].dt.to_period('M')
    df_rec = df_rec[['mois', column_rec]].groupby(['mois', column_rec]).count().reset_index()
    df_rec = df_rec.groupby(column_rec)['mois'].count().reset_index()
    df_rec = df_rec.loc[df_rec['mois']>monthPeriod*0.3]
    liste = [x.lower() for x in df_rec[column_rec].tolist()]
    liste = list(set(liste) - set([x.lower() for x in list_debit_not_expense]))
    liste = list(set(liste) - set(['']))  
    return liste


def attribuer_plage(valeur_min, valeur_max, plage=10000):
    # Déterminez dans quelle plage de 10 000 se trouvent les valeurs
    plage_debut = (valeur_min // plage) * plage
    nouvelle_valeur_min = plage_debut
    nouvelle_valeur_max = plage_debut + plage

    return nouvelle_valeur_min, nouvelle_valeur_max


def serialize_dict(dict_output):
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
                    else:
                        if cle != 'accountActivity' and cle != 'loanInflowRate' and cle != 'loanRepaymentInflowRate':
                            dict_output[cle] = math.ceil(float(valeur))
                        else:
                            if cle != 'loanInflowRate' and cle != 'loanRepaymentInflowRate':
                                dict_output[cle] = math.ceil(float(valeur) * 100) / 100 # partie entiere superieur
                            else:
                                dict_output[cle] = float(valeur)
                if isinstance(valeur, np.float32):
                    if np.isinf(valeur):
                        dict_output[cle] = None
                    else:
                        if cle != 'accountActivity' and cle != 'loanInflowRate' and cle != 'loanRepaymentInflowRate':
                            dict_output[cle] = math.ceil(float(valeur))
                        else:
                            if cle != 'loanInflowRate' and cle != 'loanRepaymentInflowRate':
                                dict_output[cle] = math.ceil(float(valeur) * 100) / 100  # partie entiere superieur
                            else:
                                dict_output[cle] = float(valeur)
                if isinstance(valeur, float):
                    if valeur == float('inf') or valeur == float('-inf') or math.isnan(valeur):
                        dict_output[cle] = None
                    else:
                        if cle != 'accountActivity' and cle != 'loanInflowRate' and cle != 'loanRepaymentInflowRate':
                            dict_output[cle] = math.ceil(float(valeur))
                        else:
                            if cle != 'loanInflowRate' and cle != 'loanRepaymentInflowRate':
                                dict_output[cle] = math.ceil(float(valeur) * 100) / 100 # partie entiere superieur
                            else:
                                dict_output[cle] = float(valeur)
            except:
                pass 
    return dict_output
    
############################### CACUL ##########################################

def process_account_activity(data):
    try:
        nb_jour_activity=data['Date'].nunique()
        min_date=data['Date'].min()
        max_date=data['Date'].max()
        nb_jour_total=(max_date - min_date).days + 1
        #print(nb_jour_activity, nb_jour_total, min_date , max_date)
        accountActivity=nb_jour_activity/nb_jour_total
        return accountActivity
    except Exception as e:
        print(f"[ERROR][utils.standard_func.accountActivity] {e}")
        return None


def process_average_balance(data):
    try:
        balance=data[["Date","Heure","Solde"]]
        # groupena dtejour, ze max heure iany no raisina - (moyenne max journaliere)
        date_heure_max=data[["Date","Heure"]].groupby(['Date']).max().reset_index()
        balance= pd.merge(date_heure_max,balance,left_on=['Date','Heure'] ,right_on=['Date','Heure'])
        balance["Date"]=balance["Date"].astype(str)

        max_date=data['Date'].max()
        min_date=data['Date'].min()
        nb_jour_total=(max_date - min_date).days + 1
        liste_date_total=pd.date_range(min_date, periods=nb_jour_total).strftime("%Y-%m-%d")
        liste_date_df = pd.DataFrame(liste_date_total,columns =['Date'])
        liste_date_df["Date"]=liste_date_df["Date"].astype(str)
         
        all_balance= pd.merge(liste_date_df,balance[["Date","Solde"]], on="Date",how="left").fillna(method='ffill')
        averageBalance=all_balance["Solde"].mean()
        averageBalance = float(averageBalance)
        return averageBalance 
    except Exception as e:
        print(f"[ERROR][utils.standard_func.averageBalance] {e}")
        return None


def process_average_credit(data):
    try:    
        averageCredits=data.loc[data['Montant'] >0, 'Montant'].mean()
        averageCredits = float(averageCredits)
        return averageCredits
    except Exception as e:
        print(f"[ERROR][utils.standard_func.averageCredits] {e}")
        return None


def process_average_debit(data):
    try:    
        averageDebits=data.loc[data['Montant'] <0, 'Montant'].abs().mean()
        averageDebits = float(averageDebits)
        return averageDebits
    except Exception as e:
        print(f"[ERROR][utils.standard_func.averageDebits] {e}")
        return None


def process_closing_balance(data):
    closingBalance,initialBalance = None, None
    try:    
        closingBalance=data[data['Datetime'] == data['Datetime'].max()]["Solde"].values[-1] # plus  heure
        closingBalance = float(closingBalance)
    except Exception as e:
        print(f"[ERROR][utils.standard_func.closingBalance] {e}")
        closingBalance = None
    try:    
        initialBalance=data[data['Datetime'] == data['Datetime'].min()]["Solde"].values[0] # plus heure
        initialBalance = int(initialBalance)
    except Exception as e:
        print(f"[ERROR][utils.standard_func.initialBalance] {e}")
        initialBalance=None
    return closingBalance,initialBalance


def process_month_period(data):
    try:
        max_date=data['Date'].max()
        min_date=data['Date'].min()
        firstDay=min_date.strftime("%Y-%m-%d")
        lastDay=max_date.strftime("%Y-%m-%d")
    except Exception as e:
        firstDay = None
        lastDay = None

    try:
        max_date=data['Date'].max()
        min_date=data['Date'].min()
        monthPeriod = (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month)
        monthPeriod = int(monthPeriod) + 1
        return firstDay, lastDay, monthPeriod
    except Exception as e:
        print(f"[ERROR][utils.standard_func.monthPeriod] {e}")
        return firstDay, lastDay, None


def process_net_average_monthly_earnings(data, averageOtherIncome, averageSalary, totalExpenses):
    """Il s'agit de la moyenne mensuelle du revenu total restant sur le compte, 
    déduction faite des emprunts (prêts), des transferts, des factures et services publics, des frais, des dépenses, etc."""
    # depende de salary, OtherIncome
    
    try:
        if averageOtherIncome is not None and averageSalary is not None and ((averageOtherIncome + averageSalary)) > 0: 
            netAverageMonthlyEarnings=averageOtherIncome + averageSalary - totalExpenses
            if netAverageMonthlyEarnings < 0:
                netAverageMonthlyEarnings = 0
        else:
            netAverageMonthlyEarnings = 0
        return netAverageMonthlyEarnings
    except Exception as e:
        print(f"[ERROR][utils.standard_func.netAverageMonthlyEarnings] {e}")
        return 0


def process_total_credit_turover(data):
    try:
        totalCreditTurnover=data.loc[data['Montant'] >0, 'Montant'].sum()
        totalCreditTurnover = int(totalCreditTurnover)
        return totalCreditTurnover
    except Exception as e:
        print(f"[ERROR][utils.standard_func.totalCreditTurnover] {e}")
        return None

def process_nb_total_credit_turnover(data):
    try:
        nbTotalCreditTurnover=data.loc[data['Montant'] >0, 'Montant'].count()
        nbTotalCreditTurnover = int(nbTotalCreditTurnover)
        return nbTotalCreditTurnover
    except Exception as e:
        print(f"[ERROR][utils.standard_func.nbTotalCreditTurnover] {e}")
        return None

def process_nb_total_debit_turnover(data):
    try:
        nbTotalDebitTurnover=data.loc[data['Montant'] <0, 'Montant'].count()
        nbTotalDebitTurnover = int(nbTotalDebitTurnover)
        return nbTotalDebitTurnover
    except Exception as e:
        print(f"[ERROR][utils.standard_func.nbTotalCreditTurnover] {e}")
        return None

def process_total_debit_turnover(data):
    try:
        totalDebitTurnover=data.loc[data['Montant'] <0, 'Montant'].abs().sum()
        totalDebitTurnover = int(totalDebitTurnover)
        return totalDebitTurnover
    except Exception as e:
        print(f"[ERROR][utils.standard_func.totalDebitTurnover] {e}")
        return None

def process_credit_debit(totalCreditTurnover,totalDebitTurnover):
    try:
        credit_debit =  totalCreditTurnover / totalDebitTurnover
        credit_debit = float(credit_debit)
        return credit_debit
    except Exception as e:
        print(f"[ERROR][utils.standard_func.credit_debit] {e}")
        return None

def process_year_in_statement(data):
    try:
        yearInStatement=pd.DatetimeIndex(data['Date']).year.unique()
        yearInStatement = int(yearInStatement[0])
        return yearInStatement
    except Exception as e:
        print(f"[ERROR][utils.standard_func.yearInStatement] {e}")
        return None    


def process_max_monthly_repayement(data):
    try:
        df = data.copy()
        df['mois'] = df['Datetime'].dt.to_period('M')
        averageMonthlyCredit = df.loc[df['Montant'] >0].groupby('mois')['Montant'].sum().mean()
        averageMonthlyDebit = abs(df.loc[df['Montant'] <0].groupby('mois')['Montant'].sum().mean())
        nbAverageMonthlyCredit = int(df.loc[df['Montant'] >0].groupby('mois')['Montant'].count().mean())
        nbAverageMonthlyDebit = int(df.loc[df['Montant'] <0].groupby('mois')['Montant'].count().mean())
        netCashFlow = (abs(averageMonthlyCredit)-abs(averageMonthlyDebit))
        if netCashFlow < 0:
            #netCashFlow = 0
            MaxMonthlyRepayment = 0
        else:
            MaxMonthlyRepayment = netCashFlow * 0.4
        del df
        return averageMonthlyCredit, averageMonthlyDebit, nbAverageMonthlyCredit, nbAverageMonthlyDebit, netCashFlow, MaxMonthlyRepayment
    except Exception as e:
        print(f"[ERROR][utils.standard_func.MaxMonthlyRepayment] {e}")
        return None, None, None, None, None, 0

def process_total_monthly_credit_debit(data):
    try:
        totalMonthlyCredit=float(data.loc[data['Montant'] >0, 'Montant'].sum())
        totalMonthlyDebit=float(data.loc[data['Montant'] <0, 'Montant'].abs().sum())
        return totalMonthlyCredit, totalMonthlyDebit
    except Exception as e:
        print(f"[ERROR][utils.standard_func.totalMonthlyCreditDebit] {e}")
        return None, None 


def process_average_other_income(data):
    try:
        liste_ = ["other-income"]
        data_other_income = data.loc[data['Montant'] > 0]
        data_other_income['LABEL'] = data_other_income['LABEL'].str.lower()
        data_other_income = data_other_income.loc[data_other_income['LABEL'].str.contains('|'.join(liste_))]
        numberOtherIncomePayments=data_other_income.loc[data_other_income['Montant'] > 0, 'Montant'].count() 
        numberOtherIncomePayments = int(numberOtherIncomePayments)
        data_other_income['mois'] = data_other_income['Datetime'].dt.to_period('M')
        averageOtherIncome = data_other_income.groupby('mois')['Montant'].sum().mean()
        averageOtherIncome=float(averageOtherIncome) # Moyenne mensuelle de toutes les transactions non salariales détectées sur la période du relevé bancaire 
        if pd.isna(averageOtherIncome):
            averageOtherIncome = 0
        if pd.isna(numberOtherIncomePayments):
            numberOtherIncomePayments = 0
        return averageOtherIncome, numberOtherIncomePayments
    except Exception as e:
        print(f"[ERROR][utils.standard_func.averageOtherIncome] {e}")
        return 0, 0


def process_average_salary(data):
    try:
        liste_ = ["salary"]
        data_temp = data.loc[data['Montant'] > 0]
        data_temp = data_temp.loc[~data_temp['LABEL'].str.contains('other-income')]
        data_temp['LABEL'] = data_temp['LABEL'].str.lower()
        data_temp = data_temp.loc[data_temp['LABEL'].str.contains('|'.join(liste_))]
        print(data_temp['LABEL'])
        numberOfSalaryPayments=data_temp.loc[data_temp['Montant'] > 0, 'Montant'].count() 
        numberOfSalaryPayments = int(numberOfSalaryPayments)
        data_temp['mois'] = data_temp['Datetime'].dt.to_period('M')
        averageSalary = data_temp.groupby('mois')['Montant'].sum().mean()
        salaryFrequency=data_temp.groupby('mois')['Montant'].count().mean()
        averageSalary=float(averageSalary) # Moyenne des transactions salariales mensuelles détectées 
        if pd.isna(averageSalary):
            averageSalary = 0
        if pd.isna(numberOfSalaryPayments):
            numberOfSalaryPayments = 0
        if pd.isna(salaryFrequency):
            salaryFrequency = None

        # si les données contient salary, bonuses,commision => the owner is a salary earner
        # on peut ne pas trouver un transaction salary tout en sachant que c'est un salarie
        data_temp2 = data.loc[data['Montant'] > 0]
        data_temp2['LABEL'] = data_temp2['LABEL'].str.lower()
        data_temp2 = data_temp2.loc[data_temp2['LABEL'].str.contains('|'.join(["salary", "slr-earner"]))]
        numberOfSalaryPayments2=data_temp2.loc[data_temp2['Montant'] > 0, 'Montant'].count()
        if numberOfSalaryPayments2 is not None and numberOfSalaryPayments2 > 0:
            salaryEarner=True
        else:
            salaryEarner=False

        return averageSalary, numberOfSalaryPayments, salaryEarner,salaryFrequency
    except Exception as e:
        print(f"[ERROR][utils.standard_func.averageSalary] {e}")
        return 0, 0, False, None


def process_confidence_interval(data, averageSalary):
    try:
        liste_ = ["salary"]
        data_salary = data.loc[data['LABEL'].str.contains('|'.join(liste_))]
        data_salary["mois_salary"]=pd.DatetimeIndex(data_salary['Date']).month
        nb_month_unique_salary = data_salary['mois_salary'].nunique()
        std = data_salary.loc[data_salary['Montant'] > 0, 'Montant'].abs().std()
        stability = (std/averageSalary)
        freq = 1 /nb_month_unique_salary
        confidenceIntervalOnSalaryDetection = 1 - stability - min([stability, freq]) # pas d'information permetant de differencier les salaires
        confidenceIntervalOnSalaryDetection = float(confidenceIntervalOnSalaryDetection) * 100
        if confidenceIntervalOnSalaryDetection > 100:
            confidenceIntervalOnSalaryDetection = 100
        return confidenceIntervalOnSalaryDetection
    except Exception as e:
        print(f"[ERROR][utils.standard_func.confidenceIntervalOnSalaryDetection] {e}")
        return None


def process_salary_day(data):
    try:
        liste_ = ["salary"]
        data_salary = data.loc[data['LABEL'].str.contains('|'.join(liste_))]
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
        return expectedSalaryDay
    except Exception as e:
        print(f"[ERROR][utils.standard_func.expectedSalaryDay] {e}")
        return None


def process_last_salary_date(data):
    try:
        liste_ = ["salary"]
        data_salary = data.loc[data['LABEL'].str.contains('|'.join(liste_))]
        lastSalaryDate=data_salary["Date"].max().strftime("%Y-%m-%d") # pas d'information permetant de differencier les salaires 
        print("lastSalaryDate", lastSalaryDate)
        return lastSalaryDate
    except Exception as e:
        print(f"[ERROR][utils.standard_func.lastSalaryDate] {e}")
        return None


def process_median_income(data):
    try:
        liste_ = ["salary", "other-income"]
        data_salary = data.loc[data['LABEL'].str.contains('|'.join(liste_))]
        medianIncome=data_salary.loc[data_salary['Montant'] > 0, 'Montant'].abs().median() # pas d'information permetant de differencier les salaires et des autres revenus
        medianIncome = int(medianIncome)
        print("medianIncome", medianIncome)
        if pd.isna(medianIncome):
            medianIncome = None
        return medianIncome
    except Exception as e:
        print(f"[ERROR][utils.standard_func.medianIncome] {e}")
        return None


def process_number_of_transacting_month(data):
    noOfTransactingMonths, noOfTransactingDays,dayPeriode = None, None, None
    try:
        noOfTransactingMonths=pd.DatetimeIndex(data['Date']).month.nunique()
        noOfTransactingMonths = int(noOfTransactingMonths)
        if pd.isna(noOfTransactingMonths):
            noOfTransactingMonths = None
    except Exception as e:
        print(f"[ERROR][utils.standard_func.noOfTransactingMonths] {e}")
        noOfTransactingMonths = None
    try:
        min_date=data['Date'].min()
        max_date=data['Date'].max()
        nb_jour_total=(max_date - min_date).days + 1
        dayPeriode=int(nb_jour_total)
        nb_jour_activity=data['Date'].nunique()
        noOfTransactingDays=int(nb_jour_activity)
        if pd.isna(noOfTransactingDays):
            noOfTransactingDays = None
        if pd.isna( dayPeriode):
            dayPeriode = None
    except Exception as e:
        print(f"[ERROR][utils.standard_func.noOfTransactingDays/dayPeriode] {e}")
        dayPeriode=None
        noOfTransactingDays=None
    return noOfTransactingMonths, noOfTransactingDays, dayPeriode



def process_spend(data, label, noOfTransactingMonths):
    try:
        liste_ = [str(label)]
        data_temp = data.loc[data['Montant'] < 0]
        out=data_temp.loc[data_temp['LABEL'].str.contains('|'.join(liste_)), 'Montant'].abs().sum()/noOfTransactingMonths
        out = float(out)
        if pd.isna(out):
            out = None
        return out 
    except Exception as e:
        print(f"[ERROR][utils.standard_func.dab] {e}")
        return 0

def process_total_expense(data, noOfTransactingMonths, list_expense=["atm-spend" ,"web-spend" ,"pos-spend","ussd-spend","mobile-spend","spend-on-transfert","international-spend","bills-spend","entertainment-spend","savingsinvestments-spend","gambling-spend","airtime-spend" ,"bankcharges-spend" ,"bundle-spend"]):
    try:
        liste_spt = list_expense
        liste_spt = [x.lower() for x in liste_spt]
        data_spt = data.loc[data['Montant'] < 0]
        totalExpenses=data_spt.loc[data['LABEL'].str.contains('|'.join(liste_spt)), 'Montant'].abs().sum()/noOfTransactingMonths
        totalExpenses = float(totalExpenses)
        if pd.isna(totalExpenses):
            totalExpenses = None
        return totalExpenses
    except Exception as e:
        print(f"[ERROR][utils.standard_func.totalExpenses] {e}")
        return None




# Fonction pour extraire la catégorie appropriée pour les reccuring depenses
reccuring_categories = ["bills-spend"]
def extract_category(label, bills_values_list):
    cat = ''
    for category in bills_values_list:
        if category in label:
            cat = category
    return cat  # Si aucune catégorie correspondante n'est trouvée, renvoyer la valeur d'origine


def process_reccuring_expense(data, monthPeriod, list_bills=["bills"]):
    averageRecurringExpense=None
    liste_reccuring_trans = []
    

    try:
        data_reccuring=data.loc[data['LABEL'].str.contains('|'.join(reccuring_categories))]
        data_reccuring=data_reccuring.loc[data_reccuring['Montant'] < 0]
        data_reccuring['TransDet'] = data_reccuring['TransDet'].str.lower()
        #data_reccuring['LABEL2'] = data_reccuring['TransDet'].apply(extract_category)
        data_reccuring['LABEL2'] = data_reccuring['TransDet'].apply(extract_category, bills_values_list=list_bills)
        data_reccuring['LABEL2'] = data_reccuring['LABEL2'].fillna('')
        print("data_reccuring['LABEL2'] ===> ", data_reccuring['LABEL2'])
        #liste = search_recurring_cat(data, "LABEL", monthPeriod, list_debit_not_expense) # on utilise plus cette fonction, ceci a ete remplacer par labels_regiluers
        liste = labels_reguliers(data_reccuring, "LABEL2", seuil=0.3)

        print("liste1",liste)
        if len(liste) > 0:
            print(liste)
            #print(data['LABEL2'])
            data_reccuring=data_reccuring.loc[data_reccuring['LABEL2'].str.contains('|'.join(liste))]
            data_reccuring=data_reccuring.loc[data_reccuring['Montant'] < 0]
            #print(data_reccuring)
            data_reccuring['Montant'] = data_reccuring['Montant'].abs()
            data_reccuring["mois"]=pd.DatetimeIndex(data_reccuring['Date']).month
            averageRecurringExpense=data_reccuring.groupby('mois')['Montant'].sum().mean()
            averageRecurringExpense = float(averageRecurringExpense)
            if pd.isna(averageRecurringExpense):
                averageRecurringExpense = None
            liste_reccuring_trans = liste
    except Exception as e:
        print(f"[ERROR][utils.standard_func.averageRecurringExpense] {e}")
        averageRecurringExpense=None
    

    hasRecurringExpense=None
    try:
        if averageRecurringExpense is not None and averageRecurringExpense > 0:
            hasRecurringExpense=True
        else:
            hasRecurringExpense=False
    except Exception as e:
        print(f"[ERROR][utils.standard_func.hasRecurringExpense] {e}")
        hasRecurringExpense=False

    recurringExpense = []
    try:
        liste = liste_reccuring_trans
        if len(liste) > 0:
            data_reccuring=data.loc[data['LABEL'].str.contains('|'.join(reccuring_categories))]
            data_reccuring=data_reccuring.loc[data_reccuring['Montant'] < 0]
            data_reccuring['TransDet'] = data_reccuring['TransDet'].str.lower()
            data_reccuring['LABEL2'] = data_reccuring['TransDet'].apply(extract_category, bills_values_list=list_bills)
            data_reccuring['LABEL2'] = data_reccuring['LABEL2'].fillna('')
            data_reccuring=data_reccuring.loc[data_reccuring['LABEL2'].str.contains('|'.join(liste))]
            data_debit = data_reccuring.loc[data_reccuring['Montant'] < 0]
            recurringExpense = process_top_3(data_debit, "LABEL2")
                
    except Exception as e:
        print(f"[ERROR][utils.standard_func.recurringExpense] {e}")
        recurringExpense=[]

    return averageRecurringExpense, hasRecurringExpense, recurringExpense



def process_account_sweep(data):
    try:
        accountSweep=False # differenciation accountSweep?
        return accountSweep
    except Exception as e:
        #print(f"[ERROR][utils.standard_func.accountSweep] {e}")
        return False

def process_gambling_rate(data):
    try:
        gamblingRate=None # differenciation gamblingRate?
        return gamblingRate
    except Exception as e:
        #print(f"[ERROR][utils.standard_func.gamblingRate] {e}")
        return None

def process_inflow_outflow_rate(data):
    try:   
        # inflowOutflowRate
        data["mois"]=pd.DatetimeIndex(data['Date']).month
        group_mois = data.groupby('mois')
        debit_mois=pd.Series(group_mois.apply(lambda x: x[x['Montant'] >0]['Montant'].sum()),name="credit").to_frame()
        credit_mois=pd.Series(group_mois.apply(lambda x: x[x['Montant'] <0]['Montant'].sum()),name="debit").to_frame()
        
        inflowOutflowRate=pd.concat([debit_mois,credit_mois], axis=1)
        inflowOutflowRate["ratio"]=inflowOutflowRate["debit"]+inflowOutflowRate["credit"]
        inflowOutflowRate["month_status"]=np.select([inflowOutflowRate['ratio'] > 0, inflowOutflowRate['ratio'] <0], ['positive_months', 'negative_months'], default='low')
        occurence=inflowOutflowRate["month_status"].value_counts()
    except Exception as e:
        print(f"[ERROR][utils.standard_func.inflowOutflowRate] {e}")
        # inflowOutflowRate
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

    return inflowOutflowRate


def process_loan(data):
    loanAmount=0
    loanInflowRate = 0
    loanRepayments = 0
    loanRepaymentInflowRate = 0
    try:
        liste_ = ["loan"]
        data_temp = data.loc[data['Montant'] > 0]
        total_credit = data_temp['Montant'].abs().sum() 
        out=data_temp.loc[data_temp['LABEL'].str.contains('|'.join(liste_)), 'Montant'].abs().sum()
        loanAmount = float(out)
        loanInflowRate =  loanAmount / float(total_credit)
        loanInflowRate = float(loanInflowRate)
        print("**************>", loanAmount, total_credit, loanInflowRate)
        if pd.isna(loanAmount):
            loanAmount = 0
        if pd.isna(loanInflowRate):
            loanInflowRate = 0
        
        else:
            loanInflowRate = loanInflowRate * 100
            if loanInflowRate > 100:
                loanInflowRate = 100
        
        liste_ = ["loan-rpymnt"]
        data_temp = data.loc[data['Montant'] < 0]
        data_temp = data_temp.loc[data_temp['LABEL'].str.contains('|'.join(liste_))]
        data_temp['mois'] = data_temp['Datetime'].dt.to_period('M')
        loanRepayments = data_temp.loc[data_temp['LABEL'].str.contains('|'.join(liste_))].groupby('mois')['Montant'].sum().mean()
        loanRepayments = float(loanRepayments)
        if pd.isna(loanRepayments):
            loanRepayments = 0

        loanRepaymentInflowRate = loanRepayments / float(total_credit)
        print("**************>", loanRepayments, total_credit, loanRepaymentInflowRate)
        if pd.isna(loanRepaymentInflowRate):
            loanRepaymentInflowRate = 0
        """
        else:
            loanRepaymentInflowRate = loanRepaymentInflowRate * 100
            if loanRepaymentInflowRate > 100:
                loanRepaymentInflowRate = 100
        """
        loanAmount=abs(loanAmount)
        loanInflowRate = abs(loanInflowRate)
        loanRepayments = abs(loanRepayments)
        loanRepaymentInflowRate = abs(loanRepaymentInflowRate)
    except Exception as e:
        print(f"[ERROR][utils.standard_func.loanAmount] {e}")
        loanAmount=0
        loanInflowRate = 0
        loanRepayments = 0
        loanRepaymentInflowRate = 0

    return loanAmount, loanInflowRate, loanRepaymentInflowRate, loanRepayments


def process_top_transfert_account(data):
    
    try:
        liste = ["transfer"]
        data_top_transfert=data.loc[data["LABEL"].str.contains('|'.join(liste))]
        data_incoming = data_top_transfert.loc[data_top_transfert['Montant'] > 0]
        """
        data_incoming = data_incoming.groupby("FROM")["Montant"].count()
        data_incoming = data_incoming.reset_index()
        head = data_incoming.sort_values(by="Montant", ascending=False).head(3) 
        head = data_incoming.sort_values(by="Montant", ascending=True).head(3) 
        """
        data_incoming = data_incoming.groupby("FROM")["Montant"].agg(['count', 'sum']).reset_index()
        # Trier d'abord par le comptage (en ordre descendant) et ensuite par la somme (en ordre descendant)
        head = data_incoming.sort_values(by=['count', 'sum'], ascending=[False, False]).head(3)

        cand = [value for value in head["FROM"].tolist() if value != ""]
        topIncomingTransferAccount = cand[0]
    except Exception as e:
        print(f"[ERROR][utils.standard_func.topIncomingTransferAccount] {e}")
        topIncomingTransferAccount=None

    try:
        liste = ["transfer"]
        data_top_transfert=data.loc[data["LABEL"].str.contains('|'.join(liste))]

        data_incoming = data_top_transfert.loc[data_top_transfert['Montant'] < 0]
        """
        data_incoming = data_incoming.groupby("TO")["Montant"].count()
        data_incoming = data_incoming.reset_index()
        head = data_incoming.sort_values(by="Montant", ascending=True).head(3) 
        """
        data_incoming = data_incoming.groupby("TO")["Montant"].agg(['count', 'sum']).reset_index()
        # Trier d'abord par le comptage (en ordre descendant) et ensuite par la somme (en ordre descendant)
        head = data_incoming.sort_values(by=['count', 'sum'], ascending=[False, False]).head(3)

        cand = [value for value in head["TO"].tolist() if value != ""]
        topTransferRecipientAccount = cand[0]

    except Exception as e:
        print(f"[ERROR][utils.standard_func.topTransferRecipientAccount] {e}")
        topTransferRecipientAccount=None

    return topIncomingTransferAccount, topTransferRecipientAccount


def process_high(data):
    highestMAWOCredit , highestMAWODebit = None, None

    try:        
        index_max = data.loc[data['Montant'] >0, 'Montant'].idxmax()
        highestMAWOCredit=week_of_month(data.loc[index_max, 'Datetime'])
    except Exception as e:
        print(f"[ERROR][utils.standard_func.highestMAWOCredit] {e}")
        highestMAWOCredit=None

    try:        
        index_max = data.loc[data['Montant'] <0, 'Montant'].idxmin()
        highestMAWODebit=week_of_month(data.loc[index_max, 'Datetime'])
    except Exception as e:
        print(f"[ERROR][utils.standard_func.highestMAWODebit] {e}")
        highestMAWODebit=None

    return highestMAWOCredit , highestMAWODebit


def process_last_date(data):
    lastDateOfCredit, lastDateOfDebit = None, None
    try: 
        lastDateOfCredit=data.loc[data['Montant'] >0]["Date"].max().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[ERROR][utils.standard_func.lastDateOfCredit] {e}")
        lastDateOfCredit=None

    try:
        lastDateOfDebit=data.loc[data['Montant'] <0]["Date"].max().strftime("%Y-%m-%d")
    except Exception as e:
        print(f"[ERROR][utils.standard_func.lastDateOfDebit] {e}")
        lastDateOfDebit=None

    return lastDateOfCredit, lastDateOfDebit


def process_balance(data):
    MAWWZeroBalanceInAccount, mostFrequentBalanceRange, NODWBalanceLess5000 = None, {"min":None,"max": None,"count": None}, None
    try:      
        df_zero_solde = data[data['Solde'] <= 0]
        occurrences_par_jour = df_zero_solde.groupby(df_zero_solde['Date']).size().reset_index(name='occurrences')
        jour_plus_occurrence = occurrences_par_jour[occurrences_par_jour['occurrences'] == occurrences_par_jour['occurrences'].max()]
        jour_max_occurrence_date = pd.to_datetime(jour_plus_occurrence['Date'].iloc[0])
        MAWWZeroBalanceInAccount=week_of_month(jour_max_occurrence_date)
    except Exception as e:
        print(f"[ERROR][utils.standard_func.MAWWZeroBalanceInAccount] {e}")
        MAWWZeroBalanceInAccount=None

    try:        
        valeur_min = data['Solde'].min()
        valeur_max = data['Solde'].max()
        frequence = 1
        while (frequence > 0 and (((valeur_max - valeur_min) / frequence) > 10)):
            frequence = frequence * 10
        if frequence > 10000:
            frequence = 10000
        b_min, _ = attribuer_plage(valeur_min, valeur_min, plage=10000)
        _, b_max = attribuer_plage(valeur_max, valeur_max, plage=10000)
        intervalle = pd.interval_range(start=b_min, end=b_max,freq=frequence,  closed='right')
        data['Intervalle'] = pd.cut(data['Solde'], bins=intervalle)
        occurrences = data['Intervalle'].value_counts()
        intervalle_plus_frequent = occurrences.idxmax()
        intervalle_min = intervalle_plus_frequent.left
        intervalle_max = intervalle_plus_frequent.right
        nombre_occurrences_max = occurrences.max()

        mostFrequentBalanceRange = {"min":int(intervalle_min),"max": int(intervalle_max),"count": int(nombre_occurrences_max)}
    except Exception as e:
        print(f"[ERROR][utils.standard_func.mostFrequentBalanceRange] {e}")
        mostFrequentBalanceRange={"min":None,"max": None,"count": None}

    try:        
        df_filtre = data[data['Solde'] < 5000]
        NODWBalanceLess5000 = df_filtre.groupby(df_filtre['Date'])['Date'].nunique().count()
    except Exception as e:
        print(f"[ERROR][utils.standard_func.NODWBalanceLess5000] {e}")
        NODWBalanceLess5000=None

    return MAWWZeroBalanceInAccount, mostFrequentBalanceRange, NODWBalanceLess5000 



def process_frequent_transaction_range(data):
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
        
        intervalle = pd.interval_range(start=b_min, end=b_max,freq=frequence, closed='right')
        data['Intervalle'] = pd.cut(data['Montant'].abs(), bins=intervalle)
        occurrences = data['Intervalle'].value_counts()
        intervalle_plus_frequent = occurrences.idxmax()
        intervalle_min = intervalle_plus_frequent.left
        intervalle_max = intervalle_plus_frequent.right
        nombre_occurrences_max = occurrences.max()
        mostFrequentTransactionRange = {"min":int(intervalle_min),"max": int(intervalle_max),"count": int(nombre_occurrences_max)}
        return mostFrequentTransactionRange
    except Exception as e:
        print(f"[ERROR][utils.standard_func.mostFrequentTransactionRange] {e}")
        return {"min":None,"max": None,"count": None}



def process_transaction(data):
    
    transactionsBetween100000And500000, transactionsBetween10000And100000, transactionsGreater500000, transactionsLess10000 = 0,0,0,0 
    try:        
        df_filtre = data[(data['Montant'].abs() >= 100000) & (data['Montant'].abs() < 500000)]
        
        transactionsBetween100000And500000 = df_filtre.shape[0]
    except Exception as e:
        print(f"[ERROR][utils.standard_func.transactionsBetween100000And500000] {e}")
        transactionsBetween100000And500000=0

    try:        
        df_filtre = data[(data['Montant'].abs() >= 10000) & (data['Montant'].abs() < 100000)]
        transactionsBetween10000And100000 = df_filtre.shape[0]
    except Exception as e:
        print(f"[ERROR][utils.standard_func.transactionsBetween10000And100000] {e}")
        transactionsBetween10000And100000=0


    try:        
        df_filtre = data[(data['Montant'].abs() >= 500000)]
        transactionsGreater500000 = df_filtre.shape[0]
    except Exception as e:
        print(f"[ERROR][utils.standard_func.transactionsGreater500000] {e}")
        transactionsGreater500000=0

    try:        
        df_filtre = data[(data['Montant'].abs() < 10000)]
        transactionsLess10000 = df_filtre.shape[0]
    except Exception as e:
        print(f"[ERROR][utils.standard_func.transactionsLess10000] {e}")
        transactionsLess10000=0

    return transactionsBetween100000And500000, transactionsBetween10000And100000, transactionsGreater500000, transactionsLess10000



def process_transaction_range(data):
    try:        
        data['Montant_temp'] = data['Montant']
        data['Montant_temp'] =data['Montant_temp'].abs()
        valeur_min = data['Montant_temp'].min()
        valeur_max = data['Montant_temp'].max()
        frequence = 1
        while (frequence > 0 and (((valeur_max - valeur_min) / frequence) > 10)):
            frequence = frequence * 10
        if frequence > 10000:
            frequence = 10000

        b_min, _ = attribuer_plage(valeur_min, valeur_min, plage=10000)
        _, b_max = attribuer_plage(valeur_max, valeur_max, plage=10000)
        
        intervalle = pd.interval_range(start=b_min, end=b_max,freq=frequence, closed='right')
        data['Intervalle'] = pd.cut(data['Montant_temp'].abs(), bins=intervalle)
        
        # Obtenir les bornes des intervalles
        intervalle_bornes = pd.IntervalIndex(data['Intervalle']).right
        data['Intervalle_Min'] = pd.IntervalIndex(data['Intervalle']).left
        data['Intervalle_Max'] = pd.IntervalIndex(data['Intervalle']).right

        # Obtenir les statistiques pour chaque intervalle
        interval_stats = data.groupby(['Intervalle_Min', 'Intervalle_Max'])['Montant_temp'].agg([('min', 'min'), ('max', 'max'), ('count', 'count')]).reset_index()

        # Filtrer les lignes avec count différent de zéro
        interval_stats = interval_stats[interval_stats['count'] != 0]
        
        # Afficher le résultat au format demandé
        transactionRanges  = []
        for index, row in interval_stats.iterrows():
            transactionRanges.append({
                "min": int(row['Intervalle_Min']),
                "max": int(row['Intervalle_Max']),
                "count": int(row['count'])
            })
        return transactionRanges 
    except Exception as e:
        print(f"[ERROR][utils.standard_func.transactionRanges ] {e}")
        return []