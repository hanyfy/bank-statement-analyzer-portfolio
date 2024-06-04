# bank-statement-analyzer-portfolio
BankStatementAnalyzerAPI est une interface de programmation d'application (API) puissante qui offre la capacité de lire des fichiers PDF de relevés bancaires et de calculer plusieurs indicateurs clés de performance (KPI) d'analyse financière. 

# BankStatementAnalyzerAPI

BankStatementAnalyzerAPI est une API puissante conçue pour lire les relevés bancaires au format PDF et calculer plusieurs indicateurs clés de performance (KPI) pour l'analyse financière. Cette API automatise le processus d'analyse des relevés bancaires, offrant ainsi une solution efficace pour les entreprises et les développeurs souhaitant extraire des informations précieuses de leurs données financières.

![Aperçu du projet](images/flow_score.png)

## Fonctionnalités

- **Analyse de Fichiers PDF** : BankStatementAnalyzerAPI peut traiter les relevés bancaires au format PDF provenant de différentes banques (ACESS BANK, ECO BANK, FCMB BANK, STERLING BANK, UNION BANK, WEMA BANK, ZENITH BANK).
  
- **Extraction des Données** : L'API extrait automatiquement les informations pertinentes des relevés bancaires, telles que les transactions, les soldes, les dates, etc.
  
- **Calcul des KPI d'Analyse Financière** : BankStatementAnalyzerAPI effectue divers calculs pour générer des KPI significatifs, tels que le solde moyen, les dépenses mensuelles, les revenus nets, etc.
  
- **Intégration Facile** : L'API est conçue pour être facilement intégrée dans n'importe quelle application existante, offrant ainsi une solution robuste pour l'automatisation de l'analyse des relevés bancaires.




## Outils et Technologies
Le projet utilise les technologies et outils suivants :
- **Langages de programmation** : Python, Java
- **Frameworks et bibliothèques** : Fastapi, Pandas, Tabula 
- **Bases de données** : 
- **Formats de données** : JSON, JWT
- **Conteneurisation** : Docker


## Installation
Pour installer et exécuter ce projet localement, veuillez suivre les étapes ci-dessous :

1. Clonez le dépôt :
    ```bash
    git clone -b main --depth=1  https://github.com/hanyfy/bank-statement-analyzer-portfolio.git
    ```

2. Accédez au répertoire du projet :
    ```bash
    cd bank-statement-analyzer-portfoli/source
    ```

3. Installez les dépendances :
    ```bash
    docker-compose up --build
    ```

    

4. Accédez à l'application via votre navigateur à l'adresse :
    ```
    http://127.0.0.1:8083/docs (pour la documentation de l'api)
    ```



## Utilisation

Voici un exemple d'utilisation de BankStatementAnalyzerAPI en Python :

```python
import requests

# URL de l'API
api_url = "https:/localhost:8083/calcul"

# Charger le fichier PDF du relevé bancaire
fichier_pdf = {
        
    "files": [
        {
            "file": {
                "path": "bank_statement.pdf",
                "name": "bank_statement.pdf",
                "baseUrl": "https:/~~/",
                "accessCode": 6521
            },
            "analysisType": {
                "id": 1,
                "name": "Relevé bancaire",
                "code": "BANK"
            },
            "fileType": {
                "id": 1,
                "name": "PDF",
                "code": "PDF"
            },
            "institution": {
                "id": 4,
                "name": "ACCESS BANK",
                "address": "Bank adress",
                "phone": "981987",
                "city": "test",
                "country": {
                    "id": 2,
                    "name": "Nigeria",
                    "code": "NGA"
                }
            }
        }
    ]
}


# Envoyer une requête POST avec le fichier PDF
reponse = requests.post(api_url, json=fichier_pdf, auth=("admin@admin.fr", "F$7wB#2nK*9v"))

# Afficher la réponse de l'API
print(reponse.json())
```



## Contribution
Les contributions sont les bienvenues ! Si vous souhaitez contribuer, veuillez créer une branche à partir de `main`, apporter vos modifications, puis soumettre une pull request.

## Licence
Ce projet est sous licence MIT. Pour plus de détails, veuillez consulter le fichier `LICENSE`.

## Auteurs
- [RAMAMONJISOA Nomenjanahary Hany Fy](https://github.com/hanyfy)
- [ramamonjisoafy@gmail.com](https://github.com/hanyfy)

Merci de votre intérêt pour ce projet ! N'hésitez pas à me contacter pour toute question ou suggestion.