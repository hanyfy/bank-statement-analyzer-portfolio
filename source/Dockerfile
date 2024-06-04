# Utilisez une image de base avec Python
FROM python:3.9

# Installez Java
RUN apt-get update && \
    apt-get install -y default-jre && \
    apt-get install -y default-jdk

# Copiez le contenu de votre application Django dans le conteneur
COPY . /app/API

# Définissez le répertoire de travail
WORKDIR /app/API

# Installez les dépendances Python depuis le fichier requirements.txt
RUN pip install -r requirements.txt

# Exécutez la commande pour démarrer le serveur Django
CMD ["python3", "main.py"]