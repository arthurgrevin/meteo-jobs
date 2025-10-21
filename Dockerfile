# Étape 1 : Image de base
FROM python:3.12-slim

# Étape 2 : Répertoire de travail
WORKDIR /app

# Étape 3 : Installer les dépendances système utiles (psycopg2, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
 && rm -rf /var/lib/apt/lists/*

# Étape 4 : Copier les fichiers
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Étape 5 : Lancer ton script
CMD ["python", "start_EL.py"]
