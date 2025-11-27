# 🚀 ETL Pipeline avec Apache Airflow

Un projet **Data Engineering** qui démontre la maîtrise d’Apache Airflow pour orchestrer un pipeline **ETL complet** : téléchargement, extraction multi-formats, consolidation et transformation des données.  
Ce dépôt illustre ma capacité à concevoir des workflows robustes, automatisés et réutilisables, qualités essentielles pour un Data Engineer.

---

## 🎯 Objectifs du projet
- **Automatiser** le traitement de données hétérogènes (CSV, TSV, Fixed-Width).
- **Orchestrer** les tâches avec Apache Airflow pour un pipeline fiable et maintenable.
- **Transformer** les données pour les rendre exploitables et prêtes à l’analyse.
- **Montrer** mes compétences pratiques en Data Engineering à travers un projet concret.

---

## 🛠️ Technologies utilisées
- **Python 3** : langage principal pour l’ETL.
- **Apache Airflow** : orchestration et automatisation des tâches.
- **Requests** : téléchargement des données.
- **Tarfile / CSV** : extraction et manipulation des fichiers.
- **Pendulum** : gestion des dates dans Airflow.

---
📂 Structure du projet

python-etl-airflow/
│
├── dags
│
│   └── etl_toll_data.py        **Ton DAG Airflow (le code que tu as partagé)**
│
├── staging                     **Dossier pour les fichiers temporaires (optionnel, peut être ignoré dans GitHub)**
│
├── requirements.txt            **Dépendances Python**
│
├── README.md                   **Documentation du projet**
│
├── .gitignore                  **Fichiers à ignorer (logs, staging, etc.)**
│
└── LICENSE                     **Licence open-source (MIT par exemple)**

## 🔎 Explication étape par étape du pipeline

### 1️⃣ **Download dataset**
- **Méthode utilisée :** `requests.get()` avec gestion du flux et timeout.
- **But :** Télécharger un fichier compressé `.tgz` depuis une source externe.
---

### 2️⃣ **Untar dataset**
- **Méthode utilisée :** `tarfile.open()` pour extraire les fichiers.
- **But :** Décompresser le jeu de données brut.
---

### 3️⃣ **Extract data (CSV, TSV, Fixed-Width)**
- **Méthodes utilisées :**
  - `csv.writer()` pour normaliser les données.
  - `split(',')`, `split('\t')` et slicing pour gérer différents formats.
- **But :** Extraire et uniformiser les données de trois formats distincts.
---

### 4️⃣ **Consolidate data**
- **Méthode utilisée :** `zip()` pour fusionner les lignes des trois fichiers.
- **But :** Créer un fichier unique `extracted_data.csv` regroupant toutes les informations.
 
---

### 5️⃣ **Transform data**
- **Méthode utilisée :** `csv.DictReader()` et `DictWriter()` pour manipuler les colonnes.
- **But :** Nettoyer et transformer les données (ex. mettre les types de véhicules en majuscules).

---

## 📊 Architecture du DAG Airflow

Download → Untar → [Extract CSV, Extract TSV, Extract Fixed-Width] → Consolidate → Transform

Chaque tâche est définie comme un **PythonOperator** et reliée par des dépendances claires, garantissant un pipeline **fiable et reproductible**.

---

💡 Points forts démontrés

- Orchestration maîtrisée avec Airflow.

- Gestion multi-formats (CSV, TSV, Fixed-Width).

- Pipeline robuste et réutilisable grâce aux chemins relatifs et à la modularité.

- Transformation de données pour les rendre prêtes à l’analyse.



👨‍💻 Auteur

- **Nom** : El Hadji Ablaye Galoup DIOP 📧
- **Email** : elhadjiablayegaloupdiop@gmail.com �
