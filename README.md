# Projet de détection d'anomalies sur le réseau Tezos

Ce projet applique l'apprentissage automatique pour détecter des anomalies sur les nœuds du réseau Tezos. Le modèle est formé sur des données de performance collectées à partir du réseau et est capable de détecter des comportements anormaux qui pourraient indiquer des problèmes potentiels.

## Arborescence du projet

Le projet est organisé comme suit:

- `data/`: data/: contient les bases de données SQLite avec les données originales (`tezos_metrics.db`, `tezos_metrics2.db`, `tezos_metrics3.db`, `tezos_metrics4.db`).
- `models/`: contient les modèles d'apprentissage automatique formés (`isolation_forest_tezos_metrics.sav`, `lstm_tezos_metrics.h5`).
- `scripts/`: contient différents sous-dossiers avec différents scripts Python:
    - `collect_data`: scripts pour collecter les données du réseau Tezos.
    - `preprocess`: contient le script `preprocess.py` pour le prétraitement des données.
    - `training`: contient les scripts `LSTM.py` et `isolation_forest.py` pour la formation des modèles.
    - `predict`: contient le script `predict.py` pour faire des prédictions sur de nouvelles données.
    - `utils`: contient des scripts auxiliaires pour des fonctions utiles communes.
- `requirements.txt` : Fichier contenant les dépendances Python requises pour ce projet.

## Comment utiliser ce projet

1. Clonez le projet sur votre machine locale.
2. Assurez-vous que toutes les dépendances Python nécessaires sont installées (pandas, sklearn, sqlite3, etc.).
"pip install -r requirements.txt"
3. Utilisez les scripts dans le dossier `collect_data` pour collecter des données du réseau Tezos.
4. Lancez le script `data_preprocessing.py` dans le dossier `preprocess` pour prétraiter les données. Cela crée de nouvelles tables dans votre base de données SQLite avec les données prétraitées.
5. Lancez le script `db_training.py` dans le dossier `training` pour entraîner le modèle. Cela sauvegarde le modèle entraîné dans le répertoire `models/`.
6. Mettez à jour le fichier `new_tezos_metrics.db` avec vos nouvelles données à prédire.
7. Lancez le script `predict.py` dans le dossier `predict` pour faire des prédictions sur les nouvelles données. Cela crée un nouveau fichier CSV dans le répertoire `data/` avec les résultats des prédictions.
