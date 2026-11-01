# Classification d'offres d'emploi en temps réel (Kafka + Spark Streaming)

Mini-projet Big Data (INPT) : des offres d'emploi arrivent en continu dans **Kafka**, **Spark Structured Streaming** les classe dans quatre domaines (**Data, Développement, Sécurité, Cloud**), puis les résultats sont indexés dans **Elasticsearch** et visualisés dans **Kibana**.

```
offres.txt ─► Producer Python ─► Kafka (topic job-offers) ─► Spark Structured Streaming ─► Elasticsearch ─► Kibana
                                                              ├─ Level 1 : règles par mots-clés
                                                              └─ Level 2 : modèle Spark MLlib
```

## Niveaux réalisés

| Niveau | Contenu | Scripts |
|---|---|---|
| Level 1 | Producer, Kafka, Spark Streaming, classification par mots-clés | `producer.py`, `stream_level1.py` |
| Level 2 | Classification avec Spark MLlib (TF-IDF + régression logistique) | `train_model.py`, `stream_level2.py` |
| Level 3 | Indexation dans Elasticsearch et tableau de bord Kibana | `stream_level3.py` |
| Level 4 (optionnel) | Lecture d'une source réelle (Bluesky, API d'offres) | `social_producer.py`, `jobs_producer.py` |

Le Level 5 (vidéo) est remplacé par une présentation.

## Prérequis (Windows, PowerShell)

| Outil | Version | Remarque |
|---|---|---|
| Python | **3.11** (3.10 accepté) | PySpark 3.5.1 ne fonctionne pas correctement avec Python 3.13 |
| Java (JDK) | **17 ou 21** | Java 23 et plus provoque l'erreur `getSubject is not supported` |
| Docker Desktop | à jour, démarré | Kafka, Elasticsearch et Kibana tournent dans Docker |
| winutils | Hadoop 3.3.x | Dans `C:\hadoop\bin` (`winutils.exe` et `hadoop.dll`) |
| Git | récent | |

Le chemin du projet ne doit **pas contenir d'espaces** (par exemple `E:\mini-projet`).

### Installer winutils

```powershell
mkdir C:\hadoop\bin -Force
Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe" -OutFile C:\hadoop\bin\winutils.exe
Invoke-WebRequest "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile C:\hadoop\bin\hadoop.dll
Unblock-File C:\hadoop\bin\*
```

Si le téléchargement échoue, récupérez les deux fichiers à la main sur le dépôt GitHub `cdarlint/winutils` (dossier `hadoop-3.3.5\bin`).

## Installation

```powershell
git clone <URL_DU_DEPOT>
cd <NOM_DU_DEPOT>

py -3.11 -m venv venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Environnement à charger dans chaque terminal

Les variables ne durent que pour la session PowerShell en cours. À faire dans **chaque** nouveau terminal (adaptez le nom du dossier JDK, vérifiable avec `Get-ChildItem "C:\Program Files\Eclipse Adoptium"`) :

```powershell
cd E:\mini-projet
venv\Scripts\Activate.ps1
$env:JAVA_HOME   = "C:\Program Files\Eclipse Adoptium\jdk-21.0.8.9-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:Path        = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
java -version      # doit afficher 17 ou 21
```

Si le dépôt contient `env.ps1`, il suffit de : `. .\env.ps1` (avec le point et l'espace).

## Structure du dépôt

```
.
├── docker-compose.yml     # Kafka, Elasticsearch, Kibana
├── requirements.txt       # dépendances Python
├── producer.py            # lit un fichier texte et l'envoie dans Kafka
├── stream_level1.py       # Level 1 : Spark Streaming + mots-clés
├── train_model.py         # Level 2 : entraînement du modèle MLlib
├── stream_level2.py       # Level 2 : prédictions en streaming
├── stream_level3.py       # Level 3 : écriture dans Elasticsearch
├── social_producer.py     # Level 4 (optionnel) : flux Bluesky
├── jobs_producer.py       # Level 4 (optionnel) : API d'offres Arbeitnow
├── test_jetstream.py      # Level 4 : test de connexion à Bluesky
├── data/
│   ├── offres.txt         # offres à streamer (sans label)
│   └── train.csv          # données d'entraînement (label;texte)
└── models/                # modèle généré (ignoré par Git)
```

## Exécution

Ouvrez un terminal par ligne « Terminal ». Lancez **toujours Spark avant le producer** (le job lit à partir de `latest`).

### 0. Démarrer Kafka et créer le topic

```powershell
docker compose up -d kafka
docker ps
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic job-offers --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Test du producer seul (le consumer console doit afficher les offres) :

```powershell
# Terminal A
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --topic job-offers --bootstrap-server localhost:9092 --from-beginning
# Terminal B
python producer.py
```

### Level 1 : mots-clés

```powershell
# Terminal 1 (en premier)
python stream_level1.py
# Terminal 2
python producer.py
```

Attendez `Batch: 0` dans le terminal 1 avant de lancer le producer. Chaque offre s'affiche avec son `domaine`.

### Level 2 : modèle MLlib

```powershell
python train_model.py        # une seule fois : affiche accuracy, f1, matrice de confusion
# Terminal 1
python stream_level2.py
# Terminal 2
python producer.py
```

Le modèle est sauvegardé dans `models\job_classifier`. La sortie affiche `domaine_L1` (mots-clés), `domaine_L2` (modèle) et `confiance` (probabilité maximale).

### Level 3 : Elasticsearch et Kibana

```powershell
docker compose up -d
Invoke-RestMethod http://localhost:9200       # doit répondre avec la version du cluster
```

Kibana : <http://localhost:5601> (1 à 2 minutes pour démarrer).

```powershell
# Terminal 1
python stream_level3.py
# Terminal 2
python producer.py
```

Vérifier l'indexation :

```powershell
Invoke-RestMethod "http://localhost:9200/job-offers-classified/_count"
Invoke-RestMethod "http://localhost:9200/job-offers-classified/_search?size=3"
```

Dans Kibana :

1. **Stack Management → Data Views → Create data view** : nom et index pattern `job-offers-classified`, champ temps `ts`.
2. **Discover** : choisir la data view et la période *Last 15 minutes*.
3. **Dashboard → Create dashboard** : ajouter un camembert (`domaine`), un histogramme (`ts`, découpé par `domaine`) et une métrique (Count).
4. Activer le rafraîchissement automatique toutes les 5 secondes.

### Level 4 (optionnel) : source réelle

Les offres arrivent dans le même topic `job-offers` : le Spark du Level 3 n'est pas modifié.

```powershell
pip install websocket-client
python test_jetstream.py          # affiche 5 posts publics de Bluesky
python social_producer.py         # filtre les posts de recrutement tech et les envoie dans Kafka
python jobs_producer.py           # interroge l'API publique Arbeitnow (1 requête par minute)
```

Les offres retenues sont copiées dans `data\social_capture.txt` et `data\jobs_capture.txt` (ignorés par Git). On peut les rejouer sans Internet :

```powershell
python producer.py data\social_capture.txt 0.5
```

## Résultats

- Level 1 : classification immédiate, mais dépendante de l'ordre des règles.
- Level 2 : accuracy d'environ **0,82** sur le jeu de test (échantillon réduit, donc à interpréter comme un ordre de grandeur).
- Level 3 : tableau de bord Kibana en temps réel (compteur, répartition par domaine, évolution dans le temps).

## Dépannage

| Erreur | Cause | Solution |
|---|---|---|
| `source : The term 'source' is not recognized` | Commande Linux | `venv\Scripts\Activate.ps1` |
| `'...' is not recognized as an internal or external command` au lancement de Spark | Chemin du projet avec des espaces | Déplacer le projet dans un dossier sans espaces |
| `getSubject is not supported` | Java trop récent (23 et plus) | Utiliser le JDK 21 ou 17 (`$env:JAVA_HOME`) |
| `HADOOP_HOME and hadoop.home.dir are unset` | winutils absent ou variable non définie | Installer winutils, définir `$env:HADOOP_HOME = "C:\hadoop"` |
| `No module named 'distutils'` ou `Python worker exited unexpectedly` | Python 3.12 ou 3.13 | Recréer le venv avec Python 3.11 |
| `No module named 'numpy'` / `'pandas'` | Dépendance manquante ou version trop récente | `pip install numpy==1.26.4 pandas==2.1.4` |
| `Failed to find data source: kafka` | Connecteur Kafka non téléchargé | Vérifier la connexion Internet, relancer |
| `NoBrokersAvailable` | Kafka arrêté | Démarrer Docker Desktop, `docker compose up -d`, `docker ps` |
| Rien ne s'affiche dans Spark | Producer lancé avant Spark | Relancer `producer.py` après `Batch: 0` |
| `Path does not exist: ...models/job_classifier` | Modèle non entraîné | `python train_model.py` |
| Conteneur Elasticsearch arrêté (code 137) | Mémoire insuffisante | Créer `C:\Users\<nom>\.wslconfig` avec `memory=4GB`, puis `wsl --shutdown` |
| Dashboard Kibana vide | Mauvaise période ou champ temps | Mettre *Last 1 hour*, vérifier que la data view utilise `ts` |
| Erreur de checkpoint après modification du code | Ancien checkpoint | Supprimer le dossier `checkpoints` |
| Messages `SUCCESS: The process with PID ... has been terminated` | Nettoyage normal de Spark | Aucune action |

## Limites et perspectives

- Peu de données d'entraînement et pas de classe « Autre » : le modèle choisit toujours un domaine, la colonne `confiance` signale les cas incertains.
- Offres hybrides (par exemple *Data Engineer AWS*) difficiles à trancher avec un seul label.
- Un seul broker Kafka, une seule partition, Elasticsearch sans sécurité : configuration de démonstration uniquement.
- Pistes : plus de données et validation croisée, embeddings à la place de TF-IDF, seuil de confiance, déploiement sur un vrai cluster.

## Sources de données et confidentialité

- Les offres de `data/offres.txt` et `data/train.csv` sont des exemples rédigés pour le projet.
- Level 4 : flux public de **Bluesky** (Jetstream) et API publique **Arbeitnow**. Seul le texte des posts ou des offres est conservé, jamais l'identité des auteurs. Citez la source dans toute présentation.
- LinkedIn n'est pas utilisé : il ne propose pas d'API publique pour lire les offres et ses conditions d'utilisation interdisent le scraping.
