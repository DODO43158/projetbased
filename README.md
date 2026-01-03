CineExplore - Projet Bases de Donnée

Ce projet Django permet l'exploration d'une base de données cinématographique en utilisant une architecture hybride : SQLite pour la gestion relationnelle et un Replica Set MongoDB pour les données structurées et la haute disponibilité.
Architecture des 5 Pages

    Accueil : Présentation du projet.

    Recherche : Filtrage des films via SQLite.

    Liste Complète : Vue d'ensemble des films.

    Statistiques : Analyses agrégées via MongoDB.

    Détails du Film : Fiche complète récupérée depuis la collection structurée MongoDB.

Installation et Configuration
1. Préparation de l'environnement

    Installez les dépendances : pip install -r requirements.txt.


2. Lancement du Replica Set MongoDB

Lancez trois instances MongoDB sur les ports suivants :

    Port 27017 : mongod --dbpath data/mongo/db-1 --port 27017 --replSet rs0

    Port 27018 : mongod --dbpath data/mongo/db-2 --port 27018 --replSet rs0

    Port 27019 : mongod --dbpath data/mongo/db-3 --port 27019 --replSet rs0

3. Initialisation du Cluster

Exécutez le script pour configurer le Replica Set :
Bash

python setup_replica.py


Pour peupler la base de données MongoDB utilisée par l'application
Transférez les données de SQLite vers MongoDB :
Étape A
python migrate_flat.py

Ce script crée les collections de base (Movie, Rating, Person, etc.) dans la base cineexplorer_db.
Étape B : Dénormalisation 

Générez la collection optimisée pour l'application :

python migrate_structured.py

Ce script crée la collection movies_complete. C'est cette collection qui est interrogée par la page Détails et les Statistiques pour garantir une performance optimale.

 Lancement de l'application

Une fois les migrations terminées, lancez le serveur Django :

python manage.py runserver

Accédez à l'interface via : http://127.0.0.1:8000
Notes Techniques

    Connexion MongoDB : L'application se connecte via l'URI mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0 pour garantir la tolérance aux pannes.

   