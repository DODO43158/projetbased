# scripts/phase1_sqlite/create_schema.py

import sqlite3
import os

# Nom du fichier de base de données
DB_FILE = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')

def create_connection(db_file):
    """Crée une connexion à la base de données SQLite spécifiée par db_file."""
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Connexion à la base de données réussie : {db_file}")
        return conn
    except sqlite3.Error as e:
        print(f"Erreur de connexion à SQLite: {e}")
        return None

def create_tables(conn):
    """Crée toutes les tables du schéma relationnel."""
    
    # Définitions SQL pour les tables
    sql_statements = [
        # Table Person (Clé principale)
        """
        CREATE TABLE IF NOT EXISTS Person (
            person_id TEXT PRIMARY KEY,
            primaryName TEXT NOT NULL,
            birthYear INTEGER,
            deathYear INTEGER
        );
        """,
        # Table Movie (Clé principale)
        """
        CREATE TABLE IF NOT EXISTS Movie (
            movie_id TEXT PRIMARY KEY,
            titleType TEXT,
            primaryTitle TEXT NOT NULL,
            originalTitle TEXT NOT NULL,
            isAdult BOOLEAN,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER
        );
        """,
        # Table Rating (Relation 1:1 avec Movie)
        """
        CREATE TABLE IF NOT EXISTS Rating (
            movie_id TEXT PRIMARY KEY,
            averageRating REAL,
            numVotes INTEGER,
            FOREIGN KEY (movie_id) REFERENCES Movie (movie_id) ON DELETE CASCADE
        );
        """,
        # Table Genre (Liste de valeurs uniques)
        """
        CREATE TABLE IF NOT EXISTS Genre (
            genre_name TEXT PRIMARY KEY
        );
        """,
        # Table Profession (Liste de valeurs uniques)
        """
        CREATE TABLE IF NOT EXISTS Profession (
            job_name TEXT PRIMARY KEY
        );
        """,
        # Table TitleAlias (Relation N:M avec Movie - via `titles.csv`)
        """
        CREATE TABLE IF NOT EXISTS TitleAlias (
            movie_id TEXT NOT NULL,
            ordering INTEGER NOT NULL,
            title TEXT NOT NULL,
            region TEXT,
            language TEXT,
            types TEXT,
            attributes TEXT,
            isOriginalTitle BOOLEAN,
            PRIMARY KEY (movie_id, ordering, title),
            FOREIGN KEY (movie_id) REFERENCES Movie (movie_id) ON DELETE CASCADE
        );
        """,
        # Table MovieGenre (Relation N:M entre Movie et Genre)
        """
        CREATE TABLE IF NOT EXISTS MovieGenre (
            movie_id TEXT NOT NULL,
            genre_name TEXT NOT NULL,
            PRIMARY KEY (movie_id, genre_name),
            FOREIGN KEY (movie_id) REFERENCES Movie (movie_id) ON DELETE CASCADE,
            FOREIGN KEY (genre_name) REFERENCES Genre (genre_name) ON DELETE CASCADE
        );
        """,
        # Table PersonProfession (Relation N:M entre Person et Profession)
        """
        CREATE TABLE IF NOT EXISTS PersonProfession (
            person_id TEXT NOT NULL,
            job_name TEXT NOT NULL,
            PRIMARY KEY (person_id, job_name),
            FOREIGN KEY (person_id) REFERENCES Person (person_id) ON DELETE CASCADE,
            FOREIGN KEY (job_name) REFERENCES Profession (job_name) ON DELETE CASCADE
        );
        """,
        # Table MoviePrincipal (Acteurs/Réalisateurs/Scénaristes - de `principals.csv`)
        """
        CREATE TABLE IF NOT EXISTS MoviePrincipal (
            movie_id TEXT NOT NULL,
            person_id TEXT NOT NULL,
            ordering INTEGER NOT NULL,
            category TEXT,
            job TEXT,
            PRIMARY KEY (movie_id, person_id, ordering),
            FOREIGN KEY (movie_id) REFERENCES Movie (movie_id) ON DELETE CASCADE,
            FOREIGN KEY (person_id) REFERENCES Person (person_id) ON DELETE CASCADE
        );
        """,
        # Table Character (Personnages joués - de `characters.csv`)
        """
        CREATE TABLE IF NOT EXISTS Character (
            movie_id TEXT NOT NULL,
            person_id TEXT NOT NULL,
            character_name TEXT NOT NULL,
            PRIMARY KEY (movie_id, person_id, character_name),
            FOREIGN KEY (movie_id, person_id) REFERENCES MoviePrincipal (movie_id, person_id) ON DELETE CASCADE
        );
        """
    ]

    try:
        cursor = conn.cursor()
        for statement in sql_statements:
            cursor.execute(statement)
        conn.commit()
        print("Toutes les tables ont été créées avec succès.")
    except sqlite3.Error as e:
        print(f"Erreur lors de la création des tables: {e}")

def main():
    """Fonction principale pour créer la base de données et les tables."""
    # S'assurer que le répertoire 'data' existe
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    
    # Supprimer l'ancienne base pour garantir un nouveau départ
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Ancienne base de données supprimée: {DB_FILE}")

    conn = create_connection(DB_FILE)
    if conn is not None:
        create_tables(conn)
        conn.close()

if __name__ == '__main__':
    main()