import sqlite3
import pandas as pd
import os
import time


DB_FILE = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')
CSV_DIR = os.path.join(os.path.dirname(__file__), '../../data/csv/')
CHUNK_SIZE = 10000


def create_connection(db_file):
    """Crée une connexion à la base de données."""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(f"Erreur de connexion à SQLite: {e}")
        return None



def import_csv_to_db(conn, csv_filename, table_name, column_map, chunk_processor=None):
    """
    Lit un fichier CSV en mappant les noms de colonnes CSV (messy) aux noms SQL (clean).
    
    Args:
        column_map (dict): { 'Nom_dans_CSV': 'Nom_dans_SQL' }
    """
    full_path = os.path.join(CSV_DIR, csv_filename)
    start_time = time.time()
    total_rows = 0

    try:
        
        messy_csv_cols = [f"('{col}',)" for col in column_map.keys()]
        
        
        rename_map = {f"('{csv_name}',)": sql_name for csv_name, sql_name in column_map.items()}

        
        for chunk in pd.read_csv(
            full_path, 
            chunksize=CHUNK_SIZE, 
            usecols=messy_csv_cols, 
            na_values=['\\N']
        ):
            
            
            chunk = chunk.rename(columns=rename_map)
            
            
            chunk = chunk.replace({'\\N': None, '': None})
            
            
            if chunk_processor:
                chunk = chunk_processor(chunk)
            
           
            chunk = chunk.where(pd.notnull(chunk), None)
            
           
            try:
                chunk.to_sql(table_name, conn, if_exists='append', index=False)
                total_rows += len(chunk)
            except sqlite3.Error as e:
                
                print(f"Erreur d'insertion dans {table_name} pour le chunk de {len(chunk)} lignes: {e}")

        end_time = time.time()
        print(f" Importation de {csv_filename} vers {table_name} terminée. Lignes: {total_rows:,}. Temps: {end_time - start_time:.2f}s")
        return total_rows
        
    except FileNotFoundError:
        print(f" Fichier non trouvé: {full_path}")
        return 0
    except Exception as e:
        print(f" Erreur lors du traitement de {csv_filename}: {e}")
        return 0




def process_movie_genres(df):
    """Normalise la table MovieGenre (N:M) et crée les entrées Genre."""
    
    genres_uniques = df['genre_name'].dropna().unique()
    with create_connection(DB_FILE) as conn:
        for genre in genres_uniques:
            conn.execute("INSERT OR IGNORE INTO Genre (genre_name) VALUES (?)", (genre,))
        conn.commit()

    return df

def process_person_professions(df):
    """Normalise la table PersonProfession (N:M) et crée les entrées Profession."""
    jobs_uniques = df['job_name'].dropna().unique()
    with create_connection(DB_FILE) as conn:
        for job in jobs_uniques:
            conn.execute("INSERT OR IGNORE INTO Profession (job_name) VALUES (?)", (job,))
        conn.commit()

    return df



def main():
    """Fonction principale pour importer toutes les données."""
    conn = create_connection(DB_FILE)
    if conn is None:
        return

    print("Début de l'importation des données...")
    start_global_time = time.time()
    
    stats = {}
    
    
    stats['Person'] = import_csv_to_db(conn, 'persons.csv', 'Person', 
        {'pid': 'person_id', 'primaryName': 'primaryName', 'birthYear': 'birthYear', 'deathYear': 'deathYear'}
    )
    stats['Movie'] = import_csv_to_db(conn, 'movies.csv', 'Movie', 
        {'mid': 'movie_id', 'titleType': 'titleType', 'primaryTitle': 'primaryTitle', 'originalTitle': 'originalTitle', 
         'isAdult': 'isAdult', 'startYear': 'startYear', 'endYear': 'endYear', 'runtimeMinutes': 'runtimeMinutes'}
    )
    
    
    stats['Rating'] = import_csv_to_db(conn, 'ratings.csv', 'Rating', 
        {'mid': 'movie_id', 'averageRating': 'averageRating', 'numVotes': 'numVotes'}
    )
    stats['TitleAlias'] = import_csv_to_db(conn, 'titles.csv', 'TitleAlias', 
        {'mid': 'movie_id', 'ordering': 'ordering', 'title': 'title', 'region': 'region', 'language': 'language', 
         'types': 'types', 'attributes': 'attributes', 'isOriginalTitle': 'isOriginalTitle'}
    )
    
    
    stats['MovieGenre'] = import_csv_to_db(conn, 'genres.csv', 'MovieGenre', 
        {'mid': 'movie_id', 'genre': 'genre_name'}, process_movie_genres 
    ) 
    stats['PersonProfession'] = import_csv_to_db(conn, 'professions.csv', 'PersonProfession', 
        {'pid': 'person_id', 'jobName': 'job_name'}, process_person_professions
    ) 
    
    
    stats['MoviePrincipal'] = import_csv_to_db(conn, 'principals.csv', 'MoviePrincipal', 
        {'mid': 'movie_id', 'ordering': 'ordering', 'pid': 'person_id', 'category': 'category', 'job': 'job'}
    )
    stats['MovieWriter'] = import_csv_to_db(conn, 'writers.csv', 'MovieWriter', 
        {'mid': 'movie_id', 'pid': 'person_id'}
    )
    
    conn.close()
    
    end_global_time = time.time()
    
    print("\n--- STATISTIQUES D'IMPORTATION ---")
    for table, count in stats.items():
        print(f"Table {table:<18}: {count:,} lignes insérées")
    print("-" * 37)
    print(f"Temps total d'importation : {end_global_time - start_global_time:.2f} secondes.")

if __name__ == '__main__':
    main()