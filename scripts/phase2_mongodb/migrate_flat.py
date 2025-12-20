import sqlite3
import os
from pymongo import MongoClient

def migrate_flat():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sqlite_path = os.path.join(base_dir, "data", "imdb.db")
    
    if not os.path.exists(sqlite_path):
        return

    sqlite_conn = None
    try:
        sqlite_conn = sqlite3.connect(sqlite_path)
        sqlite_conn.row_factory = sqlite3.Row
        
        mongo_client = MongoClient("mongodb://localhost:27017/")
        db = mongo_client["cineexplorer_db"]

        tables = [
            "Movie", "Person", "Rating", "Genre", "Profession", 
            "TitleAlias", "MovieGenre", "PersonProfession", 
            "MoviePrincipal", "Character", "MovieWriter"
        ]

        for table in tables:
            cursor = sqlite_conn.cursor()
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            documents = [dict(row) for row in rows]

            if documents:
                db[table].drop()
                db[table].insert_many(documents)
                print(f"{table} : {len(documents)} documents migres")

    except Exception as e:
        print(f"Erreur : {e}")
    finally:
        if sqlite_conn:
            sqlite_conn.close()
        mongo_client.close()

if __name__ == "__main__":
    migrate_flat()