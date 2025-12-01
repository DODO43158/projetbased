# scripts/phase1_sqlite/apply_indexes.py

import sqlite3
import os

DB_FILE = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')
SQL_FILE = os.path.join(os.path.dirname(__file__), 'create_indexes.sql')

try:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    with open(SQL_FILE, 'r') as f:
        sql_script = f.read()
    
    # Exécution des commandes SQL du fichier d'index
    cursor.executescript(sql_script)
    conn.commit()
    
    print(f"✅ Indexation complétée avec succès sur {DB_FILE}.")
    
    # Vérification (optionnel)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%';")
    indexes = [row[0] for row in cursor.fetchall()]
    print(f"Index créés (liste partielle) : {', '.join(indexes[:5])}...")
    
except sqlite3.Error as e:
    print(f"❌ Erreur lors de l'application des index : {e}")
finally:
    if conn:
        conn.close()