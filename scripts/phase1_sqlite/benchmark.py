import sqlite3
import os
import time
from queries import (
    query_actor_filmography,
    query_top_n_films,
    query_multi_role_actors,
    query_collaborations,
    query_popular_genres,
    query_career_evolution,
    query_genre_ranking,
    query_breakout_career,
    query_free_style
)


DB_FILE = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')
NUM_RUNS = 5 
TEST_PARAMS = {
    'actor_name': "Tom Hanks",
    'genre': "Drama",
    'start_year': 1990,
    'end_year': 2000,
    'n': 10
}

def create_connection_benchmark(db_file):
    """Crée une connexion SANS factory pour le benchmark brut."""
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(f"Erreur de connexion à SQLite: {e}")
        return None

def run_benchmark(conn, func, name, *args):
    """Exécute une fonction de requête NUM_RUNS fois et calcule le temps moyen."""
    
    
    func(conn, *args) 

    total_time = 0
    results = None
    
    
    for _ in range(NUM_RUNS):
        start_time = time.time()
        results = func(conn, *args)
        end_time = time.time()
        total_time += (end_time - start_time)
        
    avg_time = total_time / NUM_RUNS
    
    
    return {
        'query_name': name,
        'avg_time_ms': avg_time * 1000,
        'num_results': len(results) if results else 0
    }

def main():
    conn = create_connection_benchmark(DB_FILE)
    if conn is None:
        return

    print(f"--- Benchmark sur {DB_FILE} ({NUM_RUNS} exécutions moyennes) ---")
    
    
    queries_to_test = [
        (query_actor_filmography, "Q1", TEST_PARAMS['actor_name']),
        (query_top_n_films, "Q2", TEST_PARAMS['genre'], TEST_PARAMS['start_year'], TEST_PARAMS['end_year'], TEST_PARAMS['n']),
        (query_multi_role_actors, "Q3"),
        (query_collaborations, "Q4", TEST_PARAMS['actor_name']),
        (query_popular_genres, "Q5"),
        (query_career_evolution, "Q6", TEST_PARAMS['actor_name']),
        (query_genre_ranking, "Q7"),
        (query_breakout_career, "Q8"),
        (query_free_style, "Q9"),
    ]

    benchmark_results = []
    
    for func, name, *args in queries_to_test:
        try:
            result = run_benchmark(conn, func, name, *args)
            benchmark_results.append(result)
        except Exception as e:
            print(f" Erreur lors du benchmark de {name}: {e}")
            benchmark_results.append({'query_name': name, 'avg_time_ms': 'ERROR', 'num_results': 0})

    conn.close()
    
    print("\n| Requête | Temps Moyen (ms) | Nb Résultats |")
    print("| :--- | :--- | :--- |")
    for res in benchmark_results:
        time_str = f"{res['avg_time_ms']:.2f}" if isinstance(res['avg_time_ms'], float) else res['avg_time_ms']
        print(f"| {res['query_name']} | {time_str} | {res['num_results']:,} |")
    
    print("\n--- Fin du Benchmark ---")


if __name__ == '__main__':
    main()