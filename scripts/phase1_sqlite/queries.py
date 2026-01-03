import sqlite3
import os


DB_FILE = os.path.join(os.path.dirname(__file__), '../../data/imdb.db')

def create_connection(db_file):
    """Crée une connexion à la base de données SQLite."""
    try:
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row 
        return conn
    except sqlite3.Error as e:
        print(f"Erreur de connexion à SQLite: {e}")
        return None


def query_actor_filmography(conn, actor_name: str) -> list:
    """
    Retourne la filmographie d’un acteur donné.
    
    Args:
        conn: Connexion SQLite
        actor_name: Nom de l’acteur (ex: "Tom Hanks")
    Returns:
        Liste de tuples (titre, année, rôle/job, note)
        
    SQL utilisé:
    SELECT m.primaryTitle, m.startYear, mp.job, r.averageRating
    FROM Person pe
    JOIN MoviePrincipal mp ON pe.person_id = mp.person_id
    JOIN Movie m ON mp.movie_id = m.movie_id
    LEFT JOIN Rating r ON m.movie_id = r.movie_id
    WHERE pe.primaryName LIKE ? AND mp.category IN ('actor', 'actress')
    ORDER BY m.startYear DESC
    """
    sql = """
    SELECT 
        m.primaryTitle, 
        m.startYear, 
        mp.job, 
        r.averageRating
    FROM 
        Person pe
    JOIN 
        MoviePrincipal mp ON pe.person_id = mp.person_id
    JOIN 
        Movie m ON mp.movie_id = m.movie_id
    LEFT JOIN 
        Rating r ON m.movie_id = r.movie_id
    WHERE 
        pe.primaryName LIKE ? AND mp.category IN ('actor', 'actress')
    ORDER BY 
        m.startYear DESC
    """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()



def query_top_n_films(conn, genre: str, start_year: int, end_year: int, n: int) -> list:
    """
    Retourne les N meilleurs films d’un genre sur une période.
    
    Args:
        conn: Connexion SQLite
        genre: Nom du genre (ex: "Drama")
        start_year: Année de début (ex: 1990)
        end_year: Année de fin (ex: 1999)
        n: Nombre de films à retourner (ex: 10)
    Returns:
        Liste de tuples (titre, année, note, votes)
        
    SQL utilisé:
    SELECT m.primaryTitle, m.startYear, r.averageRating, r.numVotes
    FROM Movie m
    JOIN MovieGenre mg ON m.movie_id = mg.movie_id
    JOIN Rating r ON m.movie_id = r.movie_id
    WHERE 
        mg.genre_name = ? AND m.startYear BETWEEN ? AND ?
    ORDER BY r.averageRating DESC, r.numVotes DESC
    LIMIT ?
    """
    sql = """
    SELECT 
        m.primaryTitle, 
        m.startYear, 
        r.averageRating, 
        r.numVotes
    FROM 
        Movie m
    JOIN 
        MovieGenre mg ON m.movie_id = mg.movie_id
    JOIN 
        Rating r ON m.movie_id = r.movie_id
    WHERE 
        mg.genre_name = ? 
        AND m.startYear BETWEEN ? AND ? 
        AND m.titleType = 'movie'
    ORDER BY 
        r.averageRating DESC, r.numVotes DESC
    LIMIT ?
    """
    return conn.execute(sql, (genre, start_year, end_year, n)).fetchall()


def query_multi_role_actors(conn) -> list:
    """
    Acteurs ayant joué plusieurs personnages dans un même film, triés par nombre de rôles.
    (La colonne 'job' dans MoviePrincipal contient le personnage/rôle.)
    
    SQL utilisé:
    SELECT pe.primaryName, m.primaryTitle, COUNT(mp.job) AS num_roles
    FROM MoviePrincipal mp
    JOIN Person pe ON mp.person_id = pe.person_id
    JOIN Movie m ON mp.movie_id = m.movie_id
    WHERE mp.category IN ('actor', 'actress') AND mp.job IS NOT NULL AND mp.job != ''
    GROUP BY pe.person_id, m.movie_id
    HAVING COUNT(mp.job) > 1
    ORDER BY num_roles DESC;
    """
    sql = """
    SELECT 
        pe.primaryName, 
        m.primaryTitle, 
        COUNT(mp.job) AS num_roles
    FROM 
        MoviePrincipal mp
    JOIN 
        Person pe ON mp.person_id = pe.person_id
    JOIN 
        Movie m ON mp.movie_id = m.movie_id
    WHERE 
        mp.category IN ('actor', 'actress') 
        AND mp.job IS NOT NULL 
        AND mp.job != ''
    GROUP BY 
        pe.person_id, m.movie_id
    HAVING 
        COUNT(mp.job) > 1
    ORDER BY 
        num_roles DESC, pe.primaryName
    LIMIT 10;
    """
    return conn.execute(sql).fetchall()


def query_collaborations(conn, actor_name: str) -> list:
    """
    Réalisateurs ayant travaillé avec un acteur spécifique, avec le nombre de films ensemble.
    
    SQL utilisé:
    SELECT 
        d.primaryName AS director_name, 
        COUNT(m.movie_id) AS collaboration_count
    FROM 
        Movie m
    JOIN 
        MoviePrincipal mp_d ON m.movie_id = mp_d.movie_id AND mp_d.category = 'director'
    JOIN 
        Person d ON mp_d.person_id = d.person_id
    WHERE 
        m.movie_id IN (
            SELECT movie_id FROM MoviePrincipal mp_a
            JOIN Person pa ON mp_a.person_id = pa.person_id
            WHERE pa.primaryName LIKE ? AND mp_a.category IN ('actor', 'actress')
        )
    GROUP BY 
        director_name
    ORDER BY 
        collaboration_count DESC;
    """
    sql = """
    SELECT 
        d.primaryName AS director_name, 
        COUNT(m.movie_id) AS collaboration_count
    FROM 
        Movie m
    JOIN 
        MoviePrincipal mp_d ON m.movie_id = mp_d.movie_id AND mp_d.category = 'director'
    JOIN 
        Person d ON mp_d.person_id = d.person_id
    WHERE 
        m.movie_id IN (
            SELECT movie_id FROM MoviePrincipal mp_a
            JOIN Person pa ON mp_a.person_id = pa.person_id
            WHERE pa.primaryName LIKE ? AND mp_a.category IN ('actor', 'actress')
        )
    GROUP BY 
        director_name
    ORDER BY 
        collaboration_count DESC
    LIMIT 10;
    """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()



def query_popular_genres(conn) -> list:
    """
    Genres ayant une note moyenne > 7.0 et plus de 50 films, triés par note.
    
    SQL utilisé:
    SELECT mg.genre_name, AVG(r.averageRating) AS avg_rating, COUNT(m.movie_id) AS film_count
    FROM Movie m
    JOIN MovieGenre mg ON m.movie_id = mg.movie_id
    JOIN Rating r ON m.movie_id = r.movie_id
    GROUP BY mg.genre_name
    HAVING AVG(r.averageRating) > 7.0 AND COUNT(m.movie_id) > 50
    ORDER BY avg_rating DESC, film_count DESC;
    """
    sql = """
    SELECT 
        mg.genre_name, 
        AVG(r.averageRating) AS avg_rating, 
        COUNT(m.movie_id) AS film_count
    FROM 
        Movie m
    JOIN 
        MovieGenre mg ON m.movie_id = mg.movie_id
    JOIN 
        Rating r ON m.movie_id = r.movie_id
    GROUP BY 
        mg.genre_name
    HAVING 
        AVG(r.averageRating) > 7.0 AND COUNT(m.movie_id) > 50
    ORDER BY 
        avg_rating DESC, film_count DESC;
    """
    return conn.execute(sql).fetchall()



def query_career_evolution(conn, actor_name: str) -> list:
    """
    Pour un acteur donné, nombre de films par décennie avec note moyenne.
    
    SQL utilisé:
    WITH ActorFilms AS (
        SELECT m.movie_id, m.startYear, r.averageRating
        FROM Person pe
        JOIN MoviePrincipal mp ON pe.person_id = mp.person_id
        JOIN Movie m ON mp.movie_id = m.movie_id
        LEFT JOIN Rating r ON m.movie_id = r.movie_id
        WHERE pe.primaryName LIKE ? AND mp.category IN ('actor', 'actress')
    )
    SELECT 
        CAST(startYear / 10 AS INT) * 10 AS decade,
        COUNT(movie_id) AS num_films,
        AVG(averageRating) AS avg_rating
    FROM 
        ActorFilms
    WHERE startYear IS NOT NULL
    GROUP BY decade
    ORDER BY decade;
    """
    sql = """
    WITH ActorFilms AS (
        SELECT 
            m.movie_id, 
            m.startYear, 
            r.averageRating
        FROM 
            Person pe
        JOIN 
            MoviePrincipal mp ON pe.person_id = mp.person_id
        JOIN 
            Movie m ON mp.movie_id = m.movie_id
        LEFT JOIN 
            Rating r ON m.movie_id = r.movie_id
        WHERE 
            pe.primaryName LIKE ? AND mp.category IN ('actor', 'actress')
    )
    SELECT 
        -- Calcul de la décennie : 1987 -> 198, * 10 = 1980
        CAST(startYear / 10 AS INT) * 10 AS decade,
        COUNT(movie_id) AS num_films,
        AVG(averageRating) AS avg_rating
    FROM 
        ActorFilms
    WHERE startYear IS NOT NULL
    GROUP BY decade
    ORDER BY decade;
    """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()


def query_genre_ranking(conn) -> list:
    """
    Pour chaque genre, les 3 meilleurs films avec leur rang.
    
    SQL utilisé:
    SELECT *
    FROM (
        SELECT 
            mg.genre_name, 
            m.primaryTitle, 
            r.averageRating,
            RANK() OVER (PARTITION BY mg.genre_name ORDER BY r.averageRating DESC, r.numVotes DESC) as rank_within_genre
        FROM Movie m
        JOIN MovieGenre mg ON m.movie_id = mg.movie_id
        JOIN Rating r ON m.movie_id = r.movie_id
        WHERE r.numVotes > 1000 AND m.titleType = 'movie'
    )
    WHERE rank_within_genre <= 3
    ORDER BY genre_name, rank_within_genre;
    """
    sql = """
    SELECT *
    FROM (
        SELECT 
            mg.genre_name, 
            m.primaryTitle, 
            r.averageRating,
            RANK() OVER (PARTITION BY mg.genre_name ORDER BY r.averageRating DESC, r.numVotes DESC) as rank_within_genre
        FROM Movie m
        JOIN MovieGenre mg ON m.movie_id = mg.movie_id
        JOIN Rating r ON m.movie_id = r.movie_id
        -- Filtrer pour avoir un classement plus pertinent
        WHERE r.numVotes > 1000 AND m.titleType = 'movie'
    )
    WHERE rank_within_genre <= 3
    ORDER BY genre_name, rank_within_genre;
    """
    return conn.execute(sql).fetchall()



def query_breakout_career(conn) -> list:
    """
    Personnes ayant percé grâce à un film : (avant : films < 200k votes, après : films > 200k votes).
    
    Note: SQLite n'a pas de CTE récursif. On utilise une simple jointure sur la Personne ayant
    un rôle dans les deux catégories de films (populaires et non populaires).
    
    SQL utilisé:
    SELECT DISTINCT 
        pe.primaryName
    FROM 
        Person pe
    JOIN 
        MoviePrincipal mp_low ON pe.person_id = mp_low.person_id
    JOIN 
        Rating r_low ON mp_low.movie_id = r_low.movie_id
    JOIN 
        MoviePrincipal mp_high ON pe.person_id = mp_high.person_id
    JOIN 
        Rating r_high ON mp_high.movie_id = r_high.movie_id
    WHERE 
        r_low.numVotes < 200000 
        AND r_high.numVotes > 200000 
        AND mp_low.category IN ('actor', 'actress', 'director') 
        AND mp_high.category IN ('actor', 'actress', 'director')
    LIMIT 10;
    """
    sql = """
    SELECT DISTINCT 
        pe.primaryName
    FROM 
        Person pe
    JOIN 
        MoviePrincipal mp_low ON pe.person_id = mp_low.person_id
    JOIN 
        Rating r_low ON mp_low.movie_id = r_low.movie_id
    JOIN 
        MoviePrincipal mp_high ON pe.person_id = mp_high.person_id
    JOIN 
        Rating r_high ON mp_high.movie_id = r_high.movie_id
    WHERE 
        r_low.numVotes < 200000 
        AND r_high.numVotes > 200000 
        AND mp_low.category IN ('actor', 'actress', 'director') 
        AND mp_high.category IN ('actor', 'actress', 'director')
    LIMIT 10;
    """
    return conn.execute(sql).fetchall()




def query_free_style(conn) -> list:
    """
    Acteurs célèbres (plus de 10 films, note moyenne de leurs films > 7.0) 
    qui n'ont jamais été réalisateurs.
    
    SQL utilisé:
    SELECT 
        pe.primaryName, 
        COUNT(m.movie_id) AS num_films, 
        AVG(r.averageRating) AS avg_rating
    FROM 
        Person pe
    JOIN 
        MoviePrincipal mp ON pe.person_id = mp.person_id AND mp.category IN ('actor', 'actress')
    JOIN 
        Movie m ON mp.movie_id = m.movie_id
    JOIN 
        Rating r ON m.movie_id = r.movie_id
    WHERE 
        pe.person_id NOT IN (SELECT person_id FROM MoviePrincipal WHERE category = 'director')
    GROUP BY 
        pe.person_id
    HAVING 
        COUNT(m.movie_id) >= 10 AND AVG(r.averageRating) > 7.0
    ORDER BY 
        avg_rating DESC, num_films DESC
    LIMIT 10;
    """
    sql = """
    SELECT 
        pe.primaryName, 
        COUNT(m.movie_id) AS num_films, 
        AVG(r.averageRating) AS avg_rating
    FROM 
        Person pe
    JOIN 
        MoviePrincipal mp ON pe.person_id = mp.person_id AND mp.category IN ('actor', 'actress')
    JOIN 
        Movie m ON mp.movie_id = m.movie_id
    JOIN 
        Rating r ON m.movie_id = r.movie_id
    WHERE 
        -- Sous-requête pour exclure ceux qui sont aussi réalisateurs
        pe.person_id NOT IN (SELECT person_id FROM MoviePrincipal WHERE category = 'director')
    GROUP BY 
        pe.person_id
    HAVING 
        COUNT(m.movie_id) >= 10 AND AVG(r.averageRating) > 7.0
    ORDER BY 
        avg_rating DESC, num_films DESC
    LIMIT 10;
    """
    return conn.execute(sql).fetchall()




def main():
    conn = create_connection(DB_FILE)
    if conn:
        print("--- Début des tests des 9 requêtes ---")
        
        
        ACTOR_NAME = "Fred Astaire" 
        GENRE = "Action"
        START_YEAR = 2000
        END_YEAR = 2020
        N = 5
        
        
        print(f"\n[Q1] Filmographie de {ACTOR_NAME}:")
        r = query_actor_filmography(conn, ACTOR_NAME)
        for row in r[:3]: print(f"- {row['primaryTitle']} ({row['startYear']}) | Rôle: {row['job'] or 'N/A'}")

        
        print(f"\n[Q2] Top {N} films '{GENRE}' de {START_YEAR}-{END_YEAR}:")
        r = query_top_n_films(conn, GENRE, START_YEAR, END_YEAR, N)
        for row in r: print(f"- {row['primaryTitle']} ({row['startYear']}) | Note: {row['averageRating']}")

        
        print("\n[Q3] Acteurs multi-rôles (Top 5):")
        r = query_multi_role_actors(conn)
        for row in r[:5]: print(f"- {row['primaryName']} dans '{row['primaryTitle']}' | Rôles: {row['num_roles']}")
        
        
        print(f"\n[Q4] Collaborations avec {ACTOR_NAME}:")
        r = query_collaborations(conn, ACTOR_NAME)
        for row in r: print(f"- {row['director_name']} | Films: {row['collaboration_count']}")

        
        print("\n[Q5] Genres populaires (Note moyenne > 7.0, > 50 films):")
        r = query_popular_genres(conn)
        for row in r[:5]: print(f"- {row['genre_name']} | Note: {row['avg_rating']:.2f} | Films: {row['film_count']:,}")

        
        print(f"\n[Q6] Évolution de carrière de {ACTOR_NAME} par décennie:")
        r = query_career_evolution(conn, ACTOR_NAME)
        for row in r: print(f"- Décennie {row['decade']} | Films: {row['num_films']} | Note Moyenne: {row['avg_rating']:.2f}")

        
        print("\n[Q7] Classement par genre (Top 3 par genre, votes > 1000, 3 premiers genres affichés):")
        r = query_genre_ranking(conn)
        
        
        genres_seen = set()
        count = 0
        for row in r:
            if row['genre_name'] not in genres_seen:
                if len(genres_seen) >= 3:
                    break
                genres_seen.add(row['genre_name'])
                print(f"--- Genre: {row['genre_name']} ---")
            print(f"  {row['rank_within_genre']}. {row['primaryTitle']} ({row['averageRating']})")

        
        print("\n[Q8] Personnes ayant percé (Top 10):")
        r = query_breakout_career(conn)
        for row in r: print(f"- {row['primaryName']}")
        
        
        print("\n[Q9] Acteurs célèbres n'ayant jamais réalisé (Requête libre):")
        r = query_free_style(conn)
        for row in r: print(f"- {row['primaryName']} | Films: {row['num_films']} | Note Moyenne: {row['avg_rating']:.2f}")

        conn.close()

if __name__ == '__main__':
    main()