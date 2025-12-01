-- scripts/phase1_sqlite/create_indexes.sql

-- ====================================================================
-- Indexation basée sur les requêtes Q1 à Q9
-- ====================================================================

-- Index pour la recherche rapide de personnes par nom (Q1, Q4, Q6)
CREATE INDEX idx_person_name ON Person (primaryName);

-- Index pour les jointures Person -> MoviePrincipal et le filtre de catégorie (Q1, Q4, Q8)
-- L'index composite (person_id, category) est très efficace.
CREATE INDEX idx_principal_person_cat ON MoviePrincipal (person_id, category);

-- Index pour la recherche par genre et les partitions de genre (Q2, Q5, Q7)
-- L'index composite (genre_name, movie_id) permet d'accéder rapidement aux films d'un genre.
CREATE INDEX idx_genre_name_movieid ON MovieGenre (genre_name, movie_id);

-- Index pour la recherche par titre alias (Q9 utilise une sous-requête sur Person)
CREATE INDEX idx_titlealias_title ON TitleAlias (title);

-- Index pour le classement et le filtrage des notes (Q2, Q5, Q7, Q8)
-- L'index (averageRating DESC, numVotes DESC) est crucial pour les classements.
CREATE INDEX idx_rating_ranking ON Rating (averageRating DESC, numVotes DESC);

-- Index pour le filtrage par nombre de votes (Q8)
CREATE INDEX idx_rating_numvotes ON Rating (numVotes);

-- Index pour les dates de films (Q2, Q6)
CREATE INDEX idx_movie_startyear ON Movie (startYear);

-- Index pour la recherche rapide du nombre de rôles par personne/film (Q3)
CREATE INDEX idx_principal_movie_person ON MoviePrincipal (movie_id, person_id, job);