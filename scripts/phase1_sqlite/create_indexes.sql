
CREATE INDEX idx_person_name ON Person (primaryName);


CREATE INDEX idx_principal_person_cat ON MoviePrincipal (person_id, category);


CREATE INDEX idx_genre_name_movieid ON MovieGenre (genre_name, movie_id);


CREATE INDEX idx_titlealias_title ON TitleAlias (title);


CREATE INDEX idx_rating_ranking ON Rating (averageRating DESC, numVotes DESC);


CREATE INDEX idx_rating_numvotes ON Rating (numVotes);


CREATE INDEX idx_movie_startyear ON Movie (startYear);


CREATE INDEX idx_principal_movie_person ON MoviePrincipal (movie_id, person_id, job);