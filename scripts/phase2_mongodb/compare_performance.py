import time
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["cineexplorer_db"]
target_mid = "tt0111161" 

def test_flat_performance():
    start = time.perf_counter()
    
    movie = db.Movie.find_one({"mid": target_mid})
    rating = db.Rating.find_one({"mid": target_mid})
    genres = list(db.MovieGenre.find({"mid": target_mid}))
    cast = list(db.MoviePrincipal.find({"mid": target_mid}))
    duration = (time.perf_counter() - start) * 1000
    return duration

def test_structured_performance():
    start = time.perf_counter()
    
    movie = db.movies_complete.find_one({"_id": target_mid})
    duration = (time.perf_counter() - start) * 1000
    return duration

print(f"Temps modèle PLAT (N requêtes) : {test_flat_performance():.4f} ms")
print(f"Temps modèle STRUCTURÉ (1 requête) : {test_structured_performance():.4f} ms")