import time
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["cineexplorer_db"]

db.movies_complete.create_index("cast.name")
db.movies_complete.create_index("genres")

def measure(query_func, *args):
    start = time.perf_counter()
    result = list(query_func(*args))
    duration = (time.perf_counter() - start) * 1000
    return result, duration

def q1_filmography(name):
    return db.movies_complete.find({"cast.name": name}, {"title": 1, "year": 1, "_id": 0})

def q2_top_genre(genre, y_min, y_max, n):
    return db.movies_complete.find({
        "genres": genre,
        "year": {"$gte": y_min, "$lte": y_max}
    }).sort("rating.average", -1).limit(n)

def q3_multi_roles():
    return db.movies_complete.aggregate([
        {"$unwind": "$cast"},
        {"$group": {"_id": {"m": "$_id", "p": "$cast.person_id"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}},
        {"$limit": 10}
    ])

def q4_collabs(actor_name):
    return db.movies_complete.aggregate([
        {"$match": {"cast.name": actor_name}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$_id", "directors": {"$push": {"$cond": [{"$eq": ["$cast.category", "director"]}, "$cast.name", None]}}}},
        {"$unwind": "$directors"},
        {"$match": {"directors": {"$ne": None}}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ])

def q5_popular_genres():
    return db.movies_complete.aggregate([
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "avg": {"$avg": "$rating.average"}, "count": {"$sum": 1}}},
        {"$match": {"avg": {"$gt": 7.0}, "count": {"$gt": 5}}},
        {"$sort": {"avg": -1}}
    ])

def q6_career_evolution(name):
    return db.movies_complete.aggregate([
        {"$match": {"cast.name": name}},
        {"$project": {"decade": {"$subtract": ["$year", {"$mod": ["$year", 10]}]}}},
        {"$group": {"_id": "$decade", "total": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])

def q7_top3_per_genre():
    return db.movies_complete.aggregate([
        {"$unwind": "$genres"},
        {"$sort": {"rating.average": -1}},
        {"$group": {"_id": "$genres", "top3": {"$push": "$title"}}},
        {"$project": {"top3": {"$slice": ["$top3", 3]}}}
    ])

def q8_blockbuster_actors():
    return db.movies_complete.aggregate([
        {"$match": {"rating.votes": {"$gt": 2000}}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast.name"}}
    ])

def q9_longest_films():
    return db.movies_complete.find().sort("runtime", -1).limit(5)

if __name__ == "__main__":
    res, t = measure(q1_filmography, "Salvatore Papa")
    print(f"Q1: {t:.2f} ms | {len(res)} docs")
    
    res, t = measure(q2_top_genre, "Drama", 1900, 2024, 5)
    print(f"Q2: {t:.2f} ms")
    
    res, t = measure(q3_multi_roles)
    print(f"Q3: {t:.2f} ms")
    
    res, t = measure(q4_collabs, "Salvatore Papa")
    print(f"Q4: {t:.2f} ms")
    
    res, t = measure(q5_popular_genres)
    print(f"Q5: {t:.2f} ms")
    
    res, t = measure(q6_career_evolution, "Salvatore Papa")
    print(f"Q6: {t:.2f} ms")
    
    res, t = measure(q7_top3_per_genre)
    print(f"Q7: {t:.2f} ms")
    
    res, t = measure(q8_blockbuster_actors)
    print(f"Q8: {t:.2f} ms")
    
    res, t = measure(q9_longest_films)
    print(f"Q9: {t:.2f} ms")