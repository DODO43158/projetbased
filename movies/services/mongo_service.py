from pymongo import MongoClient
from django.conf import settings
import re

class MongoService:
    def __init__(self):
        self.client = MongoClient(settings.MONGO_URI)
        self.db = self.client[settings.MONGO_DB_NAME]
        self.collection = self.db['movies_complete'] 

    def get_movie_by_id(self, tconst):
        """
        Récupère le document complet. 
        Tente la recherche sur '_id' ET sur 'movie_id' pour parer à toute éventualité.
        """
        if not tconst:
            return None
            
        
        clean_id = str(tconst).strip()
        
        
        movie = self.collection.find_one({"_id": clean_id})
        
        
        if not movie:
            movie = self.collection.find_one({"movie_id": clean_id})
            
        
        if not movie:
            regex_id = re.compile(f"^{clean_id}$", re.I)
            movie = self.collection.find_one({
                "$or": [
                    {"_id": regex_id},
                    {"movie_id": regex_id}
                ]
            })
            
        return movie

    def get_movies_count(self):
        """Retourne le nombre total de documents (36859 confirmé par vos logs)"""
        return self.collection.count_documents({})

    def get_genre_stats(self):
        pipeline = [
            {"$unwind": "$genres"},
            {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        return list(self.collection.aggregate(pipeline))

mongo_service = MongoService()