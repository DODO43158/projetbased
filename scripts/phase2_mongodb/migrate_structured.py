"""
Phase 2 - T2.4 : Migration vers documents structurés

"""

from pymongo import MongoClient
from pymongo.errors import OperationFailure
import time


def connect_mongodb(host='localhost', port=27017, db_name='cineexplorer_db'):
    
    try:
        client = MongoClient(f"mongodb://{host}:{port}/", serverSelectionTimeoutMS=5000)
        client.server_info()
        db = client[db_name]
        print(f" Connexion réussie à MongoDB ({db_name})")
        return client, db
    except Exception as e:
        print(f" Erreur de connexion à MongoDB: {e}")
        return None, None


def create_indexes(db):
    

    
    indexes_to_create = [
        ('Movie', 'movie_id'),
        ('Rating', 'movie_id'),
        ('MovieGenre', 'movie_id'),
        ('MoviePrincipal', 'movie_id'),
        ('Person', 'person_id')
    ]
    
    for collection_name, field in indexes_to_create:
        try:
            collection = db[collection_name]
            existing_indexes = collection.index_information()
            
            
            index_exists = any(field in str(idx) for idx in existing_indexes.values())
            
            if not index_exists:
                collection.create_index(field)
                print(f"    Index créé: {collection_name}.{field}")
            else:
                print(f"     Index existe déjà: {collection_name}.{field}")
        except Exception as e:
            print(f"     Erreur index {collection_name}.{field}: {e}")


def create_structured_collection_optimized(db, limit=1000, batch_size=100):
   
    print(f"\n Création de movies_complete (limite: {limit} films, batch: {batch_size})...")
    start_time = time.time()
    
    if 'movies_complete' in db.list_collection_names():
        print("    Suppression de l'ancienne collection...")
        db.movies_complete.drop()
    
    
    pipeline = [
        {"$limit": limit},
        
        
        {"$lookup": {
            "from": "Rating",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "rating_info"
        }},
        {"$unwind": {
            "path": "$rating_info",
            "preserveNullAndEmptyArrays": True
        }},
        
        
        {"$lookup": {
            "from": "MovieGenre",
            "localField": "movie_id",
            "foreignField": "movie_id",
            "as": "genre_docs"
        }},
        
       
        {"$lookup": {
            "from": "MoviePrincipal",
            "let": {"movie_id": "$movie_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$movie_id", "$$movie_id"]}}},
                {"$sort": {"ordering": 1}},  
                {"$limit": 5}, 
                {"$project": {
                    "person_id": 1,
                    "category": 1,
                    "ordering": 1,
                    "_id": 0
                }}
            ],
            "as": "cast_list"
        }},
        
       
        {"$lookup": {
            "from": "Person",
            "let": {"cast_ids": "$cast_list.person_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$in": ["$person_id", "$$cast_ids"]}
                }},
                {"$project": {
                    "person_id": 1,
                    "primaryName": 1,
                    "_id": 0
                }}
            ],
            "as": "persons_info"
        }},
        
      
        {"$project": {
            "_id": "$movie_id",
            "title": "$primaryTitle",
            "year": {
                "$cond": {
                    "if": {"$eq": ["$startYear", "\\N"]},
                    "then": None,
                    "else": {"$toInt": "$startYear"}
                }
            },
            "runtime": {
                "$cond": {
                    "if": {"$eq": ["$runtimeMinutes", "\\N"]},
                    "then": None,
                    "else": {"$toInt": "$runtimeMinutes"}
                }
            },
            "genres": "$genre_docs.genre_name",
            "rating": {
                "$cond": {
                    "if": {"$gt": ["$rating_info", None]},
                    "then": {
                        "average": "$rating_info.averageRating",
                        "votes": "$rating_info.numVotes"
                    },
                    "else": None
                }
            },
            "cast": {
                "$map": {
                    "input": "$cast_list",
                    "as": "c",
                    "in": {
                        "person_id": "$$c.person_id",
                        "name": {
                            "$arrayElemAt": [
                                {
                                    "$map": {
                                        "input": {
                                            "$filter": {
                                                "input": "$persons_info",
                                                "as": "p",
                                                "cond": {"$eq": ["$$p.person_id", "$$c.person_id"]}
                                            }
                                        },
                                        "as": "matched",
                                        "in": "$$matched.primaryName"
                                    }
                                },
                                0
                            ]
                        },
                        "category": "$$c.category"
                    }
                }
            }
        }},
        
        {"$out": "movies_complete"}
    ]
    
    try:
        print("    Exécution du pipeline d'agrégation...")
        
       
        db.Movie.aggregate(pipeline, allowDiskUse=True)
        
        elapsed = time.time() - start_time
        count = db.movies_complete.count_documents({})
        
        print(f"\n    Collection movies_complete créée!")
        print(f"    {count:,} documents créés")
        print(f"     Temps: {elapsed:.2f}s ({elapsed/count:.3f}s/doc)")
        
        return True
        
    except Exception as e:
        print(f"\n    Erreur: {type(e).__name__}: {e}")
        return False


def show_sample_document(db):
    print("\n Exemple de document structuré:")
    print("="*70)
    
    sample = db.movies_complete.find_one()
    if sample:
        import json
        print(json.dumps(sample, indent=2, default=str, ensure_ascii=False))
    else:
        print(" Aucun document trouvé")
    
    print("="*70)


def verify_collections(db):
    print("\n Vérification des collections...")
    
    required = ['Movie', 'Rating', 'MovieGenre', 'MoviePrincipal', 'Person']
    collections = db.list_collection_names()
    
    for coll in required:
        if coll in collections:
            count = db[coll].count_documents({})
            print(f"    {coll}: {count:,} documents")
        else:
            print(f"    {coll}: MANQUANTE")
            return False
    
    return True


def main():
    print("="*70)
    print("  Phase 2 - T2.4 : Migration OPTIMISÉE")
    print("="*70)
    
    client, db = connect_mongodb(db_name='cineexplorer_db')
    if client is None or db is None:
        return
    
    if not verify_collections(db):
        print("\n Collections manquantes")
        client.close()
        return
    
    
    create_indexes(db)
    
    
    
    
    
    test_limit = 1000000
    
    if create_structured_collection_optimized(db, limit=test_limit):
        show_sample_document(db)
        
        print("\n Migration réussie!")
    
    
    client.close()


if __name__ == "__main__":
    main()