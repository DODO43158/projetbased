from pymongo import MongoClient
import time

db = MongoClient("mongodb://localhost:27017/")["cineexplorer_db"]

print("--- Suivi de la progression ---")
while True:
    count = db.movies_complete.count_documents({})
    print(f"Films dans movies_complete : {count}", end="\r")
    time.sleep(2)