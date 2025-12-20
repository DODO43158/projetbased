from pymongo import MongoClient
import time


client = MongoClient("mongodb://localhost:27017/", directConnection=True)

def setup():
    try:
        config = {
            '_id': "rs0",
            'members': [
                {'_id': 0, 'host': "localhost:27017"},
                {'_id': 1, 'host': "localhost:27018"},
                {'_id': 2, 'host': "localhost:27019"}
            ]
        }
        print(" Envoi de la configuration au noeud fantôme...")
        client.admin.command("replSetInitiate", config)
        
        print(" Attente de l'élection (15s)...")
        time.sleep(15)
        
        status = client.admin.command("replSetGetStatus")
        print(f"\n Replica Set : {status['set']}")
        for m in status['members']:
            print(f" - Noeud {m['name']} : {m['stateStr']}")
            
    except Exception as e:
        print(f" Erreur : {e}")

if __name__ == "__main__":
    setup()