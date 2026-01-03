from pymongo import MongoClient

def simple_check():
   
    uri = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        
        primary = client.admin.command("isMaster")['primary']
        print(f" Le cluster a retrouvé un chef ! Le PRIMARY est : {primary}")
        
        status = client.admin.command("replSetGetStatus")
        print("\n États actuels ")
        for m in status['members']:
            print(f"Port {m['name']} -> {m['stateStr']}")
    except Exception as e:
        print(" Toujours pas de Primary élu")

if __name__ == "__main__":
    simple_check()