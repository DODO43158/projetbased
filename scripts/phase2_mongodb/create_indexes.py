from pymongo import MongoClient

def create_indexes():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["cineexplorer_db"]
    
    print(" Création des index pour accélérer la dénormalisation...")
    
    
    db.Movie.create_index("mid")
    db.Rating.create_index("mid")
    db.MovieGenre.create_index("mid")
    db.MoviePrincipal.create_index("mid")
    db.Person.create_index("pid")
    db.Character.create_index("mid")
    db.Character.create_index("pid")
    
    print(" Index créés avec succès !")

if __name__ == "__main__":
    create_indexes()