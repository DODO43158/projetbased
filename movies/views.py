import requests
from django.shortcuts import render
from django.core.paginator import Paginator
from .models import Movie  
from .mongo_service import mongo_service

def home(request):
    """Récupère le Top 10 et force l'affichage des affiches"""
    print("--- APPEL DE LA VUE HOME ---") 
    
    
    top_movies = Movie.objects.select_related('rating').order_by('-rating__averageRating')[:10]
    
    api_key = "7fca3f7f" 
    
    for movie in top_movies:
        movie.poster_url = None 
        try:
            
            movie_identifier = str(movie.movie_id).strip()
            
           
            url = f"http://www.omdbapi.com/?i={movie_identifier}&apikey={api_key}"
            
            response = requests.get(url, timeout=5).json()
            
            if response.get('Response') == "True":
                poster = response.get('Poster')
                if poster and poster != "N/A":
                    movie.poster_url = poster
                    print(f"SUCCÈS : Image trouvée pour {movie_identifier}")
                else:
                    print(f"INFO : Pas de poster dispo pour {movie_identifier}")
            else:
                print(f"ERREUR API : {response.get('Error')} pour {movie_identifier}")
                
        except Exception as e:
            print(f"ERREUR CONNEXION : {e}")

    return render(request, 'movies/home.html', {'top_movies': top_movies})


def movie_list(request):
    movie_queryset = Movie.objects.select_related('rating').order_by('-rating__numVotes')
    paginator = Paginator(movie_queryset, 20)
    page_obj = paginator.get_page(request.GET.get('page'))
    return render(request, 'movies/list.html', {'movies': page_obj})


def movie_detail(request, tconst):
    movie = mongo_service.get_movie_by_id(tconst)
    return render(request, 'movies/detail.html', {'movie': movie})


def search_view(request):
    query = request.GET.get('q', '')
    
    movies = Movie.objects.filter(primaryTitle__icontains=query)[:20] if query else []
    return render(request, 'movies/search.html', {'movies': movies, 'query': query})


def stats_view(request):
    genre_data = mongo_service.get_genre_stats()
    return render(request, 'movies/stats.html', {'genre_data': genre_data})

def test_stats_view(request):
    """Vue de test pour vérifier la connexion aux bases"""
    nb_mongo = mongo_service.get_movies_count()
    nb_sqlite = Movie.objects.count()
    context = {
        'count_mongo': nb_mongo,
        'count_sqlite': nb_sqlite,
        'infra_status': "Opérationnel"
    }
    return render(request, 'movies/test_stats.html', context)