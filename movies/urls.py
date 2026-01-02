from django.urls import path
from . import views

urlpatterns = [
    
    path('', views.home, name='home'), 
    path('movies/', views.movie_list, name='movie_list'),
    path('movie/<str:tconst>/', views.movie_detail, name='movie_detail'),
    path('search/', views.search_view, name='search'),
    path('stats/', views.stats_view, name='stats_view'),
    path('test-db/', views.test_stats_view, name='test_db'),
]