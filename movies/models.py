from django.db import models

class Movie(models.Model):
    movie_id = models.CharField(max_length=20, primary_key=True)
    titleType = models.CharField(max_length=50, null=True, blank=True)
    primaryTitle = models.CharField(max_length=500)
    originalTitle = models.CharField(max_length=500)
    isAdult = models.BooleanField(default=False)
    startYear = models.IntegerField(null=True, blank=True)
    endYear = models.IntegerField(null=True, blank=True)
    runtimeMinutes = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'Movie'
        managed = False

    def __str__(self):
        return self.primaryTitle


class Person(models.Model):
    person_id = models.CharField(max_length=20, primary_key=True)
    primaryName = models.CharField(max_length=255)
    birthYear = models.IntegerField(null=True, blank=True)
    deathYear = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'Person'
        managed = False

    def __str__(self):
        return self.primaryName


class Rating(models.Model):
    movie = models.OneToOneField(
        Movie, 
        on_delete=models.CASCADE, 
        primary_key=True, 
        db_column='movie_id', 
        related_name='rating' 
    )
    averageRating = models.FloatField()
    numVotes = models.IntegerField()

    class Meta:
        db_table = 'Rating'
        managed = False