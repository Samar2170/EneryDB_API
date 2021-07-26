from django.db import models

class SeriesDetail(models.Model):
    Code=models.CharField(max_length=100,db_index=True)
    Name=models.CharField(max_length=500,db_index=True)
    Descrip=models.TextField()
    Geography=models.CharField(max_length=1000)
    source=models.CharField(max_length=100)

    def __str__(self):
        return self.Name 

class SeriesData(models.Model):
    series=models.ForeignKey(SeriesDetail, on_delete=models.CASCADE,db_index=True)
    period=models.CharField(max_length=50)
    data=models.FloatField()

    def __str__(self):
        return self.series_Name