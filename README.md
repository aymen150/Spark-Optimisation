# Coding Dojo Spark Avancé

## Jeu de données

### Base Insee Recensement de la population

Voir : https://www.insee.fr/fr/statistiques/3141877?sommaire=2866354

https://www.insee.fr/fr/statistiques/fichier/3141877/RP2014_logemt_txt.zip 

### Base IRIS sur les revenus déclarés

Voir : https://www.insee.fr/fr/statistiques/3288151

https://www.insee.fr/fr/statistiques/fichier/3288151/BASE_TD_FILO_DEC_IRIS_2014.xls


## Configuration

### Dojo

Editer la classe ```fr.ippon.formation.spark.codingdojo.Constants``` et mettre  
le chemin vers les datasets dans la PATH. 

### History Server

A partir d'une installation fonctionnelle de Spark, 

1. éditer le fichier ``<SPARK_HOME>/conf/spark-default.conf`` et ajouter : 

```
spark.history.fs.logDirectory    /<path_renseigné_dans_Constants>/spark-events
```

2. lancer l'history server ``<SPARK_HOME>/sbin/start-history-server.sh``
3. se connecter à l'adresse http://localhost:18080


