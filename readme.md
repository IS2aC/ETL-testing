## ETL - TESTING -PROCESS

### ENGLISH VERSION
-----------------------------------------------------------------------------------------------------------------------------------------
### FRENCH VERSION
Le testing de solution est une etape tres importante dans la conception de solution technologique; l'objectif etant de concevoir des solutions toujours plus robustes. De plus dans le contexte de mise en place des pipelines de données, des exigences de performance sont a prendre en compte. C'est donc suivant ces exigences qu'une panoplie de test sera realiser.


#### Contexte historique
Dans le contexte de ce projet, les données au format parquet sont prealablement stockées dans un bucket minio.

- Etape (1) : Extraction des données brutes depuis le bucket minio <br>

- Etape (2) : Transformation des données extraites suivant un data modeling bien elaborées.<br>

- Etape (3) : Loading des données dans base de données realitionnels Postgresql<br>

- Etape (4) : Realisation des test unitaires des fonctions des differentes fonctions de l'ETL<br>
La liste des tests realisés seront  :  
    - (1) Etape Extraction:
        -> check des colonnes dans les dataframes
        -> check des dimensions
        -> check sur les types de données
        -> check comparaison sur les deux dataframes extrait et existants
        <br>
    - (2) Etape transformation:
        ->
        <br>
    - (3) Etape loading:
        ->
        <br>