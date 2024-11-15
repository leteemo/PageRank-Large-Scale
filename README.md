# Page Rank avec Google Cloud Computing

Authors: Khoussein MAALOV et Hanahe MELEHI

# Installation

git clone https://github.com/leteemo/PageRank-Large-Scale \
chmod +x pagerank.sh \
./pagerank.sh

Tracé du graphique: \
python executionComparaison.py <bucket name>

##

Comparatif des résultats: \
![Graphique](https://github.com/leteemo/PageRank-Large-Scale/blob/main/graphique.png "Graphique") \
Le temps d'exécution diminue en fonction du nombre de workers avec deux, cependant il ne semble pas être efficace avec 4 workers, notamment quand il n'y a pas de partitionnement, peut être dû à un echange d'informations entre workers.
De plus Dataframe ne semble pas plus efficace.

## Script

Le script bash exécute les étapes suivantes :

- Téléchargement du fichier d'entrée : Le fichier contenant les liens entre les pages (small_page_links.nt) est téléchargé depuis un dépôt GitHub.
- Création du bucket Google Cloud Storage (GCS) : Un bucket GCS est créé pour stocker les données, si celui-ci n'existe pas encore.
- Configuration du projet Google Cloud : Le script configure le projet Google Cloud et active les API nécessaires (Dataproc et Storage).
- Réservation et configuration du cluster Dataproc : Un cluster Dataproc est créé avec un nombre de nœuds défini, permettant de distribuer les calculs. Plusieurs boucles permettent de spécifier le 
- Exécution du job PySpark : Le script soumet un job PySpark au cluster pour exécuter l'algorithme de PageRank. Ce job calcule les rangs des pages en utilisant une approche RDD ou DataFrame.
- Téléchargement des résultats : Les résultats finaux (les rangs calculés des pages) sont stockés dans le bucket GCS et peuvent être listés ou téléchargés.
- Suppression du cluster : Une fois le traitement terminé, le cluster est supprimé pour libérer les ressources et éviter les coûts supplémentaires.

## RDD

Dans la version RDD, le script pagerankRDD.py utilise les Resilient Distributed Datasets (RDD) pour implémenter l'algorithme de PageRank. Voici les étapes principales :

- Chargement des données : Les données sont chargées en tant que RDD. Chaque ligne du fichier d'entrée est lue, et les paires (URL, voisin) sont extraites pour représenter les liens entre les pages.
Initialisation des rangs : Chaque URL est initialisée avec un rang de 1.0.

- Calcul des contributions de PageRank :
        Pour chaque itération, chaque URL distribue son rang actuel à ses voisins, selon une fonction appelée computeContribs. Cette fonction prend en compte le nombre de voisins de chaque URL pour calculer la part de rang qui revient à chaque voisin.
- Mise à jour des rangs :
        Les contributions reçues par chaque URL sont agrégées, puis chaque URL met à jour son rang en utilisant le facteur de damping (0.85) et le facteur de téléportation (0.15).
    Sauvegarde des résultats : Après la dernière itération, les résultats sont sauvegardés dans un fichier texte sur GCS, qui contient les rangs calculés pour chaque URL.

L'approche RDD est adaptée aux opérations de transformation et d'agrégation, comme celles utilisées dans l'algorithme PageRank. Cependant, elle peut manquer d'optimisations automatiques disponibles avec les DataFrames.

## DataFrame

Dans la version DataFrame, le script pagerankDF.py utilise les DataFrames de Spark, offrant une approche SQL-like pour implémenter l'algorithme de PageRank. Voici les étapes principales :

Chargement des données en tant que DataFrame : Les données sont chargées en tant que DataFrame, et les paires (URL, voisin) sont extraites via une fonction de parsing pour structurer les données sous forme de colonnes url et neighbor.
Initialisation des rangs : Un DataFrame ranks_df est créé pour chaque URL unique avec un rang initial de 1.0.
- Calcul des contributions de PageRank :
        À chaque itération, le DataFrame des voisins est joint avec le DataFrame des rangs pour associer le rang actuel de chaque URL à ses voisins.
        Le rang de chaque URL est divisé par le nombre de voisins et redistribué sous forme de contributions à ses voisins.
- Mise à jour des rangs :
        Les contributions reçues par chaque URL sont agrégées par URL à l’aide de l’opération groupBy et de la fonction d’agrégation sum. Le facteur de damping (0.85) et le facteur de téléportation (0.15) sont appliqués pour recalculer le rang de chaque URL.
    Sauvegarde des résultats : Les résultats finaux sont sauvegardés au format CSV dans le bucket GCS.

