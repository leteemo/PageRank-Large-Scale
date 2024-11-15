#!/bin/bash

# Télécharger les données de PageRank
curl -o small_page_links.nt https://raw.githubusercontent.com/momo54/large_scale_data_management/main/small_page_links.nt

# Variables à personnaliser
PROJECT_ID="largescale-436008"
BUCKET_NAME="bucket_page_ranker"
CLUSTER_NAME="pagerank-clusterrdd"
REGION="us-central1"
INPUT_FILE_PATH="small_page_links.nt"
ZONE="us-central1-a"
PAGERANK_RDD_SCRIPT="pagerankRDD.py"
PAGERANK_DF_SCRIPT="pagerankDF.py"
ITERATIONS=10  # Nombre d'itérations pour le PageRank

# Activer les APIs nécessaires
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com

# Configurer le projet par défaut
gcloud config set project $PROJECT_ID

# Créer un bucket GCS (si nécessaire)
gsutil mb -l $REGION gs://$BUCKET_NAME/

# Copier le fichier d'entrée dans le bucket
gsutil cp $INPUT_FILE_PATH gs://$BUCKET_NAME/

# Boucle pour exécuter le calcul du PageRank plusieurs fois
for NUM_EXECUTION in {1..1}; do  # Ajustez la plage selon le nombre d'exécutions souhaitées
    OUTPUT_EXECUTION_PATH="execution-${NUM_EXECUTION}"
    
    # Boucle avec ou sans partionner
    for PARTITIONER in "no-url-partionner" "url-partionner"; do

        # Boucle pour différents nombres de workers
        for NUM_WORKERS in 1 2 4; do  # Ajustez la plage selon le nombre de workers souhaités
            OUTPUT_PATH="output-${NUM_WORKERS}workers"
            TIMER_PATH="timer-${NUM_WORKERS}workers"
            
            # Création du cluster avec le nombre de workers spécifié
            if [ "$NUM_WORKERS" -eq 1 ]; then
                gcloud dataproc clusters create $CLUSTER_NAME-$NUM_WORKERS-$NUM_EXECUTION-$PARTITIONER \
                    --region=$REGION \
                    --zone=$ZONE \
                    --single-node \
                    --master-machine-type=n1-standard-2 \
                    --master-boot-disk-size=50GB \
                    --image-version=2.0-debian10
            else
                gcloud dataproc clusters create $CLUSTER_NAME-$NUM_WORKERS-$NUM_EXECUTION-$PARTITIONER \
                    --region=$REGION \
                    --zone=$ZONE \
                    --num-workers=$NUM_WORKERS \
                    --master-machine-type=n1-standard-2 \
                    --worker-machine-type=n1-standard-2 \
                    --master-boot-disk-size=50GB \
                    --worker-boot-disk-size=50GB \
                    --image-version=2.0-debian10
            fi

            # Exécution des deux scripts PySpark : pagerankRDD.py et pagerankDF.py
            for SCRIPT in $PAGERANK_RDD_SCRIPT; do
                # Soumettre le job PySpark au cluster
                gcloud dataproc jobs submit pyspark $SCRIPT \
                    --cluster=$CLUSTER_NAME-$NUM_WORKERS-$NUM_EXECUTION-$PARTITIONER \
                    --region=$REGION \
                    -- $INPUT_FILE_PATH $ITERATIONS $BUCKET_NAME $OUTPUT_PATH $TIMER_PATH $OUTPUT_EXECUTION_PATH $NUM_WORKERS $PARTITIONER

                # Attendre quelques secondes avant la suppression du cluster pour le prochain job
                sleep 10
            done

            # Supprimer le cluster après les exécutions
            gcloud dataproc clusters delete $CLUSTER_NAME-$NUM_WORKERS-$NUM_EXECUTION-$PARTITIONER --region=$REGION --quiet

        done

    done
done