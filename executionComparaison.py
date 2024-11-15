import re
import sys
import os
import time
from operator import add
from typing import Iterable, Tuple
from google.cloud import storage
import matplotlib.pyplot as plt

def lire_fichier(bucket_name, file_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    contenu = blob.download_as_text()
    return contenu

# Paramètres de configuration
bucket_name = "bucket_page_ranker"
max_workers = 4  # Ajustez selon votre besoin

# Initialiser le client Google Cloud Storage
client = storage.Client()
bucket = client.get_bucket(bucket_name)

# Fonction pour collecter les données et générer les moyennes
def collect_data_from_folder(prefix):
    # Dictionnaire pour stocker les données pour chaque cas de partitionnement
    timers_data = {
        "url-partionner": {f"timer-{i}workers": [] for i in range(1, max_workers + 1)},
        "no-url-partionner": {f"timer-{i}workers": [] for i in range(1, max_workers + 1)}
    }

    # Parcourir chaque exécution dans le dossier
    executions = [blob.name.split('/')[1] for blob in bucket.list_blobs(prefix=prefix) if "execution-" in blob.name]
    executions = sorted(set(executions))  # Supprimer les doublons et trier

    for execution in executions:
        # Boucle pour chaque type de partitionnement
        for partitioner_file in ["url-partionner", "no-url-partionner"]:
            # Boucler sur chaque timer-{i}workers
            for i in range(1, max_workers + 1):
                prefix_dir = f"{prefix}/{execution}/{partitioner_file}/timer/timer-{i}workers/"
                
                # Lister tous les fichiers sous le préfixe (répertoire)
                blobs = list(bucket.list_blobs(prefix=prefix_dir))
                
                if blobs:
                    # Filtrer les fichiers "part-xxxxx" sans se soucier du contenu exact après "part-"
                    part_files = [blob for blob in blobs if "part-" in blob.name]
                    
                    if part_files:
                        # Trier les fichiers par ordre lexicographique pour obtenir le dernier fichier
                        part_files.sort(key=lambda blob: blob.name, reverse=True)
                        last_blob = part_files[0]
                        file_name = last_blob.name

                        try:
                            # Lire et convertir le contenu en float
                            contenu_du_fichier = lire_fichier(bucket_name, file_name)
                            try:
                                valeur = float(contenu_du_fichier)
                                timers_data[partitioner_file][f"timer-{i}workers"].append(valeur)
                                print(f"Valeur ajoutée pour {prefix_dir}: {valeur}")
                            except ValueError:
                                print(f"Erreur de conversion : Le contenu du fichier {file_name} n'est pas un nombre valide.")
                        
                        except Exception as e:
                            print(f"Erreur lors de la lecture du fichier {file_name}: {e}")
                    else:
                        print(f"Aucun fichier 'part-xxxx' trouvé dans {prefix_dir}")
                else:
                    print(f"Aucun fichier trouvé dans {prefix_dir}")

    return timers_data

# Collecter les données pour "pagerankRDD" et "pagerankDF"
timers_data_rdd = collect_data_from_folder("pagerankRDD")
timers_data_df = collect_data_from_folder("pagerankDF")

# Calculer les moyennes pour chaque timer-{i}workers pour chaque jeu de données
def calculate_averages(timers_data):
    moyennes_par_worker = {}
    for partitioner, timers in timers_data.items():
        moyennes_par_worker[partitioner] = {}
        for key, values in timers.items():
            if values:
                moyenne = sum(values) / len(values)
                moyennes_par_worker[partitioner][key] = moyenne
                print(f"Moyenne des valeurs pour {partitioner} - {key}: {moyenne}")
            else:
                print(f"Aucune donnée pour {partitioner} - {key}")
    return moyennes_par_worker

# Moyennes des valeurs pour chaque dossier
moyennes_rdd = calculate_averages(timers_data_rdd)
moyennes_df = calculate_averages(timers_data_df)

# Préparer les données pour les graphiques
def extract_plot_data(moyennes):
    workers = {partitioner: [] for partitioner in moyennes.keys()}
    moyennes_vals = {partitioner: [] for partitioner in moyennes.keys()}
    for partitioner, values in moyennes.items():
        workers[partitioner] = [int(re.search(r'(\d+)', key).group()) for key in values.keys()]
        moyennes_vals[partitioner] = [values[key] for key in values.keys()]
    return workers, moyennes_vals

workers_rdd, moyennes_rdd_vals = extract_plot_data(moyennes_rdd)
workers_df, moyennes_df_vals = extract_plot_data(moyennes_df)

# Tracer et enregistrer les graphiques dans un seul fichier avec 2 sous-graphes
fig, axs = plt.subplots(2, 1, figsize=(10, 12))

# Premier graphique : pour pagerankRDD avec et sans partitionnement
if workers_rdd["url-partionner"] or workers_rdd["no-url-partionner"]:
    if workers_rdd["url-partionner"]:
        axs[0].plot(workers_rdd["url-partionner"], moyennes_rdd_vals["url-partionner"], 
                    marker='o', linestyle='-', color='b', label="Avec partitionnement")
    if workers_rdd["no-url-partionner"]:
        axs[0].plot(workers_rdd["no-url-partionner"], moyennes_rdd_vals["no-url-partionner"], 
                    marker='x', linestyle='--', color='g', label="Sans partitionnement")
    
    axs[0].set_xlabel('Nombre de Workers')
    axs[0].set_ylabel('Temps moyen (s)')
    axs[0].set_title('Temps moyen d\'exécution pour pagerankRDD')
    axs[0].legend()
    axs[0].grid(True)
    
    # Ajuster les limites des axes
    max_workers_rdd = max(workers_rdd["url-partionner"] + workers_rdd["no-url-partionner"], default=1)
    axs[0].set_xlim(1, max_workers_rdd)  # Définir le début de l'axe X à 1
    max_y_rdd = max(moyennes_rdd_vals["url-partionner"] + moyennes_rdd_vals["no-url-partionner"], default=0)
    axs[0].set_ylim(0, max_y_rdd * 1.5)  # Limite Y : 50% supérieur à la valeur maximale
else:
    axs[0].text(0.5, 0.5, "Données manquantes pour pagerankRDD", ha='center', va='center', fontsize=12, color='red')
    axs[0].set_title('Temps moyen d\'exécution pour pagerankRDD - Données manquantes')
    axs[0].axis('off')

# Deuxième graphique : pour pagerankDF avec et sans partitionnement
if workers_df["url-partionner"] or workers_df["no-url-partionner"]:
    if workers_df["url-partionner"]:
        axs[1].plot(workers_df["url-partionner"], moyennes_df_vals["url-partionner"], 
                    marker='o', linestyle='-', color='r', label="Avec partitionnement")
    if workers_df["no-url-partionner"]:
        axs[1].plot(workers_df["no-url-partionner"], moyennes_df_vals["no-url-partionner"], 
                    marker='x', linestyle='--', color='orange', label="Sans partitionnement")
    
    axs[1].set_xlabel('Nombre de Workers')
    axs[1].set_ylabel('Temps moyen (s)')
    axs[1].set_title('Temps moyen d\'exécution pour pagerankDF')
    axs[1].legend()
    axs[1].grid(True)
    
    # Ajuster les limites des axes
    max_workers_df = max(workers_df["url-partionner"] + workers_df["no-url-partionner"], default=1)
    axs[1].set_xlim(1, max_workers_df)  # Définir le début de l'axe X à 1
    max_y_df = max(moyennes_df_vals["url-partionner"] + moyennes_df_vals["no-url-partionner"], default=0)
    axs[1].set_ylim(0, max_y_df * 1.5)  # Limite Y : 50% supérieur à la valeur maximale
else:
    axs[1].text(0.5, 0.5, "Données manquantes pour pagerankDF", ha='center', va='center', fontsize=12, color='red')
    axs[1].set_title('Temps moyen d\'exécution pour pagerankDF - Données manquantes')
    axs[1].axis('off')

# Sauvegarder le graphique sous un fichier unique
plt.tight_layout()
plt.savefig('execution_times_comparison_with_partitioning.png')
print("Graphique comparatif enregistré sous le nom 'execution_times_comparison_with_partitioning.png'")
