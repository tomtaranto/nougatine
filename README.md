# nougatine

Ce projet cherche à répondre à la problématique du cours d'optimisation.
Nous avons choisi de regarder la répartition du traffic au cours des heures par arrondissement dans Paris.

le fichier dag_dl_daily.py permet de lancer airflow et de telecharger les données journalieres.
Le fichier requete.py permet d'automatiser l'integration du fichier dans une base plus complexe via spark.
test.py est un fichier exemple permettant de lancer un spark-submit
True.ipnb est un vieux fichier permettant de tester la transformation de données.
La visualisation des résultats se fait via le fichier visualisation.ipnb
