# EXAMEN HADOOP ISAAQ SOUIDI ET OUSSAMA NASRY

**Dépôt GitHub** : [https://github.com/isaaqdemoi/CC2_Hadoop_SOUIDI_NASRY](https://github.com/isaaqdemoi/CC2_Hadoop_SOUIDI_NASRY)  






## 0. Préparation de l’environnement


Nous avons travaillé sur la sandbox HDP avec "mrjob" pour exécuter les jobs MapReduce en Python.  

Le fichier "tags.csv" contient environ 1 093 361 lignes pour une taille d’environ 38,8 Mo.


### Commandes utilisées


```bash
sudo su root

head -100 ml-25m/tags.csv > tags_sample.csv

hdfs dfs -put ml-25m/tags.csv /user/root/tags_default.csv

hdfs dfs -D dfs.blocksize=67108864 -put ml-25m/tags.csv /user/root/tags_64m.csv
```


### Commentaires : 


Le point important de ce sujet est que le fichier est un vrai CSV avec des virgules dans certains tags.  

Nous avons aussi ignoré le header "userId,movieId,tag,timestamp" dans chaque mapper 



## Question 1 — Nombre de tags par film


### Ce qu'on a fait : 


Le mapper émet "(movieId, 1)" pour chaque ligne valide.  
Le reducer additionne toutes les valeurs pour obtenir le nombre de tags associés à chaque film.


### Scripte (local)


```bash
python q1_tags_per_movie.py tags_sample.csv
```


### Scripte Hadoop


```bash
python q1_tags_per_movie.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/root/tags_default.csv -o hdfs:///user/root/output_q1
```


### Résultat de notre script local 


```text
"215"   14

"590"   9

"109487"        9

"1127"  7
```


### Résultat de notre script hadoop

Le reducer a produit 45 251 films.


Résultat complet sur :  

[results_q1.txt](https://github.com/isaaqdemoi/CC2_Hadoop_SOUIDI_NASRY/blob/main/results_q1.txt)




## Question 2 — Nombre de tags par utilisateur


### Ce qu'on a fait


Le mapper émet "(userId, 1)" pour chaque ligne valide.  

Le reducer calcule la somme pour obtenir le nombre de tags ajoutés par chaque utilisateur.


### scripte (local)


```bash
python q2_tags_per_user.py tags_sample.csv
```


### scripte  Hadoop


```bash
python q2_tags_per_user.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/root/tags_default.csv -o hdfs:///user/root/output_q2
```


### Résultat local (extrait)


```text

"91"    39

"87"    31

"4"     13

"19"    8

"84"    3

"3"     2

"20"    1

```


### Résultat Hadoop



Le reducer a produit 14 592 utilisateurs.


Résultat complet sur :  

[results_q2.txt](https://github.com/isaaqdemoi/CC2_Hadoop_SOUIDI_NASRY/blob/main/results_q2.txt)





## Question 3 — Nombre de blocs HDFS



Nous avons vérifié le nombre de blocs HDFS pour deux configurations :

- configuration par défaut ;

- configuration avec taille de bloc fixée à 64 Mo.


### Commandes utilisées


```bash

hdfs fsck /user/root/tags_default.csv -files -blocks

hdfs dfs -D dfs.blocksize=67108864 -put ml-25m/tags.csv /user/root/tags_64m.csv

hdfs fsck /user/root/tags_64m.csv -files -blocks

```


### Résultats obtenus


```text

/user/root/tags_default.csv 38810332 bytes, 1 block(s): OK

/user/root/tags_64m.csv 38810332 bytes, 1 block(s): OK

```


### Commentaire


Le fichier "tags.csv" mesure environ 38,8 Mo.  

Il est donc plus petit que 128 Mo et plus petit que 64 Mo, ce qui explique qu’il occupe **1 seul bloc** dans les deux cas.





## Question 4 — Fréquence d’utilisation de chaque tag



Le mapper émet "(tag, 1)" pour chaque ligne valide.  

Le reducer additionne les occurrences pour calculer combien de fois chaque tag a été utilisé.


### Scripte locale


```bash

python q4_tag_frequency.py tags_sample.csv

```


### Scripte Hadoop


```bash

python q4_tag_frequency.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/root/tags_64m.csv -o hdfs:///user/root/output_q4

```


### Résultat Scripte local 


```text

"artificial intelligence"       2

"bittersweet"   2

"James Cameron" 2

"love story"    2

"philosophical" 2

"sci-fi"        5

"science fiction"       2

```


### Résultat scripte Hadoop


Le job s’est terminé avec succès.  

Le reducer a produit 73 004 tags distincts.


Résultat complet :  

[results_q4.txt](https://github.com/isaaqdemoi/CC2_Hadoop_SOUIDI_NASRY/blob/main/results_q4.txt)





## Question 5 — Nombre de tags par couple (film, utilisateur)


### On sait que


Le mapper émet "((movieId, userId), 1)" pour chaque ligne valide.  

Le reducer additionne les valeurs pour obtenir le nombre de tags qu’un utilisateur donné a mis sur un film donné.


### Scripte locale


```bash

python q5_tags_user_movie.py tags_sample.csv

```


### Scripte Hadoop


```bash

python q5_tags_user_movie.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/root/tags_64m.csv -o hdfs:///user/root/output_q5

```


### Résultat scripte local 


```text

["215", "91"]   14

["1719", "91"]  10

["590", "91"]   9

["109487", "87"]        9

["1127", "87"]  7

["6537", "87"]  7

["7099", "19"]  7

["1619", "91"]  6

```


### Résultat Scritpte Hadoop


Le job s’est terminé avec succès.  

Le reducer a produit 305 356 couples "(film, utilisateur)".


Résultat complet :  

[results_q5.txt](https://github.com/isaaqdemoi/CC2_Hadoop_SOUIDI_NASRY/blob/main/results_q5.txt)





## Question 6 ) Récupération des résultats


Après l’exécution des jobs Hadoop, nous avons récupéré les sorties HDFS avec les commandes suivantes :


```bash

hdfs dfs -getmerge /user/root/output_q1 results_q1.txt

hdfs dfs -getmerge /user/root/output_q2 results_q2.txt

hdfs dfs -getmerge /user/root/output_q4 results_q4.txt

hdfs dfs -getmerge /user/root/output_q5 results_q5.txt

```








## Derniere etape :  Fichiers déposés sur GitHub


- `q1_tags_per_movie.py`

- `q2_tags_per_user.py`

- `q4_tag_frequency.py`

- `q5_tags_user_movie.py`

- `results_q1.txt`

- `results_q2.txt`

- `results_q4.txt`

- `results_q5.txt`

- `SOUIDI_NASRY.md`
