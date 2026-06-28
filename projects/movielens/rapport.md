# Rapport de projet - Pipeline Spark (Jour 4)

Gabarit du livrable noté. Remplir chaque section. Court et dense : extraits de code, extraits de
résultats, captures. Pas de pavé. Les sections reprennent le plan du rapport (section 5 de
`projects/projet-jour-4.md`). La grille reste le barème ; la qualité du code est notée sur le code
lui-même, pas dans ce document.

- **Équipe** : BENBOUABDELLAH Melissa - KANTE Ismaël Kennedy
- **Jeu de données** : MovieLens
- **Date** : 28 juin 2026

---

## 1. Jeu de données et schéma cible

- **Source** : MovieLens ml-latest-small — 100 836 notes, 9 742 films
- **Schéma cible** :

| Colonne | Type | Description |
|---------|------|-------------|
| userId | IntegerType | Identifiant utilisateur |
| movieId | IntegerType | Identifiant film |
| rating | FloatType | Note (0.5 à 5.0) |
| date | DateType | Date de la note (converti depuis timestamp) |
| year | IntegerType | Année (colonne de partitionnement) |

- **Questions métier visées** :
  - Quels films sont les mieux notés avec un minimum de votes ?
  - Quelle est la popularité par genre ?
  - Quel est le classement des films au sein de chaque genre ?

---

## 2. Pipeline (bronze -> silver -> gold)

```
data/raw/ml-latest-small/  (bronze)
  -> data/silver/ratings/  (Parquet, partitionné par year)
  -> data/gold/            (résultats agrégés)
```

- **Nettoyage appliqué** :
  - Conversion timestamp Unix → date lisible + extraction de l'année
  - Suppression des doublons (`dropDuplicates`)
  - Filtre des notes hors bornes (< 0.5 ou > 5.0)
  - Suppression des valeurs manquantes (`na.drop`)
- **Lignes brutes** : 100 836 | **après nettoyage** : 100 836 | **écartées** : 0 (0%)
  - Les données MovieLens small sont déjà très propres
- **Partitionnement** : par `year` — faible cardinalité, permet le partition pruning sur des requêtes filtrées par année

---

## 3. Analyses

### Analyse 1 — Films les mieux notés (agrégation)

- **Question** : Quels films ont la meilleure note moyenne avec au moins 50 votes ?
- **Code clé** :
```python
df.groupBy("movieId")
  .agg(F.avg("rating").alias("note_moyenne"), F.count("rating").alias("nb_votes"))
  .filter(F.col("nb_votes") >= 50)
  .orderBy(F.desc("note_moyenne"))
```
- **Résultat** :
```
+-------+-----------------+--------+
|movieId|     note_moyenne|nb_votes|
+-------+-----------------+--------+
|    318|4.429022082018927|     317|
|    858|        4.2890625|     192|
|   2959|4.272935779816514|     218|
+-------+-----------------+--------+
```
- **Lecture métier** : Shawshank Redemption (id 318) domine avec 4.43/5 sur 317 votes. Le seuil à 50 votes élimine les films anecdotiques avec peu d'avis et garantit la fiabilité de la note.

---

### Analyse 2 — Jointure ratings + movies (jointure)

- **Question** : Quels sont les films les mieux notés avec leurs titres et genres ?
- **Code clé** :
```python
df.join(F.broadcast(movies), on="movieId", how="inner")
  .groupBy("movieId", "title", "genres")
  .agg(F.avg("rating").alias("note_moyenne"), F.count("rating").alias("nb_votes"))
  .filter(F.col("nb_votes") >= 50)
  .orderBy(F.desc("note_moyenne"))
```
- **Résultat** :
```
+-------+--------------------+--------------------+-----------------+--------+
|movieId|               title|              genres|     note_moyenne|nb_votes|
+-------+--------------------+--------------------+-----------------+--------+
|    318|Shawshank Redempt...|         Crime|Drama|4.429022082018927|     317|
|    858|Godfather, The (1...|         Crime|Drama|        4.2890625|     192|
|   2959|   Fight Club (1999)|Action|Crime|Drama|4.272935779816514|     218|
+-------+--------------------+--------------------+-----------------+--------+
```
- **Lecture métier** : Le genre Crime|Drama domine le classement. Le broadcast sur `movies` (9 742 lignes) évite un shuffle réseau coûteux avec `ratings` (100 836 lignes).

---

### Analyse 3 — Classement par genre (window function)

- **Question** : Quel est le top 5 des films par genre ?
- **Code clé** :
```python
df_genres = analyse_2.withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
fenetre = Window.partitionBy("genre").orderBy(F.desc("note_moyenne"))
df_genres.withColumn("rang", F.row_number().over(fenetre)).filter(F.col("rang") <= 5)
```
- **Résultat** :
```
+---------+----+--------------------+------------------+--------+
|    genre|rang|               title|      note_moyenne|nb_votes|
+---------+----+--------------------+------------------+--------+
|   Action|   1|   Fight Club (1999)| 4.272935779816514|     218|
|   Action|   2|Dark Knight, The ...| 4.238255033557047|     149|
|Animation|   1|Spirited Away (Se...| 4.155172413793103|      87|
|Animation|   2|  Toy Story 3 (2010)| 4.109090909090909|      55|
+---------+----+--------------------+------------------+--------+
```
- **Lecture métier** : L'`explode` des genres permet de classer chaque film dans plusieurs catégories. Fight Club domine l'Action, Spirited Away l'Animation. La window function évite une auto-jointure coûteuse.

---

## 4. Optimisation

- **Optimisation choisie** : Broadcast join
- **Pourquoi** : `movies` est une petite table (9 742 lignes) jointe à `ratings` (100 836 lignes). Sans broadcast, Spark fait un sort-merge join avec shuffle réseau. Avec broadcast, `movies` est envoyé directement à chaque executor.
- **Mesure avant/après** :
```
Sans broadcast : 1.05s
Avec broadcast : 0.56s
Gain           : 46.2 %
```
- **Ce que ça change** : Le shuffle est éliminé côté `movies`. Sur un cluster avec plusieurs nœuds, le gain serait encore plus marqué car le transfert réseau est supprimé.

---

## 5. Lecture de la Spark UI

- **Job observé** : job 53 — `count` qui matérialise le cache (3 stages, 73 tasks)
- **Où se produit le shuffle** : bloc `Exchange` visible dans le DAG — `ShuffledRowRDD` avant le `WholeStageCodegen`
- **Nombre de stages** : 1 stage complété + 2 stages skipped (grâce au cache)
- **Commentaire** : Les 2 stages skipped prouvent que le cache fonctionne — Spark ne relit pas le Parquet silver pour chaque analyse. Le shuffle se produit lors du `groupBy` sur `movieId`.

---

## 6. Exploration au-delà du cours

- **Piste choisie** : AQE — Adaptive Query Execution
- **Question** : L'AQE améliore-t-il les performances d'une agrégation sur MovieLens ?
- **Protocole** : même agrégation (`groupBy movieId + avg + count`), même données, seul `spark.sql.adaptive.enabled` varie
- **Mesures** :
```
Sans AQE : 0.74s
Avec AQE : 0.34s
Différence : 0.4s
```
- **Conclusion** : L'AQE réduit le temps de 54% sur cette agrégation. Il réoptimise le nombre de partitions de shuffle en cours d'exécution selon les stats réelles — sur un petit dataset comme MovieLens small, il réduit les partitions vides et l'overhead associé.

---

## 7. Ce qu'on a appris et limites

- **Ce qui a marché** : Le broadcast join est l'optimisation la plus visible sur MovieLens, les données étant déjà propres le nettoyage est minimal. L'AQE apporte un gain réel même sur petit volume.
- **Ce qui a bloqué** : Le paramètre `movies` passé inutilement à `nettoyage()` — corrigé en cours de route. L'import `spark_session` nécessitait d'avoir le fichier en local.
- **Ce qu'on ferait avec plus de temps** : Tester le modèle ALS (MLlib) pour la recommandation, tester sur `ml-latest` (le dataset complet, 27M notes) pour voir si les gains d'optimisation sont plus marqués.


