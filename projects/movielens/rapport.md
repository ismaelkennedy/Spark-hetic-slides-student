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

### Analyse 2 - jointure

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

### Analyse 3 - window function

- Question : [...]
- Code clé :
```python
[...]
```
- Résultat (extrait) :
```
[...]
```
- Lecture métier : [...]

---

## 4. Optimisation

- Optimisation choisie : [broadcast / cache / repartition]
- Pourquoi : [...]
- Mesure avant/après ou extrait de plan :
```
avant : [...] s   |   après : [...] s
(ou extrait de explain() montrant le changement)
```
- Ce que ça change : [...]

---

## 5. Lecture de la Spark UI

- Job observé : [...]
- Où se produit le shuffle (`Exchange`) : [...]
- Nombre de stages et de tasks : [...]
- Capture(s) : [insérer]
- Commentaire : [...]

---

## 6. Exploration au-delà du cours

- Piste choisie : [AQE et partitions / skew et salting / UDF vs pandas_udf / table gérée et upsert /
  spark-submit / pushdown mesuré / benchmark formats / streaming ou MLlib]
- Question : [...]
- Protocole (ce qu'on a fait varier, ce qui reste fixe) : [...]
- Mesures :
```
[...]
```
- Conclusion (même si négative ou contre-intuitive) : [...]

---

## 7. Ce qu'on a appris et limites

- Ce qui a marché : [...]
- Ce qui a bloqué : [...]
- Ce qu'on ferait avec plus de temps : [...]
