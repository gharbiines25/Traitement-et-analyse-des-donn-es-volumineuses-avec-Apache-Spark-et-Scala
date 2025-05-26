# 📊 E-commerce Data Analytics with Apache Spark & Scala

Ce projet propose une analyse complète d'un fichier de données e-commerce en utilisant **Apache Spark** avec **Scala**. Il comprend des étapes de prétraitement, d'analyse statistique et de visualisation sous forme de dashboard HTML interactif.

## 📁 Structure du projet

```
ecommerce-spark-project/
|
├── data/
│   └── ecommerce_data_enriched.csv   # Données brutes au format CSV
│
├── src/
│   └── main/
│       └── scala/
│           └── EcommercePreprocessing.scala   # Script principal Scala
│
├── output/
│   └── dashboard.html   # Dashboard généré en HTML
│
├── build.sbt   # Dépendances SBT
└── README.md   # Ce fichier
```

## 🚀 Technologies utilisées

* Apache Spark 3.5.1
* Scala 2.12
* Chart.js (pour les visualisations HTML)
* commons-lang3 (nettoyage de texte)

## ✅ Étapes du projet

### 1. Prétraitement des données

* Chargement du CSV avec `sc.textFile`
* Nettoyage des lignes incomplètes ou erronées
* Contrôle des types et des valeurs logiques (durée, montant, note)
* Normalisation : capping, suppression d'accents, uniformisation des textes
* Transformation en objets `EcommerceSession`
* Mise en cache pour optimiser les traitements

### 2. Analyse de données

* Moyennes par catégorie, pays, appareil
* Distribution des notes, sessions, achats
* Taux de conversion global et par critère
* Analyse temporelle : heures, jours de la semaine, pics de trafic

### 3. Visualisations (HTML/Chart.js)

* Sessions par heure
* Achats par heure
* Taux de conversion par appareil
* Répartition des notes d’avis
* Taux d’achat par nombre de pages vues
* Dashboard interactif `output/dashboard.html`

## 📊 Exemple de résultats

* Sessions valides après nettoyage : **493 260**
* Taux de conversion global : **98,98%**
* Produit le plus consulté : **High-tech**
* Heures de forte activité : entre **10h et 11h**

## 📅 Lancer le projet

### 1. Prérequis

* Java 8+
* Scala 2.12
* SBT
* Apache Spark installé ou intégré via `sbt`

### 2. Compilation

```bash
sbt compile
```

### 3. Exécution

```bash
sbt run
```

Le fichier `output/dashboard.html` sera généré automatiquement.
