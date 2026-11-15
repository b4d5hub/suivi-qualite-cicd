# Suivi de la qualité CI/CD — Big Data

Une plateforme Big Data dédiée à l'analyse de la qualité des processus CI/CD,
utilisant les données GitHub Actions de freeCodeCamp.

## Architecture

API GitHub → Collecteur Python → Apache Kafka
→ MinIO/Hudi → Apache Spark
→ PostgreSQL → Grafana

## Équipe

- Badr Eddine OUAJIH : API GitHub + Kafka
- Mohamed OUABEN : Data Lake + Hudi
- Kamal ZAITAR : Spark + Analytique + MLlib
- Youssef Alahyane : Docker + Airflow + Grafana

## Source des données

https://github.com/freeCodeCamp/freeCodeCamp

## Topics Kafka

- builds
- tests
- deployments

## Développement

Ce projet est en cours de développement actif.
