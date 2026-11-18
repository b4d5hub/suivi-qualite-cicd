# Ingestion GitHub Actions → Kafka

Ce module collecte des **données CI/CD réelles de GitHub Actions** depuis le dépôt [`freeCodeCamp/freeCodeCamp`](https://github.com/freeCodeCamp/freeCodeCamp) et les publie dans Apache Kafka. **Aucun événement synthétique n'est généré.** Il constitue le composant d'ingestion du projet **Suivi de la qualité logicielle**.

## Flux de données

`API REST GitHub → collecteurs et transformations Python → Kafka → lac de données et analyses en aval`

| Topic Kafka   | Source                                               | Clé du message             | Signification                                                                             |
| ------------- | ---------------------------------------------------- | -------------------------- | ----------------------------------------------------------------------------------------- |
| `builds`      | Exécutions et tentatives de workflows GitHub Actions | `run_id:attempt_number`    | Exécution et état d'un workflow/build                                                     |
| `tests`       | Jobs GitHub Actions                                  | Identifiant du job GitHub  | Exécution et résultat d'un job ; **il ne s'agit pas des résultats de chaque cas de test** |
| `deployments` | Déploiements GitHub et leurs statuts                 | Identifiant du déploiement | État du déploiement et historique des statuts disponibles                                 |

## Fichiers du projet

- `src/collector/github_client.py` — requêtes authentifiées à l'API GitHub, nouvelles tentatives après erreur et pagination.
- `src/collector/workflows.py` — collecte des workflows, de leurs tentatives et de leurs jobs, en historique et en incrémental.
- `src/collector/deployments.py` — collecte incrémentale des déploiements et récupération de l'historique des statuts.
- `src/collector/transform.py` — transformation des réponses de l'API en événements.
- `src/collector/kafka_producer.py` — envoi des messages à Kafka et vérification des accusés de réception.
- `src/collector/state.py` — état SQLite des publications, points de reprise, workflows inachevés et déploiements surveillés.
- `src/collector/backfill_batches.py` — pilotage du rattrapage historique des workflows par lots.
- `src/collector/backfill_deployments.py` — collecte historique des déploiements.
- `src/collector/poll_lock.py` — verrou local empêchant deux cycles de collecte simultanés.
- `src/collector/poll_once.py` — point d'entrée pour un cycle complet de collecte incrémentale.
- `tools/` — scripts de diagnostic, de prévisualisation et de démonstration à lancer depuis la racine du dépôt.
- `compose.kafka.yml` — environnement Kafka local.

## Installation (Windows PowerShell)

Exécuter les commandes suivantes **depuis la racine du dépôt** :

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install requests python-dotenv confluent-kafka

docker compose -f .\ingestion\compose.kafka.yml up -d
```

Configurer le fichier `.env` à la racine du projet (ignoré par Git), avec le jeton GitHub attendu par `github_client.py` et les paramètres suivants :

```dotenv
GITHUB_OWNER=freeCodeCamp
GITHUB_REPO=freeCodeCamp
BACKFILL_MONTHS=2
```

**Vérifier dans `github_client.py` le nom exact de la variable d'environnement utilisée pour le jeton GitHub** avant de configurer les identifiants. Ne jamais committer le fichier `.env`, des jetons ou d'autres secrets.

Le broker Kafka local est accessible sur `localhost:9092` pour les scripts exécutés **sur la machine hôte**. Si le collecteur s'exécute **dans Docker**, il faut adapter l'adresse Kafka et la configuration réseau : `localhost` à l'intérieur d'un conteneur ne désigne pas le service Kafka de la machine hôte.

## Exécuter un cycle de collecte incrémentale

```powershell
python .\ingestion\src\collector\poll_once.py
```

Cette commande lance d'abord le collecteur de workflows/jobs, puis celui des déploiements. En cas de succès, la sortie se termine par :

```text
Workflows: SUCCESS
Deployments: SUCCESS
POLLING CYCLE COMPLETED SUCCESSFULLY
```

Ce point d'entrée exécute **un seul cycle** : il ne planifie pas les exécutions suivantes. La planification toutes les cinq minutes, le DAG Airflow et la configuration des conteneurs relèvent du composant d'orchestration. Le collecteur doit disposer d'un stockage d'état persistant et d'un accès au broker Kafka.

## Rattrapage historique (backfill)

Le rattrapage historique des workflows peut être exécuté par lots sur une plage de dates, par exemple :

```powershell
python .\ingestion\src\collector\backfill_batches.py --start 2026-11-01 --end 2026-11-16 --batch-days 3 --max-batches 6
```

Chaque lot exécuté avec succès produit un journal dans `ingestion/state/`. En cas d'échec, le contrôleur s'arrête ; la date concernée peut être retraitée. Les événements déjà enregistrés dans SQLite et dont le contenu n'a pas changé ne sont pas republiés.

Le rattrapage historique des déploiements utilise `backfill_deployments.py`. Son option `--all-pages` parcourt toute la liste des déploiements GitHub et peut entraîner de nombreuses requêtes API. Elle doit être réservée au rattrapage historique volontaire, **pas à une exécution toutes les cinq minutes**.

## Fiabilité et persistance de l'état

- La base SQLite `ingestion/state/collector.db` conserve les clés et empreintes des événements publiés, les identifiants des workflows inachevés, les déploiements surveillés et les watermarks distincts `workflows` / `deployments`.
- L'accusé de réception de Kafka est vérifié **avant** l'enregistrement du checkpoint de l'événement correspondant.
- La réexécution d'un événement inchangé ne provoque pas de nouvelle publication. Si son contenu change, un nouveau message peut être publié avec la même clé.
- Si un cycle de collecte échoue, son watermark final n'avance pas. Les checkpoints des événements déjà livrés pendant ce cycle peuvent néanmoins être conservés.
- Il s'agit d'une ingestion avec nouvelles tentatives et checkpoints, **pas d'une transaction « exactement une fois » entre Kafka et SQLite**.
- `poll_lock.py` empêche les chevauchements sur un **système de fichiers local partagé**. Un déploiement Airflow distribué nécessite également une limitation de concurrence au niveau de l'ordonnanceur ainsi qu'un état persistant partagé de manière appropriée.
- Les déploiements récents et terminés sont revérifiés pendant une période limitée. Les changements de statut de déploiements beaucoup plus anciens peuvent nécessiter une réconciliation périodique ou des webhooks.

**Ne pas committer** `.env`, `ingestion/state/collector.db`, les journaux locaux ou les autres fichiers d'état générés à l'exécution. Ils doivent rester ignorés par Git.

## Couverture historique et limites

État relevé après les tests de rattrapage de novembre 2026 :

| Topic avec checkpoint | Nombre de clés d'événements distinctes |
| --------------------- | -------------------------------------: |
| `builds`              |                                  7 438 |
| `tests`               |                                 22 744 |
| `deployments`         |                                    303 |
| **Total**             |                             **30 485** |

Ces chiffres correspondent aux **clés d'événements enregistrées dans SQLite**, et non à un comptage indépendant des messages encore conservés par le broker Kafka.

Pour la période historique des workflows allant du **17 septembre au 16 novembre 2026**, **57 dates sur 61 ont été traitées**. Quatre dates restent **non vérifiées** : **5, 6, 7 et 15 octobre 2026**. Des observations antérieures de l'API GitHub indiquaient des workflows ces jours-là, alors que les requêtes filtrées ultérieures ont retourné zéro résultat. Le collecteur a rejeté une réponse incohérente le 5 octobre (`expected 12, received 0`). **Ces dates ne doivent pas être considérées comme des journées vérifiées sans activité et il ne faut pas annoncer une couverture historique de 100 %.**

Des validations supplémentaires sont nécessaires pour les workflows de très longue durée, les interruptions pendant lesquelles certaines données historiques de l'API deviennent indisponibles et les changements de statut des déploiements plus anciens que la fenêtre de surveillance configurée.

## Transmission aux responsables de l'orchestration et de l'analyse

Le service d'orchestration doit exécuter :

```text
python ingestion/src/collector/poll_once.py
```

Prévoir : un environnement Python avec les dépendances installées, un jeton d'accès GitHub fourni de manière sécurisée, les paramètres du dépôt GitHub, un accès réseau à Kafka et un accès persistant au répertoire d'état SQLite. Limiter les cycles simultanés et surveiller les codes de sortie non nuls. Les consommateurs en aval doivent utiliser les trois topics Kafka et leurs clés de message stables pour gérer les mises à jour des événements.
