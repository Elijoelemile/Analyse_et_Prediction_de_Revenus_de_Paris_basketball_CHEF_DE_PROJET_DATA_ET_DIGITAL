"""Pipeline ELT - Phase 1 : Extraction (Spark, cluster distribué).

Construit, de façon paresseuse, un RDD Spark de tâches d'extraction
réparties sur le cluster (voir vm cluster/docker-compose.yml) : chaque élément du RDD
correspond à un fichier à récupérer depuis les sources déclarées dans
sources_config.SOURCES. Spark est paresseux — rien n'est lu tant qu'aucune
action n'est déclenchée (c'est load.py qui le fait) : les données restent
en mémoire distribuée sur le cluster, rien n'est écrit sur disque ici.

Chaque tâche (un fichier) est lue et décompressée directement sur
l'exécuteur qui la traite, pas sur le driver — c'est ce qui rend
l'extraction horizontalement scalable : ajouter des workers ajoute de la
capacité de traitement en parallèle.

Doit être exécuté sur le cluster Spark : les chemins de sources_config.py
référencent des volumes montés dans les conteneurs, pas le système de
fichiers Windows.

Utilisation en script (résumé, aucune écriture disque) :
    spark-submit --master spark://spark-master:7077 extract.py
"""
import fnmatch
import gzip
import mimetypes
import os
import zipfile
from datetime import datetime
from glob import glob
from pathlib import Path
from urllib.parse import urlparse

import requests
from pyspark.sql import SparkSession

from logging_config import get_logger
from sources_config import SOURCES


def list_zip_tasks(source):
    """Énumère (sans les lire) les entrées du zip correspondant à internal_pattern."""
    logger = get_logger("extract")
    try:
        with zipfile.ZipFile(source["zip_path"]) as archive:
            names = archive.namelist()
    except (OSError, zipfile.BadZipFile) as e:
        logger.error(f"impossible d'ouvrir l'archive {source['zip_path']} : {e}")
        return []

    matches = [n for n in names if fnmatch.fnmatch(n, source["internal_pattern"])]
    if not matches:
        logger.warning(f"aucune entrée ne correspond au motif : {source['internal_pattern']}")
    return [{"source": source, "kind": "zip_entry", "entry_name": n} for n in matches]


def list_path_tasks(source):
    """Énumère (sans les lire) les fichiers locaux correspondant au motif glob."""
    logger = get_logger("extract")
    matches = glob(source["pattern"])
    if not matches:
        logger.warning(f"aucun fichier ne correspond au motif : {source['pattern']}")
    return [{"source": source, "kind": "path_file", "file_path": m} for m in matches]


def list_api_tasks(source):
    """Une source API correspond à une seule tâche (un seul appel HTTP)."""
    return [{"source": source, "kind": "api_call"}]


def infer_extension(response, url):
    """Déduit l'extension du fichier à partir du Content-Type, ou de l'URL en repli."""
    content_type = response.headers.get("Content-Type", "").split(";")[0].strip()
    ext = mimetypes.guess_extension(content_type) if content_type else None

    if not ext:
        url_ext = Path(urlparse(url).path).suffix
        ext = url_ext if url_ext else ".bin"

    return ext


def run_task(task):
    """Exécute une tâche d'extraction et renvoie (nom_source, nom_fichier, contenu).
    Tourne sur l'exécuteur qui reçoit la tâche, pas sur le driver."""
    logger = get_logger("extract")
    source = task["source"]
    name = source["name"]

    if task["kind"] == "zip_entry":
        entry_name = task["entry_name"]
        entry_filename = Path(entry_name).name
        try:
            with zipfile.ZipFile(source["zip_path"]) as archive:
                raw = archive.read(entry_name)
            if entry_filename.lower().endswith(".gz"):
                return name, entry_filename[:-3], gzip.decompress(raw)
            return name, entry_filename, raw
        except (zipfile.BadZipFile, gzip.BadGzipFile, OSError, EOFError) as e:
            logger.error(f"échec d'extraction de {entry_name} : {e}")
            return None

    if task["kind"] == "path_file":
        file_path = Path(task["file_path"])
        try:
            return name, file_path.name, file_path.read_bytes()
        except OSError as e:
            logger.error(f"échec de lecture de {file_path.name} : {e}")
            return None

    if task["kind"] == "api_call":
        headers = {}
        auth_env_var = source.get("auth_env_var")
        if auth_env_var:
            token = os.environ.get(auth_env_var)
            if not token:
                logger.warning(f"ignoré : variable d'environnement {auth_env_var} non définie")
                return None
            headers["Authorization"] = f"Bearer {token}"

        try:
            response = requests.get(source["url"], headers=headers, params=source.get("params"))
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"échec de la requête API vers {source['url']} : {e}")
            return None

        ext = infer_extension(response, source["url"])
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return name, f"{name}_{timestamp}{ext}", response.content

    logger.error(f"type de tâche inconnu : {task['kind']}")
    return None


def get_spark():
    """Se connecte au cluster Spark (voir vm cluster/docker-compose.yml)."""
    master = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
    return SparkSession.builder.appName("pbb-extract").master(master).getOrCreate()


def extract_all(spark=None):
    """Construit, sans rien exécuter, le RDD Spark des données à extraire
    (nom_source, nom_fichier, contenu). Paresseux : c'est l'action
    déclenchée par load.py qui exécute réellement les tâches sur le
    cluster."""
    logger = get_logger("extract")
    spark = spark or get_spark()

    tasks = []
    for source in SOURCES:
        logger.info(f"[{source['name']}] ({source['type']})")
        if source["type"] == "zip":
            tasks.extend(list_zip_tasks(source))
        elif source["type"] == "path":
            tasks.extend(list_path_tasks(source))
        elif source["type"] == "api":
            tasks.extend(list_api_tasks(source))
        else:
            logger.error(f"type de source inconnu : {source['type']}")

    logger.info(f"{len(tasks)} tâches réparties sur le cluster")
    rdd = spark.sparkContext.parallelize(tasks, numSlices=max(len(tasks), 1))
    return rdd.map(run_task).filter(lambda r: r is not None)


def main():
    """Lance l'extraction sur le cluster et affiche un résumé (rien n'est écrit sur disque)."""
    logger = get_logger("extract")
    spark = get_spark()
    rdd = extract_all(spark)
    total_files, total_bytes = rdd.map(lambda r: (1, len(r[2]))).reduce(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )
    logger.info(f"Total : {total_files} fichiers, {total_bytes:,} octets récupérés en mémoire distribuée.")
    spark.stop()


if __name__ == "__main__":
    main()
