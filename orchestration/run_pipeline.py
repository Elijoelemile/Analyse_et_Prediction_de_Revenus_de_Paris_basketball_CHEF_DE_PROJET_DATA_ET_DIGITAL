"""Orchestrateur du pipeline ELT (Extract -> Load -> Stage -> Transform).

Enchaîne les étapes du pipeline sur le cluster Spark, en couvrant ce qui
manquait pour une exécution automatisée :

- Planification : ce script est fait pour être lancé automatiquement par
  le Planificateur de tâches Windows (pas de cron sous Windows) — voir
  register_scheduled_task.ps1 pour l'enregistrer.
- Gestion de dépendance : chaque étape ne démarre que si la précédente a
  réussi (code de sortie 0) — load.py, puis stage.py, puis transform.py.
  Si l'une échoue, les suivantes ne sont pas lancées.
- Échec visible : en cas d'échec d'une étape, le pipeline s'arrête, log
  l'erreur en ERROR (voir logs/pipeline.log) et sort avec un code non nul
  — ce que le Planificateur de tâches peut détecter (historique des
  exécutions, action "en cas d'échec").
- Historique des exécutions : chaque run ajoute une ligne à
  logs/run_history.jsonl (JSON Lines : une exécution par ligne, avec le
  statut et la durée de chaque étape) pour garder une vue d'ensemble des
  exécutions passées sans avoir besoin d'une interface dédiée.
- Archivage quotidien : à la fin de chaque run (succès ou échec), les
  lignes du jour dans logs/pipeline.log sont copiées dans
  "log storage/AAAA-MM-JJ.log" — un fichier texte par jour, toujours
  greppable, pour ne pas laisser logs/pipeline.log grossir indéfiniment
  tout en gardant un historique consultable.

Ce script tourne sur Windows (pas dans un conteneur) : il pilote Docker
Compose pour s'assurer que le cluster Spark est démarré, puis soumet les
jobs comme on le fait manuellement.

Utilisation :
    python run_pipeline.py
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent
ELT_DIR = PROJECT_DIR / "elt"
VM_CLUSTER_DIR = PROJECT_DIR / "vm cluster"
HISTORY_FILE = PROJECT_DIR / "logs" / "run_history.jsonl"
LOG_STORAGE_DIR = PROJECT_DIR / "log storage"

sys.path.insert(0, str(PROJECT_DIR / "config"))
from logging_config import get_logger, LOG_FILE  # noqa: E402

PY_FILES = ",".join([
    "/app/elt/extract.py",
    "/app/config/logging_config.py",
    "/app/config/sources_config.py",
])


def ensure_cluster_up():
    """Démarre le cluster Spark (master + 2 workers) s'il n'est pas déjà actif.
    Sans effet s'il tourne déjà (docker compose up est idempotent)."""
    logger = get_logger("orchestrator")
    logger.info("vérification du cluster Spark...")
    subprocess.run(
        ["docker", "compose", "up", "-d", "--scale", "spark-worker=2"],
        cwd=VM_CLUSTER_DIR, check=True, capture_output=True, text=True,
    )


def run_spark_job(script_name):
    """Soumet un script au cluster via spark-submit. Renvoie True si l'étape
    a réussi (code de sortie 0), False sinon."""
    logger = get_logger("orchestrator")
    logger.info(f"lancement de {script_name}...")

    result = subprocess.run(
        [
            "docker", "compose", "run", "--rm", "spark-submit",
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--py-files", PY_FILES,
            f"/app/elt/{script_name}",
        ],
        cwd=VM_CLUSTER_DIR, capture_output=True, text=True,
    )

    if result.returncode == 0:
        logger.info(f"{script_name} terminé avec succès")
        return True

    logger.error(f"{script_name} a échoué (code {result.returncode})")
    logger.error(result.stderr[-3000:] or result.stdout[-3000:])
    return False


def record_run(steps, overall_status, started_at, finished_at):
    """Ajoute une ligne à l'historique des exécutions (logs/run_history.jsonl)."""
    HISTORY_FILE.parent.mkdir(exist_ok=True)
    entry = {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_s": round((finished_at - started_at).total_seconds(), 1),
        "status": overall_status,
        "steps": steps,
    }
    with open(HISTORY_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def archive_daily_log():
    """Copie dans "log storage/AAAA-MM-JJ.log" toutes les lignes de
    logs/pipeline.log datées d'aujourd'hui (extract, load, stage et
    orchestrator confondus, puisqu'ils partagent le même fichier). Reste du
    texte brut, greppable, pas de dépendance supplémentaire."""
    logger = get_logger("orchestrator")
    today = datetime.now().strftime("%Y-%m-%d")

    if not LOG_FILE.exists():
        return

    with open(LOG_FILE, encoding="utf-8") as f:
        todays_lines = [line for line in f if line.startswith(today)]

    if not todays_lines:
        logger.warning("aucune ligne de log datée d'aujourd'hui, rien à archiver")
        return

    LOG_STORAGE_DIR.mkdir(exist_ok=True)
    archive_path = LOG_STORAGE_DIR / f"{today}.log"
    with open(archive_path, "w", encoding="utf-8") as f:
        f.writelines(todays_lines)
    logger.info(f"{len(todays_lines)} lignes archivées -> {archive_path.name}")


def run():
    logger = get_logger("orchestrator")
    started_at = datetime.now(timezone.utc)
    steps = []

    try:
        ensure_cluster_up()
    except subprocess.CalledProcessError as e:
        logger.error(f"impossible de démarrer le cluster Spark : {e.stderr}")
        record_run(steps, "failed", started_at, datetime.now(timezone.utc))
        sys.exit(1)

    for step_name in ("load.py", "stage.py", "transform.py"):
        t0 = time.monotonic()
        success = run_spark_job(step_name)
        steps.append({
            "name": step_name,
            "status": "success" if success else "failed",
            "duration_s": round(time.monotonic() - t0, 1),
        })
        if not success:
            logger.error(f"pipeline arrêté : {step_name} a échoué")
            record_run(steps, "failed", started_at, datetime.now(timezone.utc))
            sys.exit(1)

    record_run(steps, "success", started_at, datetime.now(timezone.utc))
    logger.info("pipeline terminé avec succès (load + stage + transform)")


def main():
    """Filet de sécurité : capture toute erreur imprévue (docker introuvable,
    bug, etc.) pour qu'elle soit toujours loggée en ERROR et enregistrée dans
    l'historique, plutôt que de laisser le script planter silencieusement du
    point de vue du log."""
    logger = get_logger("orchestrator")
    started_at = datetime.now(timezone.utc)
    try:
        run()
    except SystemExit:
        raise
    except Exception:
        logger.exception("échec inattendu de l'orchestrateur")
        record_run([], "failed", started_at, datetime.now(timezone.utc))
        sys.exit(1)
    finally:
        archive_daily_log()


if __name__ == "__main__":
    main()
