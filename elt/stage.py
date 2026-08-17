"""Pipeline ELT - Phase 3 : Staging (Spark, cluster distribué).

Lit les fichiers JSON de commandes déposés par load.py dans
Data Lake/orders/*.json, aplatit leur structure imbriquée (commande ->
produits -> billets, plus frais/paiements) en tables normalisées, et
écrit UNE table CSV complète par type dans Staging/orders/ :
    orders.csv, products.csv, tickets.csv, charges.csv, payments.csv

Chaque fichier source est parsé en parallèle sur le cluster (un fichier =
une tâche). Tous les fichiers partagent le même schéma, donc leurs lignes
d'un même type sont regroupées ensemble pour produire une seule table
complète par type, plutôt qu'une table par fichier.

Extrait aussi les tables de référence (sessions, venues, events,
competitions, teams) trouvées sous la clé "included" de chaque fichier —
c'est là que se trouve le calendrier des matchs : sessions.id correspond
au session_id de tickets.csv, et permet de savoir à quel match (nom,
date, lieu) appartient chaque billet. Ces tables sont des dimensions, pas
des faits : contrairement aux commandes, on ne les concatène pas — pour
chaque id, seule la version du fichier source le plus récent est gardée
(le calendrier peut être mis à jour/finalisé d'un export à l'autre).

Reprend aussi telles quelles les tables fb_transactions et
boutique_ventes_avoirs depuis le Data Lake, déposées chacune dans son
propre sous-dossier de Staging/. La colonne d'identifiant client externe
est harmonisée sous le nom "ext_id" dans les trois tables (orders,
fb_transactions, boutique_ventes_avoirs) pour permettre de les joindre
facilement plus tard.

Remplace l'ancien script convert_orders_to_csv.py (supprimé), dont il
reprend la logique d'aplatissement.

Une erreur sur un fichier (JSON invalide, tronqué de façon irrécupérable)
est loggée et n'interrompt pas le traitement des autres — voir
logs/pipeline.log.

Doit être exécuté sur le cluster Spark (voir vm cluster/docker-compose.yml).
Utilisation (depuis le conteneur spark-submit) :
    spark-submit --master spark://spark-master:7077
      --py-files /app/elt/extract.py,/app/config/logging_config.py,/app/config/sources_config.py
      /app/elt/stage.py
"""
import csv
import json
from collections import defaultdict
from pathlib import Path

from extract import get_spark
from logging_config import get_logger

DATA_LAKE = Path(__file__).parent.parent / "Data Lake"
STAGING = Path(__file__).parent.parent / "Staging"

# Délimiteur commun à toutes les tables de Staging, quel que soit le
# délimiteur d'origine de leur source (ex: boutique_ventes_avoirs.csv
# utilise ";" dans le Data Lake).
STAGING_DELIMITER = ","

ORDER_FIELDS = [
    "source_file", "order_id", "event_id", "order_status_id", "channel_id",
    "language", "creation_date", "validation_date", "amend_date",
    "validation_user", "revision_user", "is_secondary_market",
    "has_related_orders", "related_order_ids",
    "customer_id", "ext_id",
    "delivery_last_name", "delivery_first_name", "delivery_street1",
    "delivery_street2", "delivery_street3", "delivery_city",
    "delivery_postal_code", "delivery_country_name",
    "invoice_last_name", "invoice_first_name", "invoice_street1",
    "invoice_street2", "invoice_street3", "invoice_city",
    "invoice_postal_code", "invoice_country_name",
    "external_reference_id", "external_reference_source",
]

PRODUCT_FIELDS = [
    "source_file", "order_id", "order_product_id", "amount_excl_tax",
    "amount_inc_tax", "fidelity_points", "show_code", "product_price_id",
    "product_type_id", "is_cancelled",
]

TICKET_FIELDS = [
    "source_file", "order_id", "order_product_id", "ticket_id", "bar_code",
    "category_id", "session_id", "amount", "ticket_status_id",
    "ticket_format_id", "seat_id", "gate", "stand", "level", "stairs",
    "row", "number", "creation_date", "amend_date",
]

CHARGE_FIELDS = [
    "source_file", "order_id", "charge_id", "delivery_mode_id", "amount",
]

PAYMENT_FIELDS = [
    "source_file", "order_id", "payment_id", "payment_type_id", "date",
    "amount", "is_cancelled",
]

TABLE_FIELDS = {
    "orders": ORDER_FIELDS,
    "products": PRODUCT_FIELDS,
    "tickets": TICKET_FIELDS,
    "charges": CHARGE_FIELDS,
    "payments": PAYMENT_FIELDS,
}

# Tables de référence trouvées sous la clé "included" de chaque fichier
# ORDERS_*.json. sessions.id est la clé jointe par tickets.session_id ;
# venues/competitions/teams sont référencées par les colonnes *_id de
# sessions ; events.id est la clé jointe par orders.event_id.
SESSION_FIELDS = [
    "source_file", "id", "competition_id", "name", "label", "start_at",
    "end_at", "final_schedule_set", "information", "is_active", "is_away",
    "session_type_id", "venue_id", "home_team_id", "secondary_market",
    "trial_id",
]

VENUE_FIELDS = ["source_file", "id", "code", "name", "label", "capacity", "information"]

EVENT_FIELDS = [
    "source_file", "id", "code", "name", "label", "start_date", "end_date",
    "year", "event_type_code",
]

COMPETITION_FIELDS = ["source_file", "id", "code", "name", "label"]

TEAM_FIELDS = ["source_file", "id", "name"]

REFERENCE_TABLE_FIELDS = {
    "sessions": SESSION_FIELDS,
    "venues": VENUE_FIELDS,
    "events": EVENT_FIELDS,
    "competitions": COMPETITION_FIELDS,
    "teams": TEAM_FIELDS,
}


def recover_orders_array(text):
    """Retrouve, par comptage d'accolades/crochets, juste le tableau
    'orders' d'un JSON tronqué (ex : coupé en fin d'écriture) plutôt que
    d'essayer de parser tout le document. Les tables de référence
    'included' ne sont pas récupérables dans ce cas (elles arrivent après
    'orders' dans le fichier)."""
    start = text.find("[")
    depth = 0
    in_str = False
    esc = False
    end = None
    for i in range(start, len(text)):
        c = text[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                end = i
                break
    if end is None:
        raise ValueError("impossible de récupérer un tableau 'orders' dans ce JSON tronqué")
    return json.loads(text[start:end + 1])


def parse_orders_document(text):
    """Parse un fichier ORDERS_*.json et renvoie (orders, included), où
    `included` est le dict des tables de référence (sessions, venues,
    events, competitions, teams). En cas de JSON tronqué, retombe sur la
    récupération du seul tableau 'orders' ; `included` est alors vide pour
    ce fichier (les autres fichiers du run fourniront ces données)."""
    try:
        data = json.loads(text)
        return data.get("orders", []), data.get("included", {})
    except json.JSONDecodeError:
        return recover_orders_array(text), {}


def flatten_order(order, source_file):
    customer = order.get("customer") or {}
    delivery = order.get("delivery_address") or {}
    invoice = order.get("invoice_address") or {}
    ext_ref = order.get("external_references") or {}
    return {
        "source_file": source_file,
        "order_id": order.get("order_id"),
        "event_id": order.get("event_id"),
        "order_status_id": order.get("order_status_id"),
        "channel_id": order.get("channel_id"),
        "language": order.get("language"),
        "creation_date": order.get("creation_date"),
        "validation_date": order.get("validation_date"),
        "amend_date": order.get("amend_date"),
        "validation_user": order.get("validation_user"),
        "revision_user": order.get("revision_user"),
        "is_secondary_market": order.get("is_secondary_market"),
        "has_related_orders": order.get("has_related_orders"),
        "related_order_ids": ";".join(str(i) for i in (order.get("related_order_ids") or [])),
        "customer_id": customer.get("customer_id"),
        "ext_id": customer.get("external_id"),
        "delivery_last_name": delivery.get("last_name"),
        "delivery_first_name": delivery.get("first_name"),
        "delivery_street1": delivery.get("street1"),
        "delivery_street2": delivery.get("street2"),
        "delivery_street3": delivery.get("street3"),
        "delivery_city": delivery.get("city"),
        "delivery_postal_code": delivery.get("postal_code"),
        "delivery_country_name": delivery.get("country_name"),
        "invoice_last_name": invoice.get("last_name"),
        "invoice_first_name": invoice.get("first_name"),
        "invoice_street1": invoice.get("street1"),
        "invoice_street2": invoice.get("street2"),
        "invoice_street3": invoice.get("street3"),
        "invoice_city": invoice.get("city"),
        "invoice_postal_code": invoice.get("postal_code"),
        "invoice_country_name": invoice.get("country_name"),
        "external_reference_id": ext_ref.get("id"),
        "external_reference_source": ext_ref.get("source"),
    }


def flatten_products(order, source_file):
    rows = []
    for product in order.get("products") or []:
        rows.append({
            "source_file": source_file,
            "order_id": order.get("order_id"),
            "order_product_id": product.get("order_product_id"),
            "amount_excl_tax": product.get("amount_excl_tax"),
            "amount_inc_tax": product.get("amount_inc_tax"),
            "fidelity_points": product.get("fidelity_points"),
            "show_code": product.get("show_code"),
            "product_price_id": product.get("product_price_id"),
            "product_type_id": product.get("product_type_id"),
            "is_cancelled": product.get("is_cancelled"),
        })
    return rows


def flatten_tickets(order, source_file):
    rows = []
    for product in order.get("products") or []:
        for ticket in product.get("tickets") or []:
            seats = ticket.get("seats") or {}
            rows.append({
                "source_file": source_file,
                "order_id": order.get("order_id"),
                "order_product_id": product.get("order_product_id"),
                "ticket_id": ticket.get("ticket_id"),
                "bar_code": ticket.get("bar_code"),
                "category_id": ticket.get("category_id"),
                "session_id": ticket.get("session_id"),
                "amount": ticket.get("amount"),
                "ticket_status_id": ticket.get("ticket_status_id"),
                "ticket_format_id": ticket.get("ticket_format_id"),
                "seat_id": seats.get("seat_id"),
                "gate": seats.get("gate"),
                "stand": seats.get("stand"),
                "level": seats.get("level"),
                "stairs": seats.get("stairs"),
                "row": seats.get("row"),
                "number": seats.get("number"),
                "creation_date": ticket.get("creation_date"),
                "amend_date": ticket.get("amend_date"),
            })
    return rows


def flatten_charges(order, source_file):
    rows = []
    for charge in order.get("charges") or []:
        rows.append({
            "source_file": source_file,
            "order_id": order.get("order_id"),
            "charge_id": charge.get("charge_id"),
            "delivery_mode_id": charge.get("delivery_mode_id"),
            "amount": charge.get("amount"),
        })
    return rows


def flatten_payments(order, source_file):
    rows = []
    for payment in order.get("payments") or []:
        rows.append({
            "source_file": source_file,
            "order_id": order.get("order_id"),
            "payment_id": payment.get("id"),
            "payment_type_id": payment.get("payment_type_id"),
            "date": payment.get("date"),
            "amount": payment.get("amount"),
            "is_cancelled": payment.get("is_cancelled"),
        })
    return rows


def process_file(file_path):
    """Parse un fichier JSON de commandes et renvoie une liste de
    (nom_table, ligne) pour toutes ses commandes/produits/billets/frais/
    paiements, ainsi que pour les tables de référence (sessions, venues,
    events, competitions, teams) trouvées dans sa section 'included'.
    Tourne sur l'exécuteur qui reçoit la tâche, pas sur le driver."""
    logger = get_logger("stage")
    path = Path(file_path)
    source_file = path.name
    tagged_rows = []

    try:
        text = path.read_text(encoding="utf-8")
        orders, included = parse_orders_document(text)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        logger.error(f"fichier ignoré {source_file} : {e}")
        return tagged_rows

    for order in orders:
        tagged_rows.append(("orders", flatten_order(order, source_file)))
        tagged_rows.extend(("products", row) for row in flatten_products(order, source_file))
        tagged_rows.extend(("tickets", row) for row in flatten_tickets(order, source_file))
        tagged_rows.extend(("charges", row) for row in flatten_charges(order, source_file))
        tagged_rows.extend(("payments", row) for row in flatten_payments(order, source_file))

    for table_name in REFERENCE_TABLE_FIELDS:
        for row in included.get(table_name, []):
            tagged_rows.append((table_name, {**row, "source_file": source_file}))

    return tagged_rows


def dedup_reference_rows(rows):
    """Garde, pour chaque id, la ligne provenant du fichier source le plus
    récent — les tables de référence sont des dimensions (pas des faits) :
    on ne les concatène pas comme les commandes, on garde leur dernière
    version connue."""
    latest_by_id = {}
    for row in rows:
        row_id = row.get("id")
        existing = latest_by_id.get(row_id)
        if existing is None or row["source_file"] > existing["source_file"]:
            latest_by_id[row_id] = row
    return list(latest_by_id.values())


def write_csv(path, fieldnames, rows):
    logger = get_logger("stage")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=STAGING_DELIMITER, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"{len(rows):>6} lignes -> {path.name}")
    except OSError as e:
        logger.error(f"échec d'écriture de {path.name} : {e}")


def stage_passthrough_csv(source_path, dest_path, source_delimiter=",", rename_columns=None):
    """Copie un CSV du Data Lake vers Staging, en renommant éventuellement
    des colonnes d'en-tête au passage (ex: harmoniser le nom de la colonne
    d'identifiant client externe en "ext_id"). Lit avec `source_delimiter`
    (le délimiteur du fichier d'origine) mais écrit toujours avec
    STAGING_DELIMITER, pour que toutes les tables de Staging partagent le
    même délimiteur."""
    logger = get_logger("stage")
    rename_columns = rename_columns or {}
    try:
        with open(source_path, encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f, delimiter=source_delimiter)
            header = [rename_columns.get(col, col) for col in next(reader)]

            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with open(dest_path, "w", encoding="utf-8-sig", newline="") as out:
                writer = csv.writer(out, delimiter=STAGING_DELIMITER)
                writer.writerow(header)
                row_count = 0
                for row in reader:
                    writer.writerow(row)
                    row_count += 1
        logger.info(f"{row_count:>6} lignes -> {dest_path.name}")
    except OSError as e:
        logger.error(f"échec du traitement de {source_path.name} : {e}")


def main():
    """Traite en parallèle tous les fichiers Data Lake/orders/*.json et
    écrit les 5 tables normalisées regroupées dans Staging/orders/, puis
    reprend telles quelles les tables fb_transactions et
    boutique_ventes_avoirs dans leurs propres sous-dossiers de Staging/."""
    logger = get_logger("stage")

    data_lake_orders = DATA_LAKE / "orders"
    file_paths = [str(p) for p in data_lake_orders.glob("*.json")]
    logger.info(f"{len(file_paths)} fichiers orders à traiter")

    if file_paths:
        spark = get_spark()
        rdd = spark.sparkContext.parallelize(file_paths, numSlices=len(file_paths))
        tagged_rows = rdd.flatMap(process_file).collect()
        spark.stop()

        tables = defaultdict(list)
        for table_name, row in tagged_rows:
            tables[table_name].append(row)

        for table_name, fields in TABLE_FIELDS.items():
            write_csv(STAGING / "orders" / f"{table_name}.csv", fields, tables[table_name])

        for table_name, fields in REFERENCE_TABLE_FIELDS.items():
            rows = dedup_reference_rows(tables[table_name])
            write_csv(STAGING / "orders" / f"{table_name}.csv", fields, rows)
    else:
        logger.warning(f"aucun fichier trouvé dans {data_lake_orders}")

    stage_passthrough_csv(
        DATA_LAKE / "fb_transactions" / "fb_transactions.csv",
        STAGING / "fb_transactions" / "fb_transactions.csv",
    )

    stage_passthrough_csv(
        DATA_LAKE / "boutique_ventes_avoirs" / "boutique_ventes_avoirs.csv",
        STAGING / "boutique_ventes_avoirs" / "boutique_ventes_avoirs.csv",
        source_delimiter=";",
        rename_columns={"VENTe_client": "ext_id"},
    )


if __name__ == "__main__":
    main()
