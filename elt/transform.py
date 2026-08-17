"""Pipeline ELT - Phase 4 : Transform (Spark, cluster distribué).

Modélise les tables normalisées de Staging/ en schéma en étoile (tables
de faits + dimensions) et écrit chaque table au format Parquet dans
Data Warehouse/Rev Paris Basketball/ — un fichier par table (coalesce(1)).

Conçu pour l'analyse "revenu par match" décidée avec l'utilisateur :

Dimensions :
- dim_matchs   : depuis sessions (+ venues, competitions). Grain = 1 match.
- dim_client   : ext_id unifié entre orders/fb_transactions/boutique.
- dim_date     : calendrier (saison 2025-07-01 -> 2026-12-31).

Faits :
- fact_billetterie : grain = billet non annulé (products.is_cancelled=False).
  session_id direct -> jointure propre à dim_matchs.
- fact_buvette      : grain = transaction buvette. match_id de
  fb_transactions n'existe pas dans sessions ; on le retrouve par date
  (date_match == sessions.start_at, mapping vérifié 41/41 sans ambiguïté).
- fact_boutique     : grain = ligne de vente boutique. session_id rempli
  uniquement pour les clients n'ayant vu qu'un seul match (cas non
  ambigu) — sinon NULL, cf. discussion sur le fan-out des clients
  multi-matchs.
- fact_commandes    : grain = charge (frais de livraison), niveau
  commande — pas attribuable à un match précis (une commande peut
  couvrir plusieurs matchs, ex: abonnements).
- fact_paiements    : grain = paiement, filtré sur is_cancelled=False
  (is_cancelled=True correspond à des tentatives de paiement
  échouées/dupliquées, pas à du revenu réel — vérifié sur les données).

Doit être exécuté sur le cluster Spark (voir vm cluster/docker-compose.yml).
Utilisation (depuis le conteneur spark-submit) :
    spark-submit --master spark://spark-master:7077
      --py-files /app/elt/extract.py,/app/config/logging_config.py,/app/config/sources_config.py
      /app/elt/transform.py
"""
from pathlib import Path

from pyspark.sql import Window, functions as F

from extract import get_spark
from logging_config import get_logger

PROJECT_DIR = Path(__file__).parent.parent
STAGING = PROJECT_DIR / "Staging"
WAREHOUSE = PROJECT_DIR / "Data Warehouse" / "Rev Paris Basketball"

logger = get_logger("transform")


def read_staging_csv(spark, relative_path):
    """Lit un CSV de Staging avec les types inférés (nombres, booléens)."""
    return spark.read.csv(str(STAGING / relative_path), header=True, inferSchema=True)


def dedup_by_latest_file(df, id_column):
    """Ne garde, pour chaque valeur de id_column, que la ligne du fichier
    source le plus récent. Corrige un cas réel : certaines commandes sont
    amendées puis réexportées dans un fichier ultérieur (3 order_id sur
    84 658, qui se répercutent aussi sur products/tickets/charges/
    payments) — sans ce filtre, une jointure sur ces id démultiplie les
    lignes concernées (fan-out)."""
    window = Window.partitionBy(id_column).orderBy(F.col("source_file").desc())
    return (
        df
        .withColumn("_rank", F.row_number().over(window))
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )


def read_orders_table(spark, relative_path, id_column):
    """Lit une table de la famille orders (orders/products/tickets/charges/
    payments) et la dédoublonne par id_column (voir dedup_by_latest_file)."""
    return dedup_by_latest_file(read_staging_csv(spark, relative_path), id_column)


def build_dim_matchs(spark):
    """1 ligne par match, enrichie avec le lieu et la compétition."""
    sessions = read_staging_csv(spark, "orders/sessions.csv")
    venues = read_staging_csv(spark, "orders/venues.csv")
    competitions = read_staging_csv(spark, "orders/competitions.csv")

    return (
        sessions
        .withColumnRenamed("id", "session_id")
        .withColumn("match_date", F.substring("start_at", 1, 10).cast("date"))
        .join(
            venues.select(
                F.col("id").alias("venue_id"),
                F.col("name").alias("venue_name"),
                F.col("capacity").alias("venue_capacity"),
            ),
            "venue_id", "left",
        )
        .join(
            competitions.select(
                F.col("id").alias("competition_id"),
                F.col("name").alias("competition_name"),
            ),
            "competition_id", "left",
        )
        .select(
            "session_id", "name", "label", "match_date", "start_at", "end_at",
            "is_away", "venue_id", "venue_name", "venue_capacity",
            "competition_id", "competition_name",
        )
    )


def build_dim_client(spark):
    """ext_id unifié entre les trois sources (orders, fb_transactions, boutique)."""
    orders = read_orders_table(spark, "orders/orders.csv", "order_id")
    fb = read_staging_csv(spark, "fb_transactions/fb_transactions.csv")
    boutique = read_staging_csv(spark, "boutique_ventes_avoirs/boutique_ventes_avoirs.csv")

    orders_clients = orders.select("ext_id", "customer_id").where(F.col("ext_id").isNotNull())
    fb_clients = (
        fb.select("ext_id").where(F.col("ext_id").isNotNull())
        .withColumn("customer_id", F.lit(None).cast("long"))
    )
    boutique_clients = (
        boutique.select("ext_id").where(F.col("ext_id").isNotNull())
        .withColumn("customer_id", F.lit(None).cast("long"))
    )

    all_clients = orders_clients.unionByName(fb_clients).unionByName(boutique_clients)
    return all_clients.groupBy("ext_id").agg(F.first("customer_id", ignorenulls=True).alias("customer_id"))


def build_dim_date(spark):
    """Calendrier couvrant la saison (et une marge), pour analyser par jour/mois/semaine."""
    return (
        spark.sql("SELECT explode(sequence(to_date('2025-07-01'), to_date('2026-12-31'), interval 1 day)) as date")
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    )


def build_fact_billetterie(spark):
    """Grain = billet. Exclut les billets dont le produit associé est annulé."""
    tickets = read_orders_table(spark, "orders/tickets.csv", "ticket_id")
    products = read_orders_table(spark, "orders/products.csv", "order_product_id")
    orders = read_orders_table(spark, "orders/orders.csv", "order_id")

    valid_products = (
        products.where(F.col("is_cancelled") == False)  # noqa: E712
        .select("order_product_id")
    )
    orders_ext = orders.select("order_id", "ext_id")

    return (
        tickets
        .join(valid_products, "order_product_id", "inner")
        .join(orders_ext, "order_id", "left")
        .withColumn("ticket_date", F.substring("creation_date", 1, 10).cast("date"))
        .select(
            "ticket_id", "order_id", "order_product_id", "session_id",
            "ext_id", "category_id", "amount", "ticket_date",
        )
    )


def build_fact_buvette(spark, dim_matchs):
    """Grain = transaction buvette. session_id retrouvé par date (match_id
    de fb_transactions n'a pas d'équivalent direct dans sessions)."""
    fb = read_staging_csv(spark, "fb_transactions/fb_transactions.csv")
    matches = dim_matchs.select("session_id", "match_date")

    return (
        fb
        .withColumn("match_date", F.col("date_match").cast("date"))
        .join(matches, "match_date", "left")
        .select(
            "transaction_id", "session_id", "ext_id", "famille", "quantite",
            "prix_unitaire", "montant", "moyen_paiement", "match_date",
        )
    )


def build_fact_boutique(spark, fact_billetterie):
    """Grain = ligne de vente boutique. session_id rempli uniquement pour
    les clients n'ayant vu qu'un seul match (attribution non ambiguë) ;
    NULL pour les autres (voir discussion sur le fan-out multi-matchs)."""
    boutique = read_staging_csv(spark, "boutique_ventes_avoirs/boutique_ventes_avoirs.csv")

    client_match_counts = (
        fact_billetterie
        .where(F.col("ext_id").isNotNull())
        .groupBy("ext_id")
        .agg(
            F.countDistinct("session_id").alias("nb_matchs"),
            F.first("session_id").alias("unique_session_id"),
        )
    )
    unambiguous_clients = (
        client_match_counts
        .where(F.col("nb_matchs") == 1)
        .select("ext_id", F.col("unique_session_id").alias("session_id"))
    )

    vente_date = F.coalesce(
        F.to_date("VENTE_date", "dd/MM/yyyy"),
        F.to_date("VENTE_date", "dd-MM-yyyy"),
        F.to_date("VENTE_date", "yyyy-MM-dd"),
    )
    total = F.regexp_replace(F.col("LIGNE_Total"), ",", ".").cast("double")

    return (
        boutique
        .join(unambiguous_clients, "ext_id", "left")
        .select(
            F.col("LIGNE_ligne").alias("ligne_id"),
            F.col("VENTE_vente").alias("vente_id"),
            "ext_id", "session_id",
            F.col("LIGNE_Famille").alias("famille"),
            F.col("LIGNE_Designation").alias("designation"),
            F.col("LIGNE_Quantite").alias("quantite"),
            total.alias("total"),
            vente_date.alias("vente_date"),
        )
    )


def build_fact_commandes(spark):
    """Grain = charge (frais de livraison), niveau commande — pas
    attribuable à un match précis (une commande peut couvrir plusieurs
    matchs)."""
    charges = read_orders_table(spark, "orders/charges.csv", "charge_id")
    orders = read_orders_table(spark, "orders/orders.csv", "order_id")

    orders_ext = (
        orders.select(
            "order_id", "ext_id",
            F.substring("creation_date", 1, 10).cast("date").alias("order_date"),
        )
    )

    return (
        charges
        .join(orders_ext, "order_id", "left")
        .select("charge_id", "order_id", "ext_id", "delivery_mode_id", "amount", "order_date")
    )


def build_fact_paiements(spark):
    """Grain = paiement. Filtré sur is_cancelled=False : is_cancelled=True
    correspond à des tentatives de paiement échouées/dupliquées, pas à du
    revenu réel (vérifié sur les données : plusieurs True suivis d'un
    False au même montant, sur la même commande)."""
    payments = read_orders_table(spark, "orders/payments.csv", "payment_id")
    orders = read_orders_table(spark, "orders/orders.csv", "order_id")
    orders_ext = orders.select("order_id", "ext_id")

    return (
        payments
        .where(F.col("is_cancelled") == False)  # noqa: E712
        .join(orders_ext, "order_id", "left")
        .withColumn("payment_date", F.substring("date", 1, 10).cast("date"))
        .select("payment_id", "order_id", "ext_id", "payment_type_id", "amount", "payment_date")
    )


def write_parquet(df, table_name):
    """Écrit une table en un seul fichier Parquet (coalesce(1)) dans
    Data Warehouse/Rev Paris Basketball/<table_name>/."""
    path = str(WAREHOUSE / table_name)
    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info(f"{df.count():>6} lignes -> {table_name}/ (parquet)")


def main():
    spark = get_spark()

    dim_matchs = build_dim_matchs(spark)
    dim_client = build_dim_client(spark)
    dim_date = build_dim_date(spark)

    fact_billetterie = build_fact_billetterie(spark)
    fact_buvette = build_fact_buvette(spark, dim_matchs)
    fact_boutique = build_fact_boutique(spark, fact_billetterie)
    fact_commandes = build_fact_commandes(spark)
    fact_paiements = build_fact_paiements(spark)

    write_parquet(dim_matchs, "dim_matchs")
    write_parquet(dim_client, "dim_client")
    write_parquet(dim_date, "dim_date")
    write_parquet(fact_billetterie, "fact_billetterie")
    write_parquet(fact_buvette, "fact_buvette")
    write_parquet(fact_boutique, "fact_boutique")
    write_parquet(fact_commandes, "fact_commandes")
    write_parquet(fact_paiements, "fact_paiements")

    spark.stop()
    logger.info("transformation terminee : star schema ecrit dans Data Warehouse/Rev Paris Basketball/")


if __name__ == "__main__":
    main()
