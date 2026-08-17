"""Convert ORDERS_*.json billetterie exports into normalized CSV tables.

Reads one or more ORDERS_*.json files (order -> products -> tickets, plus
charges/payments), flattens nested objects, and writes:
    orders.csv, products.csv, tickets.csv, charges.csv, payments.csv

Usage:
    python convert_orders_to_csv.py <json_file1> [<json_file2> ...]
"""
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "pipeline"))
from logging_config import get_logger  # noqa: E402

logger = get_logger("convert_orders_to_csv")

OUTPUT_DIR = Path(__file__).parent

ORDER_FIELDS = [
    "source_file", "order_id", "event_id", "order_status_id", "channel_id",
    "language", "creation_date", "validation_date", "amend_date",
    "validation_user", "revision_user", "is_secondary_market",
    "has_related_orders", "related_order_ids",
    "customer_id", "customer_external_id",
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


def extract_orders_array(text):
    """Return the parsed list of orders, recovering from a truncated tail
    (e.g. a 'sessions' section cut off mid-write) by bracket-matching just
    the 'orders' array instead of parsing the whole document."""
    try:
        return json.loads(text)["orders"]
    except json.JSONDecodeError:
        pass

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
        raise ValueError("Could not recover an 'orders' array from truncated JSON")
    logger.warning("recovered truncated file: parsed 'orders' array only, ignored the rest")
    return json.loads(text[start:end + 1])


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
        "customer_external_id": customer.get("external_id"),
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


def write_csv(path, fieldnames, rows):
    try:
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"wrote {len(rows):>6} rows -> {path.name}")
    except OSError as e:
        logger.error(f"failed to write {path.name} : {e}")


def main(json_paths):
    all_orders, all_products, all_tickets, all_charges, all_payments = [], [], [], [], []

    for json_path in json_paths:
        path = Path(json_path)
        logger.info(f"Reading {path.name}...")
        try:
            text = path.read_text(encoding="utf-8")
            orders = extract_orders_array(text)
        except (OSError, ValueError, json.JSONDecodeError) as e:
            logger.error(f"skipped {path.name} : {e}")
            continue
        logger.info(f"{len(orders)} orders found in {path.name}")

        for order in orders:
            all_orders.append(flatten_order(order, path.name))
            all_products.extend(flatten_products(order, path.name))
            all_tickets.extend(flatten_tickets(order, path.name))
            all_charges.extend(flatten_charges(order, path.name))
            all_payments.extend(flatten_payments(order, path.name))

    logger.info("Writing CSV files...")
    write_csv(OUTPUT_DIR / "orders.csv", ORDER_FIELDS, all_orders)
    write_csv(OUTPUT_DIR / "products.csv", PRODUCT_FIELDS, all_products)
    write_csv(OUTPUT_DIR / "tickets.csv", TICKET_FIELDS, all_tickets)
    write_csv(OUTPUT_DIR / "charges.csv", CHARGE_FIELDS, all_charges)
    write_csv(OUTPUT_DIR / "payments.csv", PAYMENT_FIELDS, all_payments)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: python convert_orders_to_csv.py <json_file1> [<json_file2> ...]")
    main(sys.argv[1:])
