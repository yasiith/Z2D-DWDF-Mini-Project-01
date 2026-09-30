"""
Table metadata for the ABC Hub NiFi flow - the single source of truth for
what is extracted, how it is validated and how it is cleansed.

For every operational table the builder (build_flow.py) derives:
  * raw.schema         all-nullable Avro schema used to land BRONZE
  * avro.schema        validation schema: mandatory fields are NOT nullable,
                       so ValidateRecord routes records missing them to "invalid"
  * cleanse.sql        Calcite SQL run by QueryRecord -> standardised values,
                       business-rule fixes, dq_flags, in-batch de-duplication
  * duplicates.sql     Calcite SQL returning the in-batch duplicates (rejected)

Column spec syntax:  "<name>:<type>[!]"  where ! marks a mandatory field.
Types: long | int | double | string | date | ts | bool

Every rule below comes from sql/01_profiling/data_quality_profiling.sql.
"""

# Extraction groups: reference data changes rarely, transactions change often.
REFERENCE = "reference"
TRANSACTIONAL = "transactional"


def v(expr):
    """Calcite types CASE/literal strings as fixed-width CHAR and pads them;
    casting to VARCHAR keeps values exactly as written."""
    return f"CAST({expr} AS VARCHAR(500))"


def flag(condition, name):
    """One dq_flags fragment: 'NAME,' when the rule fired, '' otherwise.
    Both branches are cast individually - casting the CASE as a whole would
    keep the padding Calcite already added to the shorter literal."""
    return f"CASE WHEN {condition} THEN {v(repr(name + ','))} ELSE {v(repr(''))} END"


TABLES = [
    # ------------------------------------------------------------ reference
    dict(name="country", group=REFERENCE, pk=["country_id"],
         columns="country_id:long! country_name:string! country_code:string! created_at:ts! updated_at:ts!",
         transforms={"country_name": "TRIM(country_name)",
                     "country_code": "UPPER(TRIM(country_code))"}),
    dict(name="city", group=REFERENCE, pk=["city_id"],
         columns="city_id:long! country_id:long! city_name:string! created_at:ts! updated_at:ts!",
         transforms={"city_name": "TRIM(city_name)"}),
    dict(name="subscription_plan", group=REFERENCE, pk=["plan_id"],
         columns="plan_id:long! plan_name:string! monthly_fee:double! video_quality:string max_devices:int "
                 "created_at:ts! updated_at:ts!",
         transforms={"plan_name": "TRIM(plan_name)"}),
    dict(name="content_type", group=REFERENCE, pk=["content_type_id"],
         columns="content_type_id:long! content_type:string! created_at:ts! updated_at:ts!",
         transforms={"content_type": "TRIM(content_type)"}),
    dict(name="genre", group=REFERENCE, pk=["genre_id"],
         columns="genre_id:long! genre_name:string! created_at:ts! updated_at:ts!",
         transforms={"genre_name": "TRIM(genre_name)"}),
    dict(name="artist", group=REFERENCE, pk=["artist_id"],
         columns="artist_id:long! artist_name:string! country:string created_at:ts! updated_at:ts!",
         transforms={"artist_name": "TRIM(artist_name)", "country": "NULLIF(TRIM(country), '')"}),
    dict(name="warehouse", group=REFERENCE, pk=["warehouse_id"],
         columns="warehouse_id:long! city_id:long! warehouse_name:string! created_at:ts! updated_at:ts!",
         transforms={"warehouse_name": "TRIM(warehouse_name)"}),

    # ------------------------------------------------------ customer domain
    dict(name="customer", group=TRANSACTIONAL, pk=["customer_id"],
         columns="customer_id:long! customer_no:string! first_name:string last_name:string email:string! "
                 "phone:string date_of_birth:date gender:string registration_date:date! status:string! "
                 "created_at:ts! updated_at:ts!",
         transforms={
             "first_name": "TRIM(first_name)",
             "last_name": "TRIM(last_name)",
             "email": "LOWER(TRIM(email))",
             "phone": "NULLIF(TRIM(phone), '')",
             "gender": v("COALESCE(NULLIF(TRIM(gender), ''), 'Unknown')"),
             "status": "INITCAP(TRIM(status))",
         },
         flags=[("email <> LOWER(TRIM(email))", "EMAIL_NORMALISED"),
                ("status <> INITCAP(TRIM(status))", "STATUS_STANDARDISED"),
                ("gender IS NULL OR TRIM(gender) = ''", "GENDER_DEFAULTED")],
         # 8 customers re-registered with the same e-mail: keep the first account.
         dedupe=dict(partition="LOWER(TRIM(email))", order="customer_id")),
    dict(name="customer_address", group=TRANSACTIONAL, pk=["address_id"],
         columns="address_id:long! customer_id:long! city_id:long! address_line:string postal_code:string "
                 "address_type:string! created_at:ts! updated_at:ts!",
         transforms={"address_line": "TRIM(address_line)",
                     "postal_code": "NULLIF(TRIM(postal_code), '')",
                     "address_type": "INITCAP(TRIM(address_type))"},
         flags=[("address_type <> INITCAP(TRIM(address_type))", "ADDRESS_TYPE_STANDARDISED")]),
    dict(name="customer_subscription", group=TRANSACTIONAL, pk=["subscription_id"],
         columns="subscription_id:long! customer_id:long! plan_id:long! start_date:date! end_date:date "
                 "status:string! auto_renew:bool created_at:ts! updated_at:ts!",
         transforms={"status": "INITCAP(TRIM(status))"},
         flags=[("end_date < start_date", "END_BEFORE_START")]),

    # ------------------------------------------------------- content domain
    dict(name="content", group=TRANSACTIONAL, pk=["content_id"],
         columns="content_id:long! content_type_id:long! title:string! release_date:date duration_minutes:int "
                 "language:string age_rating:string created_at:ts! updated_at:ts!",
         # "language" is a reserved word in Calcite SQL, so it is always quoted.
         transforms={"title": "TRIM(title)",
                     "language": v("""COALESCE(NULLIF(TRIM("language"), ''), 'Unknown')"""),
                     "age_rating": "UPPER(TRIM(age_rating))"},
         flags=[(""""language" IS NULL OR TRIM("language") = ''""", "LANGUAGE_DEFAULTED")]),
    dict(name="content_genre", group=TRANSACTIONAL, pk=["content_id", "genre_id"],
         columns="content_id:long! genre_id:long! created_at:ts! updated_at:ts!"),
    dict(name="content_artist", group=TRANSACTIONAL, pk=["content_id", "artist_id"],
         columns="content_id:long! artist_id:long! role:string created_at:ts! updated_at:ts!",
         transforms={"role": "TRIM(role)"}),

    # ---------------------------------------------------- physical rentals
    dict(name="inventory_item", group=TRANSACTIONAL, pk=["inventory_id"],
         columns="inventory_id:long! content_id:long! warehouse_id:long! barcode:string purchase_date:date "
                 "item_condition:string status:string! created_at:ts! updated_at:ts!",
         transforms={"barcode": "TRIM(barcode)",
                     "item_condition": "INITCAP(TRIM(item_condition))",
                     "status": "INITCAP(TRIM(status))"}),
    dict(name="rental", group=TRANSACTIONAL, pk=["rental_id"],
         columns="rental_id:long! customer_id:long! inventory_id:long! rental_date:date! due_date:date! "
                 "return_date:date rental_fee:double! late_fee:double status:string! created_at:ts! updated_at:ts!",
         transforms={
             "status": "INITCAP(TRIM(status))",
             # Profiling: late / overdue rentals were billed ABS(late_fee);
             # on-time rentals were billed nothing.
             "late_fee": "CASE WHEN late_fee < 0 THEN "
                         "CASE WHEN return_date IS NULL OR return_date > due_date THEN ABS(late_fee) ELSE 0.0 END "
                         "ELSE late_fee END",
         },
         flags=[("status <> INITCAP(TRIM(status))", "STATUS_STANDARDISED"),
                ("late_fee < 0 AND (return_date IS NULL OR return_date > due_date)", "LATE_FEE_SIGN_FIXED"),
                ("late_fee < 0 AND return_date <= due_date", "LATE_FEE_ZEROED")]),

    # ------------------------------------------------------------ streaming
    dict(name="streaming_session", group=TRANSACTIONAL, pk=["stream_id"],
         columns="stream_id:long! customer_id:long! content_id:long! device_id:long! start_time:ts! end_time:ts! "
                 "watch_duration:int completion_percentage:double created_at:ts! updated_at:ts!",
         transforms={
             # end_time - start_time equals watch_duration on every populated row.
             "watch_duration": "COALESCE(watch_duration, "
                               "CAST(TIMESTAMPDIFF(MINUTE, start_time, end_time) AS INTEGER))",
             "completion_percentage": "CASE WHEN completion_percentage > 100 THEN 100.0 "
                                      "WHEN completion_percentage < 0 THEN 0.0 ELSE completion_percentage END",
         },
         flags=[("watch_duration IS NULL", "WATCH_DURATION_DERIVED"),
                ("completion_percentage > 100", "COMPLETION_CAPPED"),
                ("completion_percentage < 0", "COMPLETION_FLOORED")],
         dedupe=dict(partition="customer_id, content_id, device_id, start_time", order="stream_id")),

    # ------------------------------------------------------------- payments
    dict(name="payment", group=TRANSACTIONAL, pk=["payment_id"],
         columns="payment_id:long! customer_id:long! rental_id:long subscription_id:long payment_method_id:long! "
                 "amount:double! payment_date:ts! payment_type:string! status:string! created_at:ts! updated_at:ts!",
         transforms={"amount": "ABS(amount)",
                     "payment_type": "INITCAP(TRIM(payment_type))",
                     "status": "INITCAP(TRIM(status))"},
         flags=[("amount < 0", "AMOUNT_SIGN_FIXED")],
         dedupe=dict(partition="customer_id, payment_type, amount, payment_date, rental_id, subscription_id",
                     order="payment_id")),

    # --------------------------------------------------- engagement/support
    dict(name="review", group=TRANSACTIONAL, pk=["review_id"],
         columns="review_id:long! customer_id:long! content_id:long! rating:int review_text:string "
                 "review_date:ts! created_at:ts! updated_at:ts!",
         transforms={"rating": "CASE WHEN rating BETWEEN 1 AND 5 THEN rating ELSE CAST(NULL AS INTEGER) END",
                     "review_text": "NULLIF(TRIM(review_text), '')"},
         flags=[("rating IS NULL OR rating NOT BETWEEN 1 AND 5", "RATING_INVALID")]),
    dict(name="wishlist", group=TRANSACTIONAL, pk=["wishlist_id"],
         columns="wishlist_id:long! customer_id:long! content_id:long! added_date:ts! created_at:ts! updated_at:ts!"),
    dict(name="support_ticket", group=TRANSACTIONAL, pk=["ticket_id"],
         columns="ticket_id:long! customer_id:long! category_id:long! opened_date:ts! closed_date:ts "
                 "priority:string status:string! created_at:ts! updated_at:ts!",
         transforms={"priority": "INITCAP(TRIM(priority))",
                     "status": "INITCAP(TRIM(status))",
                     "closed_date": "CASE WHEN closed_date < opened_date THEN CAST(NULL AS TIMESTAMP) "
                                    "ELSE closed_date END"},
         flags=[("priority <> INITCAP(TRIM(priority))", "PRIORITY_STANDARDISED"),
                ("closed_date < opened_date", "CLOSED_BEFORE_OPENED")]),
]

AVRO_TYPES = {
    "long": "long", "int": "int", "double": "double", "string": "string", "bool": "boolean",
    "date": {"type": "int", "logicalType": "date"},
    "ts": {"type": "long", "logicalType": "timestamp-millis"},
}


def parse_columns(spec):
    cols = []
    for token in spec.split():
        name, typ = token.split(":")
        mandatory = typ.endswith("!")
        cols.append((name, typ.rstrip("!"), mandatory))
    return cols


def avro_schema(table, nullable_all=False):
    fields = []
    for name, typ, mandatory in parse_columns(table["columns"]):
        t = AVRO_TYPES[typ]
        fields.append({"name": name, "type": t if (mandatory and not nullable_all) else ["null", t]})
    suffix = "_raw" if nullable_all else ""
    return {"type": "record", "name": table["name"] + suffix, "namespace": "abc_hub", "fields": fields}


def _numbered(table):
    d = table["dedupe"]
    return (f"SELECT f.*, ROW_NUMBER() OVER (PARTITION BY {d['partition']} ORDER BY {d['order']}) AS dedupe_rank "
            f"FROM FLOWFILE f")


def cleanse_sql(table):
    # Output columns are double-quoted so reserved words (e.g. language) are safe.
    exprs = []
    for name, _, _ in parse_columns(table["columns"]):
        expr = table.get("transforms", {}).get(name, f'"{name}"')
        exprs.append(f'{expr} AS "{name}"')
    flags = table.get("flags", [])
    if flags:
        joined = " || ".join(flag(c, n) for c, n in flags)
        exprs.append(f"NULLIF(TRIM(TRAILING ',' FROM {joined}), '') AS \"dq_flags\"")
    else:
        exprs.append('CAST(NULL AS VARCHAR(500)) AS "dq_flags"')
    # Evaluated by QueryRecord at run time (see the $$ escaping in build_flow.py).
    exprs.append("'${batch.id}' AS \"_batch_id\"")
    source = f"({_numbered(table)}) numbered WHERE dedupe_rank = 1" if table.get("dedupe") else "FLOWFILE"
    return "SELECT " + ", ".join(exprs) + " FROM " + source


def duplicates_sql(table):
    if not table.get("dedupe"):
        return "SELECT * FROM FLOWFILE WHERE 1 = 0"
    return f"SELECT * FROM ({_numbered(table)}) numbered WHERE dedupe_rank > 1"
