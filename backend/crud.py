
import json
import re

from sqlalchemy import or_
from sqlalchemy.orm import joinedload

from models import Material, Smeta, SmetaAccess, SmetaItem, SmetaRevision


SEARCH_SYNONYMS = {
    "ip": ["ip", "ип", "шз", "айпи"],
    "ип": ["ip", "ип", "шз", "айпи"],
    "шз": ["ip", "ип", "шз", "айпи"],
    "mp": ["mp", "мп", "vg", "мегапикс", "мегапиксель"],
    "мп": ["mp", "мп", "vg", "мегапикс", "мегапиксель"],
    "vg": ["mp", "мп", "vg", "мегапикс", "мегапиксель"],
    "камера": [
        "камера",
        "камеры",
        "камеру",
        "камерой",
        "видеокамера",
        "видеокамеры",
        "видеокамеру",
        "видеокамер",
    ],
    "видеокамера": [
        "камера",
        "камеры",
        "камеру",
        "камерой",
        "видеокамера",
        "видеокамеры",
        "видеокамеру",
        "видеокамер",
    ],
    "регистратор": [
        "регистратор",
        "регистратора",
        "регистраторы",
        "видеорегистратор",
        "видеорегистратора",
        "nvr",
        "dvr",
    ],
    "видеорегистратор": [
        "регистратор",
        "регистратора",
        "регистраторы",
        "видеорегистратор",
        "видеорегистратора",
        "nvr",
        "dvr",
    ],
    "видеосервер": ["видеосервер", "видеосервера", "сервер"],
    "жд": ["жд", "hdd", "диск", "жесткий", "жёсткий"],
    "hdd": ["жд", "hdd", "диск", "жесткий", "жёсткий"],
    "ворота": ["ворота", "воротами", "автоматика", "привод"],
    "скуд": ["скуд", "контроллер", "с2000"],
    "ибп": ["ибп", "источник", "источник питания", "блок питания", "бесперебойного питания"],
    "источник": ["ибп", "источник", "источник питания", "блок питания", "бесперебойного питания"],
    "считыватель": ["считыватель", "считывателя", "reader"],
    "кнопка": ["кнопка", "выход", "no", "nc"],
    "замок": ["замок", "электромагнитный"],
    "смк": ["смк", "магнитоконтактный"],
    "кронштейн": ["кронштейн", "кронштейна"],
    "монтаж": ["монтаж", "монтажа", "установка", "установки"],
    "установка": ["установка", "установки", "монтаж", "монтажа"],
}

for camera_alias in ["камер", "камеры", "камеру", "камерой"]:
    SEARCH_SYNONYMS[camera_alias] = SEARCH_SYNONYMS["камера"]

for recorder_alias in ["регистратора", "регистраторы", "nvr", "dvr"]:
    SEARCH_SYNONYMS[recorder_alias] = SEARCH_SYNONYMS["регистратор"]


RU_TO_EN_LAYOUT = str.maketrans(
    "ёйцукенгшщзхъфывапролджэячсмитьбю",
    "`qwertyuiop[]asdfghjkl;'zxcvbnm,.",
)
EN_TO_RU_LAYOUT = str.maketrans(
    "`qwertyuiop[]asdfghjkl;'zxcvbnm,.",
    "ёйцукенгшщзхъфывапролджэячсмитьбю",
)


def normalize_search_text(value):
    value = (value or "").lower().replace("ё", "е")
    return re.sub(r"[^0-9a-zа-я]+", " ", value).strip()


def search_tokens(value):
    return [token for token in normalize_search_text(value).split() if token]


def keyboard_layout_variants(token):
    variants = {token}
    variants.add(token.translate(RU_TO_EN_LAYOUT))
    variants.add(token.translate(EN_TO_RU_LAYOUT))
    return {normalize_search_text(variant) for variant in variants if normalize_search_text(variant)}


def megapixel_variants(token):
    match = re.fullmatch(r"(\d+)(?:[.,]\d+)?(mp|мп|vg)", token)
    if not match:
        return set()
    number = match.group(1)
    return {
        f"{number}mp",
        f"{number}мп",
        f"{number}vg",
        f"{number} mp",
        f"{number} мп",
        f"{number} vg",
    }


def expanded_query_groups(q):
    groups = []
    for token in search_tokens(q):
        base_variants = set()
        for token_variant in keyboard_layout_variants(token):
            base_variants.update(SEARCH_SYNONYMS.get(token_variant, [token_variant]))
            base_variants.update(megapixel_variants(token_variant))
        normalized_variants = []
        for variant in base_variants or [token]:
            normalized_variants.extend(search_tokens(variant))
            normalized_variants.append(normalize_search_text(variant))
        groups.append(sorted({variant for variant in normalized_variants if variant}))
    return groups


def material_search_text(material):
    return normalize_search_text(
        f"{material.name or ''} {material.characteristics or ''} {material.source or ''}"
    )


def material_field_texts(material):
    return {
        "name": normalize_search_text(material.name),
        "characteristics": normalize_search_text(material.characteristics),
        "source": normalize_search_text(material.source),
    }


def contains_term(text_value, term):
    if not term:
        return True
    return term in text_value


def matches_query_groups(material, groups):
    text_value = material_search_text(material)
    return all(any(contains_term(text_value, term) for term in group) for group in groups)


def relevance_score(material, q, groups):
    fields = material_field_texts(material)
    query_text = normalize_search_text(q)
    score = 0
    if fields["name"] == query_text:
        score += 500
    if fields["name"].startswith(query_text):
        score += 250
    if query_text and query_text in fields["name"]:
        score += 180
    for group in groups:
        if any(fields["name"].startswith(term) for term in group):
            score += 90
        if any(contains_term(fields["name"], term) for term in group):
            score += 60
        if any(contains_term(fields["characteristics"], term) for term in group):
            score += 18
        if any(contains_term(fields["source"], term) for term in group):
            score += 8
    return score


BROAD_QUERY_TERMS = {
    "ip",
    "ип",
    "шз",
    "камера",
    "видеокамера",
    "регистратор",
    "видеорегистратор",
    "nvr",
    "dvr",
    "скуд",
    "считыватель",
    "замок",
    "турникет",
    "контроллер",
    "ибп",
    "жд",
    "hdd",
    "коммутатор",
    "кабель",
    "монтаж",
    "пусконаладка",
    "видеонаблюдение",
    "оборудование",
    "материал",
    "материалы",
}


def query_is_broad(q, groups):
    tokens = search_tokens(q)
    if not tokens:
        return False
    informative_tokens = [token for token in tokens if token not in BROAD_QUERY_TERMS and not token.isdigit()]
    return len(informative_tokens) == 0 or (len(tokens) <= 2 and len(informative_tokens) <= 1)


def like_variants(term):
    variants = {term, term.upper(), term.capitalize()}
    if term.startswith("видео"):
        variants.add(term.replace("видео", ""))
    return [f"%{variant}%" for variant in variants if variant]


def get_materials(db, q="", item_type="", limit=200, offset=0, return_total=False):
    query = db.query(Material)
    if q:
        groups = expanded_query_groups(q)
        terms = sorted({term for group in groups for term in group})
        clauses = []
        for term in terms:
            for pattern in like_variants(term):
                clauses.extend(
                    [
                        Material.name.like(pattern),
                        Material.characteristics.like(pattern),
                        Material.source.like(pattern),
                    ]
                )
        if clauses:
            query = query.filter(or_(*clauses))
    if item_type and item_type != "all":
        query = query.filter(Material.item_type == item_type)
    offset = max(0, int(offset or 0))
    limit = max(1, int(limit or 200))
    if not q:
        total = query.count()
        rows = query.order_by(Material.name).offset(offset).limit(limit).all()
        if return_total:
            return rows, total
        return rows
    rows = query.order_by(Material.name).limit(max(limit * 25, 5000)).all()
    if q:
        groups = expanded_query_groups(q)
        filtered_rows = [row for row in rows if matches_query_groups(row, groups)]
        if not filtered_rows and item_type and item_type != "all":
            fallback_query = db.query(Material).filter(Material.item_type == item_type)
            fallback_rows = fallback_query.order_by(Material.name).limit(20000).all()
            filtered_rows = [row for row in fallback_rows if matches_query_groups(row, groups)]
        broad_query = query_is_broad(q, groups)
        rows = sorted(
            filtered_rows,
            key=lambda material: (
                -relevance_score(material, q, groups),
                float(material.price or 0) if broad_query else 0,
                (material.name or "").lower(),
            ),
        )
    total = len(rows)
    page = rows[offset : offset + limit]
    if return_total:
        return page, total
    return page


def filter_materials(rows, technology="", megapixels="", price_to=None):
    technology = normalize_search_text(technology)
    megapixels = normalize_search_text(megapixels)

    def has_technology(material):
        if not technology:
            return True
        text_value = normalize_search_text(f"{material.name or ''} {material.source or ''}")
        tokens = set(text_value.split())
        if technology == "ip":
            return "ip" in tokens or "ип" in tokens
        if technology == "ahd":
            return "ahd" in tokens
        if technology == "poe":
            return "poe" in tokens or "рое" in tokens
        return technology in tokens

    def has_megapixels(material):
        if not megapixels:
            return True
        text_value = material_search_text(material)
        number = re.sub(r"\D", "", megapixels)
        if not number:
            return True
        variants = [
            f"{number} мп",
            f"{number}мп",
            f"{number} mp",
            f"{number}mp",
            f"{number} мегапикс",
            f"{number} мегапиксель",
        ]
        return any(variant in text_value for variant in variants)

    def within_price(material):
        if price_to is None:
            return True
        return float(material.price or 0) <= float(price_to)

    return [
        material
        for material in rows
        if has_technology(material) and has_megapixels(material) and within_price(material)
    ]


def get_material(db, material_id):
    return db.query(Material).filter(Material.id == material_id).first()


def create_material(db, name, unit, price, source, characteristics="", item_type="equipment"):
    material = Material(
        item_type=item_type,
        name=name,
        characteristics=characteristics,
        unit=unit,
        price=price,
        source=source,
    )
    db.add(material)
    db.commit()
    db.refresh(material)
    return material


def create_smeta(db, name, data=None):
    data = data or {}
    smeta = Smeta(name=name, **{key: value for key, value in data.items() if hasattr(Smeta, key)})
    db.add(smeta)
    db.commit()
    db.refresh(smeta)
    return smeta


def smeta_snapshot_payload(smeta):
    return {
        "name": smeta.name,
        "parent_id": smeta.parent_id,
        "owner_id": smeta.owner_id,
        "customer_name": smeta.customer_name or "",
        "customer_details": smeta.customer_details or "",
        "contractor_name": smeta.contractor_name or "",
        "contractor_details": smeta.contractor_details or "",
        "approver_name": smeta.approver_name or "",
        "approver_details": smeta.approver_details or "",
        "tax_mode": smeta.tax_mode or "none",
        "tax_rate": smeta.tax_rate or 0,
        "section_adjustments": smeta.section_adjustments or "{}",
        "items": [
            {
                "item_type": item.item_type or "material",
                "section": item.section or "Оборудование",
                "name": item.name,
                "characteristics": item.characteristics or "",
                "unit": item.unit or "",
                "quantity": item.quantity or 1,
                "unit_price": item.unit_price or 0,
                "source": item.source or "",
            }
            for item in smeta.items
        ],
    }


def create_smeta_revision(db, smeta, label=""):
    revision = SmetaRevision(
        smeta_id=smeta.id,
        label=label or "",
        payload=json.dumps(smeta_snapshot_payload(smeta), ensure_ascii=False),
    )
    db.add(revision)
    db.commit()
    db.refresh(revision)
    return revision


def get_smeta_revisions(db, smeta_id, limit=30):
    return (
        db.query(SmetaRevision)
        .filter(SmetaRevision.smeta_id == smeta_id)
        .order_by(SmetaRevision.id.desc())
        .limit(limit)
        .all()
    )


def restore_smeta_revision(db, smeta_id, revision_id):
    revision = (
        db.query(SmetaRevision)
        .filter(SmetaRevision.smeta_id == smeta_id, SmetaRevision.id == revision_id)
        .first()
    )
    if not revision:
        return None
    smeta = get_smeta(db, smeta_id)
    if not smeta:
        return None
    try:
        payload = json.loads(revision.payload or "{}")
    except json.JSONDecodeError:
        return None

    for key in [
        "name",
        "parent_id",
        "owner_id",
        "customer_name",
        "customer_details",
        "contractor_name",
        "contractor_details",
        "approver_name",
        "approver_details",
        "tax_mode",
        "tax_rate",
        "section_adjustments",
    ]:
        if key in payload:
            setattr(smeta, key, payload.get(key))

    db.query(SmetaItem).filter(SmetaItem.smeta_id == smeta_id).delete(synchronize_session=False)
    for item_data in payload.get("items", []):
        db.add(
            SmetaItem(
                smeta_id=smeta_id,
                item_type=item_data.get("item_type", "material"),
                section=item_data.get("section", "Оборудование"),
                name=item_data.get("name", ""),
                characteristics=item_data.get("characteristics", ""),
                unit=item_data.get("unit", ""),
                quantity=item_data.get("quantity", 1),
                unit_price=item_data.get("unit_price", 0),
                base_unit_price=item_data.get("base_unit_price", item_data.get("unit_price", 0)),
                source=item_data.get("source", ""),
            )
        )
    db.commit()
    db.refresh(smeta)
    return smeta


def update_smeta(db, smeta_id, data):
    smeta = get_smeta(db, smeta_id)
    if not smeta:
        return None
    for key, value in data.items():
        if hasattr(smeta, key) and value is not None:
            setattr(smeta, key, value)
    db.commit()
    db.refresh(smeta)
    return smeta


def get_smetas(db):
    return db.query(Smeta).options(joinedload(Smeta.items)).order_by(Smeta.id.desc()).all()


def get_visible_smetas(db, user):
    if getattr(user, "is_admin", 0):
        return get_smetas(db)
    access_ids = [
        row[0]
        for row in db.query(SmetaAccess.smeta_id)
        .filter(SmetaAccess.user_id == user.id)
        .all()
    ]
    query = db.query(Smeta).options(joinedload(Smeta.items)).filter(
        or_(Smeta.owner_id == user.id, Smeta.owner_id == str(user.id), Smeta.id.in_(access_ids or [-1]))
    )
    return query.order_by(Smeta.id.desc()).all()


def get_smeta(db, smeta_id):
    return (
        db.query(Smeta)
        .options(joinedload(Smeta.items))
        .filter(Smeta.id == smeta_id)
        .first()
    )


def delete_smeta(db, smeta_id):
    smeta = get_smeta(db, smeta_id)
    if not smeta:
        return False
    db.delete(smeta)
    db.commit()
    return True


def clone_smeta(db, smeta_id, name=None):
    source = get_smeta(db, smeta_id)
    if not source:
        return None
    clone = Smeta(
        parent_id=source.id,
        name=name or f"{source.name} - вариант",
        customer_name=source.customer_name or "",
        customer_details=source.customer_details or "",
        contractor_name=source.contractor_name or "",
        contractor_details=source.contractor_details or "",
        approver_name=source.approver_name or "",
        approver_details=source.approver_details or "",
        tax_mode=source.tax_mode or "none",
        tax_rate=source.tax_rate or 0,
        section_adjustments=source.section_adjustments or "{}",
    )
    db.add(clone)
    db.flush()
    for item in source.items:
        db.add(
            SmetaItem(
                smeta_id=clone.id,
                item_type=item.item_type or "material",
                section=item.section or "Оборудование",
                name=item.name,
                characteristics=item.characteristics or "",
                unit=item.unit or "",
                quantity=item.quantity or 1,
                unit_price=item.unit_price or 0,
                base_unit_price=item.base_unit_price if getattr(item, "base_unit_price", None) is not None else (item.unit_price or 0),
                source=item.source or "",
            )
        )
    db.commit()
    db.refresh(clone)
    return clone


def add_smeta_item(db, smeta_id, item_data):
    normalized_name = normalize_search_text(item_data.get("name"))
    normalized_unit = normalize_search_text(item_data.get("unit"))
    existing_items = db.query(SmetaItem).filter(SmetaItem.smeta_id == smeta_id).all()
    for existing in existing_items:
        same_position = (
            normalize_search_text(existing.name) == normalized_name
            and normalize_search_text(existing.unit) == normalized_unit
            and (existing.section or "") == (item_data.get("section") or "")
            and (existing.item_type or "") == (item_data.get("item_type") or "")
            and float(existing.unit_price or 0) == float(item_data.get("unit_price") or 0)
        )
        if same_position:
            existing.quantity = (existing.quantity or 0) + (item_data.get("quantity") or 1)
            if item_data.get("characteristics") and not existing.characteristics:
                existing.characteristics = item_data["characteristics"]
            if item_data.get("source") and not existing.source:
                existing.source = item_data["source"]
            if getattr(existing, "base_unit_price", None) is None:
                existing.base_unit_price = float(item_data.get("base_unit_price") or item_data.get("unit_price") or 0)
            db.commit()
            db.refresh(existing)
            return existing
    if "base_unit_price" not in item_data or item_data.get("base_unit_price") is None:
        item_data = {**item_data, "base_unit_price": float(item_data.get("unit_price") or 0)}
    item = SmetaItem(smeta_id=smeta_id, **item_data)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


def delete_smeta_item(db, smeta_id, item_id):
    item = (
        db.query(SmetaItem)
        .filter(SmetaItem.smeta_id == smeta_id, SmetaItem.id == item_id)
        .first()
    )
    if not item:
        return False
    db.delete(item)
    db.commit()
    return True


def update_smeta_item(db, smeta_id, item_id, data):
    item = (
        db.query(SmetaItem)
        .filter(SmetaItem.smeta_id == smeta_id, SmetaItem.id == item_id)
        .first()
    )
    if not item:
        return None
    for key, value in data.items():
        if hasattr(item, key) and value is not None:
            setattr(item, key, value)
    db.commit()
    db.refresh(item)
    return item
