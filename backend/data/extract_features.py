"""Extract structured, normalised features from raw supermarket product names.

Turns the free-text `products.name` written by each scraper into the fields the
deterministic grouper needs:

    manufacturer_id, product_name, quantity_value, quantity_unit,
    quantity_raw, is_fluid, metadata

Design notes (see AGENTS.md / .opencode/todo.md):

- The LLM only *extracts pieces* (value as written, unit as written, pack count).
  All canonicalisation — unit mapping, multipack multiplication, g<->ml for
  fluids, kg/l promotion, rounding — happens deterministically in Python below,
  so the rules are testable and don't drift with the model.
- product_name keeps every distinguishing trait (fat %, flavour, variant) but
  loses the manufacturer, the quantity token and shop noise.
- Fluids (pourable liquids only) are stored in ml/l; solids stay in g/kg.

Usage:
    python -m backend.data.extract_features --dry-run --sample 5
    python -m backend.data.extract_features --limit 50
    python -m backend.data.extract_features --ids <uuid>,<uuid> --dry-run
    python -m backend.data.extract_features --reprocess
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import List, Optional

import psycopg2
from dotenv import find_dotenv, load_dotenv
from google import genai
from psycopg2.extras import RealDictCursor, execute_values
from pydantic import BaseModel, Field

from backend.data.RateLimiter import (
    TEXT_MODEL_RPM_LIMIT,
    TEXT_MODEL_SCOPE,
    TEXT_MODEL_TPM_LIMIT,
    call_with_retries,
    get_rate_limiter,
)
from backend.data.db_utils import connect_to_db
from backend.data.text_utils import normalize_string

FEATURES_VERSION = 1
DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite")
DEFAULT_BATCH_SIZE = 20
DEFAULT_CONCURRENCY = 128
RETRY_ATTEMPTS = 5
# "Разно" (catch-all main category) and the "Останато" subcategory are never
# groupable and together cover ~21% of the table; extracting features for them
# only burns tokens.
SKIP_MAIN_CATEGORIES = ("Разно",)
SKIP_SUB_CATEGORIES = ("Останато",)

# Same scope/limits as categorize_products.py: the process-wide limiter and its
# persisted 60 s window are shared, so this stage does not burst the quota the
# categorizer just spent.
rate_limiter = get_rate_limiter(
    TEXT_MODEL_RPM_LIMIT, TEXT_MODEL_TPM_LIMIT, scope=TEXT_MODEL_SCOPE
)


# --------------------------------------------------------------------------
# LLM response contract
# --------------------------------------------------------------------------
class MetadataEntry(BaseModel):
    """One leftover key/value pair (Gemini Developer API rejects free-form dicts)."""

    key: str = Field(description="Short machine-friendly key, e.g. packaging, promo, code")
    value: str = Field(description="Value as written or lightly normalised")


class ProductFeatures(BaseModel):
    """Features extracted from one raw product name."""

    index: int = Field(description="1-based position of the product in the input list")
    manufacturer: Optional[str] = Field(
        default=None,
        description="Brand / producer exactly as written, or null when the name has none",
    )
    product_name: str = Field(
        description="Product description WITHOUT the manufacturer and WITHOUT quantity or pack tokens"
    )
    quantity_value: Optional[float] = Field(
        default=None,
        description="Size of ONE unit as written (no pack multiplication). null when absent",
    )
    quantity_unit: Optional[str] = Field(
        default=None,
        description="Unit as written: g, gr, kg, ml, l, pcs. null when absent",
    )
    pack_count: Optional[int] = Field(
        default=1,
        description="Units in the package: 6x2 -> 6 with value 2; 2+1 -> 3. 1 when not a multipack",
    )
    is_fluid: bool = Field(
        default=False,
        description="True only for pourable liquids (milk, water, juice, oil, vinegar, liquid detergent, shampoo)",
    )
    metadata: Optional[List[MetadataEntry]] = Field(
        default=None,
        description="Everything that is neither manufacturer, product name nor quantity: promo text, packaging, codes",
    )
    confidence: float = Field(description="Confidence 0.0-1.0", ge=0.0, le=1.0)


class BatchFeatures(BaseModel):
    products: List[ProductFeatures]


PROMPT = """You extract structured product data from Macedonian supermarket product names.
The names come from 7 different shops and are inconsistent in casing, spacing, word order,
Latin/Cyrillic mixing, abbreviations and duplicated fragments.

For EACH product, return:
- index: the 1-based position of the product in the input list (must match input order)
- manufacturer: the brand or producer as written, or null when the name has none.
  Do NOT include legal forms (АД, ДОО, ДООЕЛ, ЕМ).
- product_name: the product description with the manufacturer REMOVED and with all quantity,
  pack-size and multipack tokens REMOVED. Keep everything that identifies the product:
  fat percentage, flavour, variant words (light, без лактоза, интегрален...).
  Do not translate, do not reorder into marketing text, just clean it.
- quantity_value: the size of ONE unit as a number (no pack multiplication).
  "1,5" -> 1.5. Never a percentage, never a dimension like cm.
- quantity_unit: the unit as written, normalised to one of: g, gr, kg, ml, l, pcs.
  "гр"/"г"/"g" -> g; "кг"/"kg" -> kg; "мл"/"ml" -> ml; "л"/"l" -> l;
  "ком"/"пар"/"парчиња"/"x" with no size -> pcs. null when the name has no quantity.
- pack_count: how many units the package contains. "6x2" -> 6 with value 2; "2+1" -> 3;
  "3x330ml" -> 3 with value 330 and unit ml. Plain single items -> 1.
- is_fluid: true ONLY for pourable liquids: milk, water, juice, oil, vinegar, liquid
  detergent, shampoo, soft drinks. FALSE for semi-solids and solids even when sold by
  weight: yogurt, sour cream, ice cream, butter, cheese, honey, marmalade, dough.
- metadata: a small object for everything that is neither manufacturer, product name nor
  quantity — promotion text ("акција", "подарок", "2+1" wording), packaging ("стакло",
  "лименка", "тетрапак"), internal shop codes. Omit keys you don't need.
- confidence: 0.9-1.0 when the name is unambiguous, 0.7-0.89 minor guessing,
  below 0.7 when quantity or manufacturer is unclear.

RULES
1. Never invent a quantity. If none is written, quantity_value/quantity_unit are null.
2. A percentage ("2.8%", "3,2 % масност") is fat content, NOT a quantity.
3. Ignore dimensions (20см, 30м) — they belong in metadata, not quantity.
4. Strip duplicated fragments some shops append ("NAME - NAME"), keep one.
5. product_name must never contain the manufacturer or a quantity token.
6. Return exactly one entry per input product, in input order.
7. Packaging and material words (вакум, ПЕТ, стакло, лименка, тетрапак, пластика, картон)   are NOT part of product_name — put them in metadata and remove them from the name.
8. Physical dimensions (30X45СМ, 175/165/55) are NOT a quantity: metadata only, null quantity.
9. Dry goods sold by weight are NOT fluids: tea (Чај), coffee, cocoa, flour, sugar, salt, spices,
   rice, pasta, pudding powder, washing powder. "ЧАЈ 30Г" -> quantity 30 g with is_fluid FALSE.
   Only the liquid the tea makes is a drink, not the product in the box.
10. Multi-word brands are ONE manufacturer: "БИБИ И БОБИ ОВОШНИ ТОПЧИЊА" ->
    manufacturer "Биби и Боби" and product_name "овошни топчиња". Never leave part of the
    brand ("и боби") inside product_name.

EXAMPLES
1) "Сок Schweppes 1л Bitter Lemon"
   -> manufacturer "Schweppes", product_name "сок bitter lemon", quantity_value 1, quantity_unit l,
      pack_count 1, is_fluid true
2) "ВОДА ПЕЛИСТЕРКА 2.5л ОБИЧНА"
   -> manufacturer null, product_name "вода пелистерка обична", value 2.5, unit l, is_fluid true
3) "КОКА КОЛА 6x330ml"
   -> manufacturer "Кока Кола", product_name "", value 330, unit ml, pack_count 6, is_fluid true
4) "КИСЕЛО МЛЕКО БИТОЛСКО 400гр 2.8%"
   -> manufacturer "Битолско", product_name "кисело млеко 2.8%", value 400, unit g,
      pack_count 1, is_fluid false (yogurt is not pourable)
5) "ДЕТЕРГЕНТ ЗА САДОВИ 1кг"
   -> manufacturer null, product_name "детергент за садови", value 1, unit kg, is_fluid true
6) "ЊОКИ РЕГИА 500ГР - REGGIA ЊОКИ ОД КОМПИР 70% КОМПИР"
   -> manufacturer "Региа", product_name "њоки од компир 70% компир", value 500, unit g,
      pack_count 1, is_fluid false
"""


# --------------------------------------------------------------------------
# Deterministic normalisation
# --------------------------------------------------------------------------
_UNIT_MAP = {
    "g": "g", "gr": "g", "grs": "g", "gram": "g", "grams": "g",
    "г": "g", "гр": "g", "грам": "g", "грама": "g",
    "kg": "kg", "kgs": "kg", "килограм": "kg", "килограми": "kg", "кг": "kg",
    "ml": "ml", "mls": "ml", "мл": "ml", "милилитар": "ml", "милилитри": "ml",
    "l": "l", "lt": "l", "ltr": "l", "литар": "l", "литри": "l", "л": "l",
    "pcs": "pcs", "pc": "pcs", "piece": "pcs", "pieces": "pcs",
    "ком": "pcs", "ком.": "pcs", "пар": "pcs", "парчиња": "pcs", "ед": "pcs",
}

_LEGAL_SUFFIX_RE = re.compile(r"\b(?:а\.?\s?д\.?|дооел|доо|дпт|еоод|ем)\b", re.IGNORECASE)

# multipack ("6x2", "3 x 330ml") and promotion ("2+1") expressions
_MULTIPACK_RE = re.compile(r"\b(\d{1,3})\s*[xх×]\s*(\d+(?:[.,]\d+)?)", re.IGNORECASE)
_PROMO_SUM_RE = re.compile(r"\b(\d{1,3})\s*\+\s*(\d{1,3})\b")

# "<number><unit>" tokens, used to strip the size out of product_name
_QTY_TOKEN_RE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*(?:кг|kg|kgs|гр|г|g|grs|мл|ml|mls|л|l|lt|ком|пар|парчиња|pcs)\b\.?",
    re.IGNORECASE,
)
_PACK_TOKEN_RE = re.compile(r"\b\d{1,3}\s*[xх×]\s*\d+(?:[.,]\d+)?\b", re.IGNORECASE)
_PROMO_TOKEN_RE = re.compile(r"\b\d{1,3}\s*\+\s*\d{1,3}\b")

# "30Х45СМ", "175/165/55" — dimensions, never a sellable quantity
_DIMENSION_RE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*[xXхХ×]\s*\d+(?:[.,]\d+)?\s*(?:см|цм|mm|мм|cm|m|м)\b",
    re.IGNORECASE,
)
_MULTIPLIER_RE = re.compile(r"\b\d{1,4}\s*[xXхХ×]\s*\d{1,4}(?!\d)")
_EXPLICIT_COUNT_RE = re.compile(r"\b(\d{1,4})\s*(?:пар|ком|парчиња|pcs)\b")

# dry goods sold by weight must never be converted to millilitres
_DRY_GOODS_RE = re.compile(
    r"\b(?:чај|чаеви|кафе|прашок|брашно|шеќер|сол|зачин|бибер|ориз|тестенини|гриз|какао|желатин|квасец|супа|засладувач|цикорија|овес|жито|леќа|грав|компир|сладолед)\b",
    re.IGNORECASE,
)
_PACKAGING_RE = re.compile(
    r"\b(?:вакум|вакуум|вакуумско|вакумско|стакло|стаклена|лименка|тетрапак|пластика|пластична|картон|картонска)\b"
    r"|\bпет\b(?=\s+\d)",
    re.IGNORECASE,
)


def _dedupe_fragments(text: str) -> str:
    """Drop exact duplicate fragments shops append ("NAME - NAME - NAME")."""
    parts = re.split(r"\s*[-–—/]\s*", text)
    kept: List[str] = []
    for part in parts:
        part = part.strip()
        if part and part not in kept:
            kept.append(part)
    return " ".join(kept)


def _collapse_repeats(text: str) -> str:
    """Collapse token blocks that repeat back-to-back ("x y x y" -> "x y")."""
    tokens = text.split()
    out: List[str] = []
    i = 0
    while i < len(tokens):
        matched = False
        for size in (3, 2, 1):
            if i + 2 * size <= len(tokens) and tokens[i : i + size] == tokens[i + size : i + 2 * size]:
                out.extend(tokens[i : i + size])
                i += 2 * size
                matched = True
                break
        if not matched:
            out.append(tokens[i])
            i += 1
    return " ".join(out)


def _strip_noise_tokens(text: str, metadata: Optional[dict] = None) -> str:
    """Remove quantity/pack tokens, packaging words, legal forms, codes and metadata values."""
    text = _MULTIPACK_RE.sub(" ", text)
    text = _PROMO_SUM_RE.sub(" ", text)
    text = _QTY_TOKEN_RE.sub(" ", text)
    text = _PACKAGING_RE.sub(" ", text)
    text = _LEGAL_SUFFIX_RE.sub(" ", text)
    text = re.sub(r"\b\d{4,}\b", " ", text)  # shop codes / barcodes
    for value in (metadata or {}).values():
        cleaned = normalize_string(value)
        if cleaned and len(cleaned) > 2:
            text = re.sub(rf"(?<!\w){re.escape(cleaned)}(?!\w)", " ", text)
    return re.sub(r"\s+", " ", text).strip(" .-")


def clean_product_name(
    llm_name: Optional[str],
    raw_name: Optional[str],
    manufacturer: Optional[str],
    metadata: Optional[dict] = None,
) -> Optional[str]:
    """Build product_name from the raw source tokens the model selected.

    The model decides *which* words belong to the product description, but the
    spelling always comes from the raw shop name: LLM paraphrases and typos
    ("стаклена" -> "метелен") would otherwise break exact-match grouping.
    """
    raw_tokens = (normalize_string(raw_name) or "").split()
    wanted = set((normalize_string(llm_name) or "").split())
    if manufacturer:
        wanted -= set((normalize_string(manufacturer) or "").split())

    kept = [token for token in raw_tokens if token in wanted]
    text = _strip_noise_tokens(" ".join(kept), metadata)

    if not text:
        # model returned nothing usable: fall back to stripping the raw name
        fallback = normalize_string(raw_name) or ""
        fallback = _MULTIPACK_RE.sub(" ", fallback)
        fallback = _PROMO_SUM_RE.sub(" ", fallback)
        fallback = _QTY_TOKEN_RE.sub(" ", fallback)
        if manufacturer:
            brand = normalize_string(manufacturer)
            if brand:
                fallback = re.sub(rf"(?<!\w){re.escape(brand)}(?!\w)", " ", fallback)
        text = _strip_noise_tokens(fallback, metadata)

    text = _dedupe_fragments(text)
    text = _collapse_repeats(text)
    text = re.sub(r"\s+", " ", text).strip(" .-")
    text = re.sub(r"^(?:и|and)\s+", "", text)        # leftover conjunction from a split brand
    # a name made only of 1-2 character fragments carries no identity
    if text and not any(len(token) >= 3 for token in text.split()):
        return None
    return text or None


def canonical_quantity(
    value: Optional[float],
    unit: Optional[str],
    pack_count: Optional[int],
    is_fluid: bool,
) -> tuple[Optional[float], Optional[str]]:
    """Apply pack multiplication, fluid conversion, unit promotion and rounding."""
    if value is None or unit is None:
        return None, None
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return None, None

    mapped = _UNIT_MAP.get(str(unit).strip().lower().strip("."))
    if mapped is None:
        return None, None

    pack = pack_count or 1
    if pack < 1:
        pack = 1
    amount *= pack
    if amount <= 0:
        return None, None

    if mapped == "pcs":
        return (int(amount) if float(amount).is_integer() else round(amount, 3)), "pcs"

    # mass units -> grams, volume -> millilitres
    if mapped == "kg":
        amount *= 1000
        mapped = "g"
    elif mapped == "l":
        amount *= 1000
        mapped = "ml"

    # pourable liquids stored as volume, density treated as 1.0
    if is_fluid and mapped == "g":
        mapped = "ml"

    if mapped == "g" and amount >= 1000:
        amount /= 1000
        mapped = "kg"
    elif mapped == "ml" and amount >= 1000:
        amount /= 1000
        mapped = "l"

    return round(amount, 3), mapped


def parse_multipack_from_name(raw_name: str) -> tuple[Optional[float], Optional[int]]:
    """Fallback arithmetic straight from the raw name when the model misses it."""
    if not raw_name:
        return None, None
    promo = _PROMO_SUM_RE.search(raw_name)
    if promo:
        total = int(promo.group(1)) + int(promo.group(2))
        return total, 1
    multi = _MULTIPACK_RE.search(raw_name)
    if multi:
        count = int(multi.group(1))
        value = float(multi.group(2).replace(",", "."))
        return value, count
    return None, None


# --------------------------------------------------------------------------
# Manufacturer registry
# --------------------------------------------------------------------------
def resolve_manufacturer_id(conn, raw_manufacturer: Optional[str], cache: Optional[dict] = None) -> Optional[int]:
    """Get-or-create the canonical manufacturer and its normalised alias."""
    alias = normalize_string(raw_manufacturer)
    if not alias:
        return None
    alias = _LEGAL_SUFFIX_RE.sub(" ", alias)
    alias = re.sub(r"\s+", " ", alias).strip()
    if not alias:
        return None
    if cache is not None and alias in cache:
        return cache[alias]

    with conn.cursor() as cur:
        cur.execute("SELECT manufacturer_id FROM manufacturer_aliases WHERE alias = %s", (alias,))
        row = cur.fetchone()
        if row:
            if cache is not None:
                cache[alias] = row[0]
            return row[0]

        cur.execute(
            """
            INSERT INTO manufacturers (canonical_name, display_name)
            VALUES (%s, %s)
            ON CONFLICT (canonical_name) DO UPDATE SET canonical_name = EXCLUDED.canonical_name
            RETURNING id
            """,
            (alias, str(raw_manufacturer).strip()),
        )
        manufacturer_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO manufacturer_aliases (alias, manufacturer_id)
            VALUES (%s, %s)
            ON CONFLICT (alias) DO NOTHING
            """,
            (alias, manufacturer_id),
        )
    if cache is not None:
        cache[alias] = manufacturer_id
    return manufacturer_id


# --------------------------------------------------------------------------
# Gemini calls
# --------------------------------------------------------------------------
def estimate_tokens(batch: List[dict]) -> int:
    body = "\n".join(f"{i + 1}. {p['name']}" for i, p in enumerate(batch))
    return 900 + len(body) // 4 + len(batch) * 120


def _products_text(batch: List[dict]) -> str:
    lines = []
    for i, product in enumerate(batch, 1):
        context = " / ".join(
            part for part in (product.get("market"), product.get("sub_category")) if part
        )
        lines.append(f"{i}. Name: {product['name']}\n   Context: {context or 'n/a'}")
    return "\n".join(lines)


async def extract_batch(
    batch: List[dict], client: genai.Client, model_id: str, estimated_tokens: int
) -> List[ProductFeatures]:
    prompt = f"{PROMPT}\n\nPRODUCTS ({len(batch)}):\n{_products_text(batch)}"

    response = await call_with_retries(
        lambda: asyncio.to_thread(
            client.models.generate_content,
            model=model_id,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
                response_schema=BatchFeatures,
            ),
        ),
        rate_limiter=rate_limiter,
        estimated_tokens=estimated_tokens,
        attempts=RETRY_ATTEMPTS,
        label="extract-features",
    )
    parsed = BatchFeatures(**json.loads(response.text))

    by_index = {item.index: item for item in parsed.products}
    aligned: List[ProductFeatures] = []
    for position in range(1, len(batch) + 1):
        item = by_index.get(position)
        if item is None and len(parsed.products) == len(batch):
            item = parsed.products[position - 1]
        aligned.append(item)  # type: ignore[arg-type]
    return aligned


def build_row(product: dict, features: Optional[ProductFeatures]) -> dict:
    """Combine LLM output with deterministic normalisation into a DB row."""
    raw_name = product.get("name") or ""

    if features is None:
        # model dropped this product: keep the row raw, leave it for a retry
        return {
            "id": product["id"],
            "manufacturer_raw": None,
            "product_name": None,
            "quantity_value": None,
            "quantity_unit": None,
            "quantity_raw": None,
            "is_fluid": None,
            "metadata": None,
            "features_confidence": 0.0,
            "failed": True,
        }

    quantity_value = features.quantity_value
    quantity_unit = features.quantity_unit
    pack_count = features.pack_count or 1

    # fallback: model returned no quantity but the raw name clearly contains one
    if quantity_value is None:
        fallback_value, fallback_pack = parse_multipack_from_name(raw_name)
        if fallback_value is not None:
            quantity_value = fallback_value
            pack_count = fallback_pack or 1
            matched = _QTY_TOKEN_RE.search(raw_name) or _MULTIPACK_RE.search(raw_name)
            unit_match = re.search(
                r"(кг|kg|kgs|гр|г|g|grs|мл|ml|mls|л|l|lt|ком|пар|парчиња|pcs)",
                matched.group(0) if matched else "",
                re.IGNORECASE,
            )
            quantity_unit = unit_match.group(1) if unit_match else "pcs"

    # Guard: dry goods sold by weight (tea, coffee, flour) are never fluids, even
    # when the model reads "чај" and thinks "drink".
    is_fluid = features.is_fluid
    raw_unit = str(quantity_unit or "").lower().strip(".")
    if is_fluid and raw_unit in ("g", "gr", "г", "гр", "kg", "кг") and _DRY_GOODS_RE.search(raw_name):
        is_fluid = False

    final_value, final_unit = canonical_quantity(
        quantity_value, quantity_unit, pack_count, is_fluid
    )

    metadata = {entry.key.strip(): entry.value.strip() for entry in (features.metadata or []) if entry.key}
    if pack_count and pack_count > 1:
        metadata.setdefault("pack_count", str(pack_count))

    # Guards on "pcs" quantities: written counts win, dimension arithmetic loses.
    norm_raw = normalize_string(raw_name) or ""
    explicit_count = _EXPLICIT_COUNT_RE.search(norm_raw)
    looks_like_dimension = bool(_DIMENSION_RE.search(norm_raw)) or bool(_MULTIPLIER_RE.search(norm_raw))

    if explicit_count and final_unit in (None, "pcs"):
        # "15пар 27х20" or "50пар": the written count is the quantity
        final_value, final_unit = int(explicit_count.group(1)), "pcs"
        pack_count = 1
        quantity_value = float(explicit_count.group(1))
        quantity_unit = "pcs"
        metadata["pack_count"] = str(final_value)
        if final_value == 1:
            metadata.pop("pack_count")
    elif final_unit == "pcs" and looks_like_dimension:
        # "24Х24ЦМ 3СЛ": dimensions are not a quantity
        final_value, final_unit = None, None
        pack_count = 1
        quantity_value = None
        metadata.pop("pack_count", None)

    quantity_raw = None
    if final_value is not None and quantity_value is not None:
        suffix = f" x{pack_count}" if pack_count and pack_count > 1 else ""
        quantity_raw = f"{quantity_value:g} {quantity_unit or ''}{suffix}".strip()

    return {
        "id": product["id"],
        "manufacturer_raw": features.manufacturer,
        "product_name": clean_product_name(
            features.product_name or raw_name, raw_name, features.manufacturer, metadata
        ),
        "quantity_value": final_value,
        "quantity_unit": final_unit,
        "quantity_raw": quantity_raw,
        "is_fluid": is_fluid,
        "metadata": metadata or None,
        "features_confidence": float(features.confidence),
        "failed": False,
    }


async def extract_all(products: List[dict], model_id: str, batch_size: int, concurrency: int) -> List[dict]:
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY not found in environment")
    client = genai.Client(api_key=api_key)

    batches = [products[i : i + batch_size] for i in range(0, len(products), batch_size)]
    semaphore = asyncio.Semaphore(concurrency)
    results: List[dict] = []
    completed = 0
    failed = 0
    started = time.time()

    async def run(batch: List[dict]) -> None:
        nonlocal completed, failed
        async with semaphore:
            estimated_tokens = estimate_tokens(batch)
            try:
                features = await extract_batch(batch, client, model_id, estimated_tokens)
            except Exception as exc:  # noqa: BLE001 - one bad batch must not kill the run
                failed += len(batch)
                completed += len(batch)
                print(f"   x batch failed ({len(batch)} products, left for retry): {exc}")
                return
            rows = [build_row(product, item) for product, item in zip(batch, features)]
            failed += sum(1 for row in rows if row["failed"])
            results.extend(rows)
            completed += len(batch)
            elapsed = time.time() - started
            rate = completed / elapsed if elapsed else 0
            print(f"   {completed:,}/{len(products):,} ({completed * 100 // max(len(products), 1)}%) | {rate:.1f}/s")

    await asyncio.gather(*(run(batch) for batch in batches))
    print(f"\n   Batches: {len(batches)} | products failed/unaligned: {failed:,}")
    return results


# --------------------------------------------------------------------------
# DB access
# --------------------------------------------------------------------------
def load_products(conn, limit: Optional[int], sample: Optional[int], ids: Optional[List[str]], reprocess: bool) -> List[dict]:
    # "Разно" / "Останато" rows are never groupable, so they are excluded from the queue.
    where = [
        "name IS NOT NULL",
        "name <> ''",
        "COALESCE(main_category, '') <> ALL(%s::text[])",
        "COALESCE(sub_category, '') <> ALL(%s::text[])",
    ]
    params: list = [list(SKIP_MAIN_CATEGORIES), list(SKIP_SUB_CATEGORIES)]
    if not reprocess:
        where.append("features_extracted_at IS NULL")
    if ids:
        where.append("id = ANY(%s::uuid[])")
        params.append(ids)

    if sample:
        query = f"""
            SELECT id, name, market, main_category, sub_category
            FROM products
            WHERE {' AND '.join(where)}
            ORDER BY random()
            LIMIT {int(sample)}
        """
    else:
        query = f"""
            SELECT id, name, market, main_category, sub_category
            FROM products
            WHERE {' AND '.join(where)}
            ORDER BY id
        """
        if limit:
            query += f" LIMIT {int(limit)}"

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return [dict(row) for row in cur.fetchall()]


def save_features(conn, rows: List[dict]) -> int:
    if not rows:
        return 0

    payload = []
    now = datetime.now()
    cache: dict = {}
    for row in rows:
        manufacturer_id = resolve_manufacturer_id(conn, row["manufacturer_raw"], cache)
        payload.append(
            (
                str(row["id"]),
                manufacturer_id,
                row["product_name"],
                row["quantity_value"],
                row["quantity_unit"],
                row["quantity_raw"],
                row["is_fluid"],
                json.dumps(row["metadata"], ensure_ascii=False) if row["metadata"] else None,
                row["features_confidence"],
                FEATURES_VERSION,
                now,
            )
        )

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            UPDATE products AS p SET
                manufacturer_id       = v.manufacturer_id,
                product_name          = v.product_name,
                quantity_value        = v.quantity_value,
                quantity_unit         = v.quantity_unit,
                quantity_raw          = v.quantity_raw,
                is_fluid              = v.is_fluid,
                metadata              = v.metadata::jsonb,
                features_confidence   = v.features_confidence,
                features_version      = v.features_version,
                features_extracted_at = v.extracted_at
            FROM (VALUES %s) AS v(
                id, manufacturer_id, product_name, quantity_value, quantity_unit,
                quantity_raw, is_fluid, metadata, features_confidence, features_version, extracted_at
            )
            WHERE p.id = v.id::uuid
            """,
            payload,
            page_size=500,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        )
    conn.commit()
    return len(payload)


PRINT_FIELDS = (
    "manufacturer_raw",
    "product_name",
    "quantity_value",
    "quantity_unit",
    "quantity_raw",
    "is_fluid",
    "metadata",
    "features_confidence",
)


def print_rows(products: List[dict], rows: List[dict], limit: int = 40) -> None:
    by_id = {str(p["id"]): p for p in products}
    print("\n" + "=" * 100)
    for row in rows[:limit]:
        source = by_id.get(str(row["id"]), {})
        print(f"\nRAW    : {source.get('name')}")
        print(f"  market: {source.get('market')} | {source.get('main_category')} / {source.get('sub_category')}")
        if row["failed"]:
            print("  FAILED : not extracted (row left for retry)")
            continue
        for field in PRINT_FIELDS:
            print(f"  {field:<20}: {row[field]}")
    print("\n" + "=" * 100)
    print(f"showing {min(len(rows), limit)} of {len(rows)} extracted rows")


# --------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------
async def main() -> int:
    parser = argparse.ArgumentParser(description="Extract product features (manufacturer / name / quantity).")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N products (first N by id)")
    parser.add_argument("--sample", type=int, default=None, help="Process N randomly chosen products")
    parser.add_argument("--ids", type=str, default=None, help="Comma-separated product UUIDs")
    parser.add_argument("--dry-run", action="store_true", help="Print results, write nothing")
    parser.add_argument("--reprocess", action="store_true", help="Include rows that were already extracted")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    args = parser.parse_args()

    load_dotenv(find_dotenv())

    # Windows consoles default to cp1252; Macedonian output and symbols need UTF-8.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    ids = [value.strip() for value in args.ids.split(",") if value.strip()] if args.ids else None

    conn = connect_to_db()
    try:
        products = load_products(conn, args.limit, args.sample, ids, args.reprocess)
        if not products:
            print("Nothing to extract — every groupable product already has features.")
            return 0

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM products
                WHERE features_extracted_at IS NULL
                  AND (COALESCE(main_category, '') = ANY(%s::text[])
                       OR COALESCE(sub_category, '') = ANY(%s::text[]))
                """,
                (list(SKIP_MAIN_CATEGORIES), list(SKIP_SUB_CATEGORIES)),
            )
            skipped = cur.fetchone()[0]
        if skipped:
            print(
                f"Skipping {skipped:,} products in "
                f"{'/'.join(SKIP_MAIN_CATEGORIES)} main category or "
                f"{'/'.join(SKIP_SUB_CATEGORIES)} subcategory (not groupable, no tokens spent)"
            )

        print(f"Extracting features for {len(products):,} products")
        print(f"  model={args.model} batch_size={args.batch_size} concurrency={args.concurrency} dry_run={args.dry_run}")

        rows = await extract_all(products, args.model, args.batch_size, args.concurrency)
        rows = [row for row in rows if not row["failed"]]

        print_rows(products, rows)

        if args.dry_run:
            print("\nDRY RUN - nothing written to the database.")
        else:
            written = save_features(conn, rows)
            print(f"\n[saved] Updated {written:,} rows (features_version={FEATURES_VERSION})")

        stats = rate_limiter.get_stats()
        print(f"API requests: {stats['total_requests']:,} | tokens: {stats['total_tokens']:,}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
