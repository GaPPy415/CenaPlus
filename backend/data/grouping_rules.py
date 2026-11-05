"""Deterministic grouping rules — the single home for identity/variant logic.

Extracted from the collision study (`same_market_collisions.py`) so that both the
study and the grouper use exactly the same rules.

Vocabulary:
  identity key  = manufacturer_id + product_name_sorted + quantity + unit + is_fluid + variant_key
  variant       = anything that makes two same-named products different:
                  numbers+units (75М vs 100М), dimensions (24Х24СМ), gender (м/ж),
                  metadata leftovers (code, packaging, accessory, pack_count, shop…)
  promo         = wording that is NOT identity (гратис, промо, N+M, "со 2 чаши")
"""

from __future__ import annotations

import difflib
import hashlib
import json
import re

from backend.data.text_utils import normalize_string

# --------------------------------------------------------------------------
# metadata keys that describe the physical variant of the product
# --------------------------------------------------------------------------
VARIANT_KEY_ALIASES = {
    "dimensions": "dimensions", "dimension": "dimensions", "dimenzii": "dimensions",
    "size": "dimensions", "sizes": "dimensions", "dimensii": "dimensions",
    "packaging": "packaging", "pack": "packaging", "pakuvanje": "packaging",
    "packing": "packaging", "paket": "packaging", "pakovanje": "packaging",
    "code": "code", "model": "code", "sku": "code", "barcode": "code",
    "sifra": "code", "artikul": "code", "product_code": "code",
    "pack_count": "pack_count", "packcount": "pack_count", "units": "pack_count",
    "accessory": "accessory", "accessories": "accessory", "dodatok": "accessory",
}

# promo wording is not identity
PROMO_TOKENS = {
    "гратис", "промо", "акција", "акциja", "подарок", "бесплатно", "бонус",
    "попуст", "ново", "специјална", "специјална_понуда",
}
PROMO_KEYS = {"promo", "promotion", "акција", "промо"}

# unit-form words describe the physical form of the product
FORM_WORDS = {"парче", "парчиња", "цело", "цела", "рефус", "пакет", "пак", "сецкано", "лист"}

# unit spellings that are pure formatting
UNIT_CANON = {
    "г": "g", "гр": "g", "g": "g", "kg": "kg", "кг": "kg",
    "ml": "ml", "мл": "ml", "l": "l", "л": "l",
    "см": "cm", "cm": "cm", "мм": "mm", "mm": "mm", "m": "m", "м": "m",
}
NUMBER_UNIT_RE = re.compile(
    r"(\d+(?:[.,]\d+)?)\s*(гр|г|g|kg|кг|ml|мл|л|l|см|cm|мм|mm|m|м)\b",
    re.IGNORECASE,
)
DIMENSION_RE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*[xх×]\s*\d+(?:[.,]\d+)?(?:\s*[xх×]\s*\d+(?:[.,]\d+)?)?\b"
)

KEY_VERSION = 2


# --------------------------------------------------------------------------
# string helpers
# --------------------------------------------------------------------------
def canonicalize_units(name: str) -> str:
    """Fold unit spellings so "250 ГР" and "250Г" compare equal ("250g")."""
    def repl(match: re.Match) -> str:
        number = match.group(1).replace(",", ".")
        unit = UNIT_CANON.get(match.group(2).lower())
        return f"{number}{unit}" if unit else match.group(0)

    return NUMBER_UNIT_RE.sub(repl, name)


def strip_promo(name: str) -> str:
    """Drop promo wording and n+m patterns so campaigns don't look like variants."""
    text = re.sub(r"\b\d\s*\+\s*\d\b", " ", name)
    return " ".join(t for t in text.split() if t not in PROMO_TOKENS).strip()


def strip_metadata_promo(name: str, metadata) -> str:
    """Remove the tokens the model itself flagged as promo ("со 2 чаши")."""
    for key, value in (metadata or {}).items():
        if str(key).strip().lower() not in PROMO_KEYS or not value:
            continue
        for token in (normalize_string(str(value)) or "").split():
            name = re.sub(rf"(?<!\w){re.escape(token)}(?!\w)", " ", name)
    return " ".join(name.split())


def form_signature(name: str) -> tuple:
    return tuple(sorted(t for t in name.split() if t in FORM_WORDS))


def name_variant_signature(name: str) -> tuple:
    """Variant signals left inside the raw name: sizes (75М/100М), dimensions, gender."""
    signature = set()
    for number, unit in NUMBER_UNIT_RE.findall(name):
        signature.add(f"{number.replace(',', '.')}{UNIT_CANON.get(unit.lower(), unit.lower())}")
    for dims in DIMENSION_RE.findall(name):
        signature.add(re.sub(r"\s", "", dims))
    for token in re.findall(r"(?<!\d)\s(м|ж)\s", f" {name} "):
        signature.add(f"gender:{token}")
    for token in name.split():
        if token in {"машки", "женски"}:
            signature.add(f"gender:{token}")
    return tuple(sorted(signature))


def variant_dict(metadata) -> dict[str, str]:
    """Variant fingerprint as a dict (every key except promo wording is identity)."""
    out: dict[str, str] = {}
    for key, value in (metadata or {}).items():
        raw_key = str(key).strip().lower()
        if raw_key in PROMO_KEYS:
            continue
        canonical = VARIANT_KEY_ALIASES.get(raw_key, raw_key)
        text = normalize_string(str(value)) or ""
        text = re.sub(r"[\s×xх*.,-]", "", text)
        out[canonical] = out.get(canonical, "") + "|" + text
    return out


def canonical_variant(metadata) -> tuple:
    """Variant fingerprint from metadata: every key except promo wording is identity."""
    return tuple(sorted(variant_dict(metadata).items()))


def variants_compatible(a: dict[str, str], b: dict[str, str]) -> bool:
    """Compatible when no key present in both holds different values.

    A missing key means "unknown", not "different" — the model records leftovers
    inconsistently, so requiring identical key sets splits identical products.
    """
    return all(a[key] == b[key] for key in a.keys() & b.keys())


# --------------------------------------------------------------------------
# merge/split decision for one collision bucket
# --------------------------------------------------------------------------
def prices_compatible(rows: list[dict]) -> bool:
    """True when prices agree, or when a row is out of stock and can't be compared."""
    prices = [row["price"] for row in rows if row["price"] is not None]
    if not prices:
        return True
    if len(set(prices)) == 1:
        return True
    return any(not row["in_stock"] for row in rows)


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if abs(len(a) - len(b)) > 1:
        return 2
    previous = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        current = [i]
        for j, cb in enumerate(b, 1):
            current.append(min(previous[j] + 1, current[j - 1] + 1, previous[j - 1] + (ca != cb)))
        previous = current
    return previous[-1]


MERGE_CLASSES = ("kratok_rok", "duplicate", "promo", "word_order", "near_identical", "single_char")

# tokens that may differ without changing the product
SOFT_TOKENS = {"со", "и", "за", "од", "на", "во", "по", "до"}
# known spelling equivalences between shops (add as they are found)
TOKEN_SYNONYMS = {
    frozenset(("милк", "млеко")),
    frozenset(("концетрат", "концентрат")),
}


def _token_close(a: str, b: str) -> bool:
    if a == b or frozenset((a, b)) in TOKEN_SYNONYMS:
        return True
    return difflib.SequenceMatcher(None, a, b).ratio() >= 0.85


def _has_digit(token: str) -> bool:
    return any(ch.isdigit() for ch in token)


def guarded_fuzzy_merge(name_a: str, name_b: str) -> bool:
    """Merge two names only when every difference is a typo or a soft word.

    Numeric/percentage/model tokens block the merge outright: `формула 1` vs
    `формула 2`, `батерии аа` vs `ааа`, `јогурт 0%` vs `10%` are different
    products even at 0.98 character similarity.
    """
    tokens_a, tokens_b = name_a.split(), name_b.split()
    if sorted(tokens_a) == sorted(tokens_b):
        return True

    counts_a, counts_b = {}, {}
    for token in tokens_a:
        counts_a[token] = counts_a.get(token, 0) + 1
    for token in tokens_b:
        counts_b[token] = counts_b.get(token, 0) + 1
    only_a = [t for t in counts_a for _ in range(counts_a[t] - counts_b.get(t, 0))]
    only_b = [t for t in counts_b for _ in range(counts_b[t] - counts_a.get(t, 0))]

    if not only_a and not only_b:
        return True
    if len(only_a) > 3 or len(only_b) > 3:
        return False
    if any(_has_digit(t) for t in only_a + only_b):
        return False
    if not only_a or not only_b:
        # one name carries extra words: only soft words are acceptable
        return all(t in SOFT_TOKENS for t in (only_a or only_b))

    def all_match(source: list[str], target: list[str]) -> bool:
        pool = list(target)
        for token in source:
            match = next((cand for cand in pool if _token_close(token, cand)), None)
            if match is None:
                return False
            pool.remove(match)
        return True

    return all_match(only_a, only_b) and all_match(only_b, only_a)


def classify(rows: list[dict]) -> str:
    """Classify one same-market collision bucket. MERGE_CLASSES may share a group."""
    raw_names = [normalize_string(row["raw_name"]) or "" for row in rows]
    names = [strip_metadata_promo(strip_promo(canonicalize_units(name)), row["metadata"])
             for name, row in zip(raw_names, rows)]
    variants = [canonical_variant(row["metadata"]) for row in rows]
    market = rows[0]["market"]

    if market == "vero" and any("краток рок" in (row["raw_name"] or "").lower() for row in rows):
        return "kratok_rok"
    if len(set(variants)) > 1:
        return "variant"
    if len({name_variant_signature(name) for name in raw_names}) > 1:
        return "variant"
    if len({form_signature(name) for name in names}) > 1:
        return "variant"
    if len(set(names)) == 1:
        return "promo" if len(set(raw_names)) > 1 else "duplicate"
    if len({tuple(sorted(name.split())) for name in names}) == 1:
        return "word_order"

    max_similarity = max(
        difflib.SequenceMatcher(None, names[i], names[j]).ratio()
        for i in range(len(names)) for j in range(i + 1, len(names))
    )
    if max_similarity >= 0.90 and prices_compatible(rows):
        return "near_identical"

    max_distance = max(levenshtein(names[i], names[j])
                       for i in range(len(names)) for j in range(i + 1, len(names)))
    if max_distance <= 1 and prices_compatible(rows):
        return "single_char"
    return "residual"


# --------------------------------------------------------------------------
# identity key derivation
# --------------------------------------------------------------------------
def compute_keys(product_name: str | None, raw_name: str | None, metadata) -> tuple[str | None, str | None, str]:
    """Return (product_name_key, product_name_sorted, variant_key) for one product."""
    base = normalize_string(product_name) or ""
    base = canonicalize_units(strip_metadata_promo(strip_promo(base), metadata))
    if not base:
        # extraction produced no name: fall back to the cleaned raw name so these
        # rows keep an identity instead of all collapsing onto one NULL key
        base = canonicalize_units(strip_promo(normalize_string(raw_name) or ""))
    name_key = base or None
    name_sorted = " ".join(sorted(base.split())) or None

    signature = canonical_variant(metadata) + name_variant_signature(normalize_string(raw_name) or "")
    payload = json.dumps(signature, ensure_ascii=False, default=str)
    variant_key = hashlib.md5(payload.encode("utf-8")).hexdigest()
    return name_key, name_sorted, variant_key
