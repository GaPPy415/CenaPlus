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
import itertools
import json
import math
import re
from collections import Counter, defaultdict
from statistics import median

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


def _is_negation_pair(a: str, b: str) -> bool:
    """True for exact Macedonian negation opposites like газирана / негазирана."""
    return (a.startswith("не") and a[2:] == b) or (b.startswith("не") and b[2:] == a)


def _token_close(a: str, b: str) -> bool:
    if a == b or frozenset((a, b)) in TOKEN_SYNONYMS:
        return True
    if _is_negation_pair(a, b):
        return False
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


# --------------------------------------------------------------------------
# Layers 1-4: Non-LLM cross-market blocking, safety guards, and IR matching
# --------------------------------------------------------------------------
CORE_GROCERY_CATEGORIES = {
    "Сосови, намази и конзерви",
    "Млечни производи",
    "Пекара",
    "Безалкохолни пијалоци",
    "Топли пијалоци",
    "Основни намирници",
    "Алкохолни пијалоци",
    "Слатки и грицки",
    "Замрзната храна",
    "Месо и ладна трпеза",
    "Здравје и исхрана",
    "Домашна хемија",
    "Лична хигиена",
    "Овошје и зеленчук",
    "Риба и морска храна",
    "Тутун и никотин",
}

NON_FOOD_CATEGORIES = {
    "Дом и домаќинство",
    "Облека",
    "Разно",
    "Бебиња и деца",
    "Миленици",
}

# Physical variant keys allowed to split cross-market groups (S2.3c allowlist).
# Shop-internal 4-10 digit ERP codes and descriptive LLM leftovers are dropped,
# while short/alphanumeric shade/pasta/model codes (e.g. no.48, 109, С126-6) are kept.
PHYSICAL_VARIANT_KEYS = {
    "dimensions",
    "packaging",
    "pack_count",
    "accessory",
    "shade",
    "color",
    "age",
    "weight_range",
    "alcohol",
    "layers",
    "power",
}

# Pure category/type boilerplate (never ingredients like кашкавал, сирење, путер, млеко, бисквит, житарици)
GENERIC_HEAD_NOUNS = {
    "чоколадо", "чоколада", "чок", "јогурт", "сок", "нектар", "пијалок", "вода",
    "кафе", "чај", "пиво", "вино", "кекс", "наполитанки", "вафли",
    "мусли", "гранола", "каша", "чипс", "крекер", "крекери", "грицки",
    "паста", "шампон", "сапун", "крема", "крем", "лосион", "дезодоранс", "део",
    "детергент", "детер", "омекнувач", "средство", "освежувач", "чистач",
    "туна", "паштета", "салата", "сос", "кечап", "мајонез", "зачин", "супа",
    "тестенини", "макарони", "шпагети", "леб", "пециво",
    "храна", "риба", "месо", "виршли", "салама", "кулен", "пршута",
    "бонбони", "бомбони", "гуми", "лижавка", "лижавче", "пудинг", "сладолед",
    "сет", "пакет", "производ", "артикл", "вкус", "кондиционер", "регенератор",
}

# Universal high-risk distinguishing modifiers (never silently dropped or swapped)
PROTECTED_MODIFIERS = {
    "протеин", "протеински", "протеинска", "протеинско",
    "без", "лактоза", "глутен", "шеќер", "шекер", "посна", "посен", "посно",
    "цел", "цели", "цело", "цела", "мелен", "мелена", "мелено", "сецкан", "сецкано",
    "кранчи", "crunchy",
    "бел", "бела", "бело", "темен", "темна", "темно", "црн", "црна", "црно",
    "црвен", "црвена", "црвено", "зелен", "зелена", "зелено", "жолт", "жолта", "жолто",
    "син", "сина", "сино", "розев", "розева", "розево", "портокал",
    "лајт", "light", "зиро", "зеро", "zero", "free", "фри",
    "макс", "max", "мини", "mini", "микро", "xl", "xxl", "xxxl", "xs", "xxs",
    "аа", "ааа", "aa", "aaa", "lr6", "lr03",
    "сензитив", "сенситив", "нормал", "нормална", "сува", "масна", "интензив", "екстра", "ултра",
    "дневна", "ноќна", "ноќен", "дневен",
    "машки", "женски", "детски", "бебешки", "јуниор", "junior", "men", "women", "kids", "baby",
    "бонбониера", "бонбони", "прашок", "гел", "капсули", "таблети", "течен", "течна", "течно",
    "суво", "свежо", "дим", "димен", "димена", "димено", "лут", "лута", "луто", "благ", "блага", "благо",
    "газирана", "негазирана", "природна", "минерална", "изворска",
    "декаф", "безкофеинско", "турско", "еспресо", "филтер",
    "интегрален", "интегрална", "интегрално", "обичен", "обична", "обично",
    "класик", "класичен", "оригинал", "оригинален",
    "дуо", "трио", "мулти", "рефил", "дополнување",
}

APPLICATOR_FORMS = {
    "спреј": "spray",
    "стик": "stick",
    "рол": "rollon",
    "ролон": "rollon",
    "пена": "foam",
    "крема": "cream",
    "крем": "cream",
    "лосион": "lotion",
    "масло": "oil",
    "гел": "gel",
}

# Form/packaging words that never cross-align
ALL_FORM_WORDS = set(FORM_WORDS) | {
    "бонбони", "бонбониера", "колач", "колачи", "колачиња",
    "пакет", "пакетче", "стапче", "стапчиња", "снек", "млеко", "млечно",
}

_STANDARD_G_ML = (
    10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 65, 70, 75, 80, 85, 90, 95, 100,
    110, 115, 120, 125, 130, 140, 150, 160, 170, 175, 180, 190, 200, 220, 225,
    240, 250, 270, 275, 280, 300, 320, 330, 350, 360, 375, 380, 400, 420, 450,
    480, 500, 550, 600, 650, 700, 720, 750, 800, 850, 900, 950, 1000,
    1200, 1250, 1500, 1750, 1800, 2000, 2500, 3000, 4000, 5000, 6000, 10000,
)
_STANDARD_PCS = (
    1, 2, 3, 4, 5, 6, 8, 10, 12, 15, 16, 18, 20, 24, 25, 30, 32, 36, 40, 48, 50, 60, 72, 80, 100,
)

_SP_PATTERN_A = re.compile(
    r"^\s*(\d+(?:[.,]\d+)?)\s*(?:ден\.?|д\.?)\s*/\s*(100\s*)?(кг|kg|гр|г|g|л|l|мл|ml|парче|пар|ком|pcs|ед)\b",
    re.IGNORECASE,
)
_SP_PATTERN_B = re.compile(
    r"^\s*(\d+(?:[.,]\d+)?)\s*(кг|kg|гр|г|g|л|l|мл|ml|парче|пар|par|ком|pcs|ед)\.?\s*:?\s*=\s*(\d+(?:[.,]\d+)?)",
    re.IGNORECASE,
)
_GLUED_CODE_RE = re.compile(r"([А-Яа-яЃѓЅѕЈјЉљЊњЌќЏџA-Za-z]{4,})(\d{5,8})\b")
_ZUR_BACKTICK_RE = re.compile(r"(?<=[А-Яа-яЃѓЅѕЈјЉљЊњЌќЏџ])`(?=[А-Яа-яЃѓЅѕЈјЉљЊњЌќЏџ])")
_ERP_ONLY_CODE_RE = re.compile(r"^\|?\d{4,10}$")

_RAW_BATTERY_RE = re.compile(
    r"(?<![А-Яа-яA-Za-z0-9])(ааа|аа|aaa|aa|lr03|lr6|cr\d{4}|9v|9в)(?![А-Яа-яA-Za-z0-9])",
    re.IGNORECASE,
)
_RAW_SPF_RE = re.compile(
    r"(?<![А-Яа-яA-Za-z0-9])(?:spf|спф|ф|f)\s*([0-9]{1,2}\+?)(?![А-Яа-яA-Za-z0-9])",
    re.IGNORECASE,
)
_RAW_FAT_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*%")
_RAW_AGE_PLUS_RE = re.compile(r"(?<![0-9])(\d{2})\+")

_CYR_VOWELS = set("аеиоуaeiouy")
_PHONETIC_FOLD = str.maketrans({
    "з": "с", "ѕ": "с",
    "џ": "ч", "ѓ": "к", "ќ": "к",
    "њ": "н", "љ": "л",
    "ј": "и",
})
_MK_SUFFIXES = ("ски", "ска", "ско", "ени", "ена", "ено", "ен", "на", "но", "ни", "ци", "и", "а", "о")


def extract_raw_safety_sig(raw_name: str | None) -> dict[str, str]:
    """Critical physical specs pulled from raw name so metadata pruning can never lose them."""
    if not raw_name:
        return {}
    s = raw_name.lower()
    sig: dict[str, str] = {}

    bats = sorted({m.group(1).replace("a", "а") for m in _RAW_BATTERY_RE.finditer(s)})
    if bats:
        sig["battery"] = "|".join(bats)

    spfs = sorted({m.group(1) for m in _RAW_SPF_RE.finditer(s)})
    if spfs:
        sig["spf"] = "|".join(spfs)

    fats = sorted({str(float(m.group(1).replace(",", "."))) for m in _RAW_FAT_RE.finditer(s)})
    if fats:
        sig["pct"] = "|".join(fats)

    ages = sorted({m.group(1) for m in _RAW_AGE_PLUS_RE.finditer(s)})
    if ages:
        sig["age_plus"] = "|".join(ages)

    norm_toks = set((normalize_string(raw_name) or "").split())
    apps = sorted({APPLICATOR_FORMS[t] for t in norm_toks if t in APPLICATOR_FORMS})
    if len(apps) == 1:
        sig["app"] = apps[0]

    return sig


def safety_sigs_compatible(a: dict[str, str], b: dict[str, str]) -> bool:
    for strict_k in ("battery", "spf", "age_plus"):
        if (strict_k in a or strict_k in b) and a.get(strict_k) != b.get(strict_k):
            return False
    for both_k in ("pct", "app"):
        if both_k in a and both_k in b and a[both_k] != b[both_k]:
            return False
    return True


def recover_qty_from_singular_price(price: int | None, singular_price: str | None) -> tuple[float | None, str | None]:
    """Derive (base_qty, kind) from price / singular_price when within +/-4% of a standard size."""
    if not price or price <= 0 or not singular_price:
        return None, None
    s = singular_price.strip()
    ref_val = None
    unit_raw = None
    unit_price = None

    m = _SP_PATTERN_A.match(s)
    if m:
        unit_price = float(m.group(1).replace(",", "."))
        ref_val = 100.0 if m.group(2) else 1.0
        unit_raw = m.group(3).lower()
    else:
        m = _SP_PATTERN_B.match(s)
        if m:
            ref_val = float(m.group(1).replace(",", "."))
            unit_raw = m.group(2).lower()
            unit_price = float(m.group(3).replace(",", "."))

    if not unit_price or unit_price <= 0 or not unit_raw:
        return None, None

    ratio = price / unit_price
    if unit_raw in ("кг", "kg", "л", "l"):
        raw_base = ratio * ref_val * 1000.0
        kind = "mv"
        standards = _STANDARD_G_ML
    elif unit_raw in ("гр", "г", "g", "мл", "ml"):
        raw_base = ratio * ref_val
        kind = "mv"
        standards = _STANDARD_G_ML
    elif unit_raw in ("парче", "пар", "par", "ком", "pcs", "ед"):
        raw_base = ratio * ref_val
        kind = "pcs"
        standards = _STANDARD_PCS
    else:
        return None, None

    best = min(standards, key=lambda x: abs(x - raw_base))
    if best > 0 and abs(raw_base - best) / best <= 0.04:
        return float(best), kind
    return None, None


def unified_base_qty(qty_val, qty_unit: str | None) -> tuple[str | None, float | None]:
    """Fold g/kg/ml/l onto one numeric base scale (grams/ml) so 400g == 400ml and 1kg == 1000g."""
    if qty_val is None or not qty_unit:
        return None, None
    val = float(qty_val)
    if qty_unit in ("g", "ml"):
        return "mv", round(val, 2)
    if qty_unit in ("kg", "l"):
        return "mv", round(val * 1000.0, 2)
    if qty_unit == "pcs":
        return "pcs", round(val, 2)
    return qty_unit, round(val, 2)


def clean_variant_allowlist(variant: dict | None, raw_sig: dict[str, str]) -> dict[str, str]:
    """Filter variant dict to physical keys + short/alphanumeric model codes + raw safety specs."""
    out = {k: v for k, v in (variant or {}).items() if k in PHYSICAL_VARIANT_KEYS}
    code_val = (variant or {}).get("code")
    if code_val and not _ERP_ONLY_CODE_RE.match(str(code_val).strip()):
        out["code"] = code_val
    for k in ("battery", "spf", "age_plus", "pct"):
        if k in raw_sig:
            out[f"_raw_{k}"] = raw_sig[k]
    return out


def clean_name_tokens(
    product_name_key: str | None,
    raw_name: str | None,
    manufacturer_id: int | None,
    alias_map: dict[int, dict[str, str]] | None = None,
    mfr_own_tokens: dict[int, set[str]] | None = None,
) -> list[str]:
    """Repair Zur backtick=ж leaks, recover glued-code words, apply token aliases, strip leaked brand tokens."""
    key = product_name_key or ""
    raw = raw_name or ""

    if "`" in raw and _ZUR_BACKTICK_RE.search(raw):
        for m in re.finditer(r"[А-Яа-яЃѓЅѕЈјЉљЊњЌќЏџ]+`[А-Яа-яЃѓЅѕЈјЉљЊњЌќЏџ]+", raw):
            bad_parts = m.group(0).lower().split("`")
            good_word = normalize_string(m.group(0).replace("`", "ж")) or ""
            if len(bad_parts) == 2 and good_word:
                p0 = normalize_string(bad_parts[0]) or ""
                p1 = normalize_string(bad_parts[1]) or ""
                toks = key.split()
                if p0 in toks and p1 in toks:
                    toks.remove(p0)
                    toks.remove(p1)
                    toks.append(good_word)
                    key = " ".join(toks)

    toks = key.split()
    for m in _GLUED_CODE_RE.finditer(raw):
        word = normalize_string(m.group(1)) or ""
        if len(word) >= 4 and word not in toks:
            toks.append(word)

    if alias_map and manufacturer_id is not None:
        mfr_aliases = alias_map.get(manufacturer_id)
        if mfr_aliases:
            toks = [mfr_aliases.get(t, t) for t in toks]

    if mfr_own_tokens and manufacturer_id is not None and len(toks) > 1:
        own = mfr_own_tokens.get(manufacturer_id)
        if own:
            filtered = [t for t in toks if t not in own]
            if filtered:
                toks = filtered

    return toks


def consonant_skeleton(token: str) -> str:
    """Consonant skeleton for Cyrillic/transliterated tokens (алпен/алпин/алпине -> лпн)."""
    if not token:
        return ""
    folded = token.translate(_PHONETIC_FOLD)
    chars = [c for c in folded if c not in _CYR_VOWELS]
    out: list[str] = []
    for c in chars:
        if not out or out[-1] != c:
            out.append(c)
    return "".join(out)


def mk_stem(token: str) -> str:
    if len(token) < 6 or _has_digit(token):
        return token
    for suf in _MK_SUFFIXES:
        if token.endswith(suf) and len(token) - len(suf) >= 4:
            return token[: -len(suf)]
    return token


def _levenshtein_bounded(a: str, b: str, max_d: int = 2) -> int:
    if a == b:
        return 0
    if abs(len(a) - len(b)) > max_d:
        return max_d + 1
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        row_min = i
        for j, cb in enumerate(b, 1):
            v = min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + (ca != cb))
            curr.append(v)
            if v < row_min:
                row_min = v
        if row_min > max_d:
            return max_d + 1
        prev = curr
    return prev[-1]


def tokens_align(a: str, b: str) -> bool:
    """True when two tokens are strongly aligned (exact, synonym, phonetic, stem, or prefix abbrev)."""
    if a == b:
        return True
    if frozenset((a, b)) in TOKEN_SYNONYMS:
        return True
    if _is_negation_pair(a, b):
        return False
    if _has_digit(a) or _has_digit(b):
        return False
    if a in ALL_FORM_WORDS or b in ALL_FORM_WORDS:
        return False
    if a in PROTECTED_MODIFIERS or b in PROTECTED_MODIFIERS:
        return len(a) >= 5 and len(b) >= 5 and mk_stem(a) == mk_stem(b)

    short, long_ = (a, b) if len(a) <= len(b) else (b, a)
    if len(short) < 3:
        return False

    if len(short) >= 5 and mk_stem(a) == mk_stem(b):
        return True

    if long_.startswith(short):
        if len(short) >= 4 and len(long_) - len(short) <= 7:
            return True
        if len(short) == 3 and len(long_) >= 6:
            return True

    if len(short) >= 4 and a[0] == b[0]:
        sa, sb = consonant_skeleton(a), consonant_skeleton(b)
        if len(sa) >= 3 and sa == sb and abs(len(a) - len(b)) <= 2:
            return True
        if (
            len(short) >= 6
            and a[:2] == b[:2]
            and len(sa) >= 4
            and len(sb) >= 4
            and _levenshtein_bounded(sa, sb, 1) <= 1
            and _levenshtein_bounded(a, b, 2) <= 2
        ):
            return True

    if len(short) >= 5 and a[0] == b[0] and _levenshtein_bounded(a, b, 1) <= 1:
        return True

    return False


def build_cluster_summary(idx: int, rows: list[dict]) -> dict:
    """Build a compact descriptor for one cluster of rows inside a block."""
    in_stock_mkts = {r["market"] for r in rows if r.get("in_stock")}
    all_mkts = {r["market"] for r in rows}
    prices = [r["price"] for r in rows if r.get("in_stock") and r.get("price") and r["price"] > 0]
    var: dict[str, str] = {}
    raw_sig: dict[str, str] = {}
    for r in rows:
        for k, v in (r.get("variant") or {}).items():
            var.setdefault(k, v)
        for k, v in (r.get("_raw_sig") or extract_raw_safety_sig(r.get("name"))).items():
            raw_sig.setdefault(k, v)
    rep_key = Counter(r.get("product_name_sorted") or "" for r in rows).most_common(1)[0][0]
    main_cat = Counter(r.get("main_category") for r in rows).most_common(1)[0][0]
    sub_cat = Counter(r.get("sub_category") for r in rows).most_common(1)[0][0]
    embed_text = (rows[0].get("product_name") or rows[0].get("product_name_key") or rows[0].get("name") or "").strip()
    return {
        "idx": idx,
        "rows": list(rows),
        "in_stock_markets": in_stock_mkts,
        "all_markets": all_mkts,
        "median_price": median(prices) if prices else None,
        "variant": var,
        "raw_sig": raw_sig,
        "main_category": main_cat,
        "sub_category": sub_cat,
        "rep_key": rep_key,
        "content_tokens": rep_key.split(),
        "embed_text": embed_text,
        "has_kratok_rok": any("краток рок" in (r.get("name") or "").lower() for r in rows),
    }


def discover_same_market_differentiators(clusters: list[dict]) -> set[str]:
    """Layer 2: Tokens separating distinct in-stock clusters at the SAME market."""
    by_market: dict[str, list[dict]] = defaultdict(list)
    for cl in clusters:
        for mkt in cl["in_stock_markets"]:
            by_market[mkt].append(cl)

    protected: set[str] = set()
    for mkt, mkt_clusters in by_market.items():
        if len(mkt_clusters) < 2 or len(mkt_clusters) > 80:
            continue
        for c1, c2 in itertools.combinations(mkt_clusters, 2):
            if mkt == "vero" and (c1["has_kratok_rok"] or c2["has_kratok_rok"]):
                continue
            t1, t2 = c1["content_tokens"], c2["content_tokens"]
            if not t1 or not t2:
                continue
            shared = [x for x in t1 if any(tokens_align(x, y) for y in t2)]
            if not shared:
                continue
            only1 = [x for x in t1 if not any(tokens_align(x, y) for y in t2)]
            only2 = [y for y in t2 if not any(tokens_align(y, x) for x in t1)]
            if 1 <= len(only1) + len(only2) <= 3:
                for tok in only1 + only2:
                    if tok not in SOFT_TOKENS and tok not in GENERIC_HEAD_NOUNS and len(tok) >= 2:
                        protected.add(tok)
    return protected


def hard_veto_check(
    ca: dict,
    cb: dict,
    block_protected: set[str],
    max_price_ratio: float = 2.0,
) -> tuple[bool, float, list[tuple[str, str]], list[str], list[str], str]:
    """Shared hard-veto guard for Layers 2-4 and Layer 5.

    Returns (passed, price_ratio, aligned_pairs, unmatched_a, unmatched_b, veto_reason).
    """
    if ca["in_stock_markets"] & cb["in_stock_markets"]:
        return False, 99.0, [], [], [], "same_market"

    if not variants_compatible(ca["variant"], cb["variant"]):
        return False, 99.0, [], [], [], "variant"

    if not safety_sigs_compatible(ca["raw_sig"], cb["raw_sig"]):
        return False, 99.0, [], [], [], "raw_safety"

    price_ratio = 1.0
    if ca["median_price"] and cb["median_price"]:
        hi = max(ca["median_price"], cb["median_price"])
        lo = min(ca["median_price"], cb["median_price"])
        price_ratio = hi / lo if lo > 0 else 99.0
        if price_ratio > max_price_ratio:
            return False, price_ratio, [], [], [], "price_ratio"

    ta = [t for t in ca["content_tokens"] if t not in SOFT_TOKENS]
    tb = [t for t in cb["content_tokens"] if t not in SOFT_TOKENS]
    if not ta or not tb:
        return False, price_ratio, [], [], [], "empty"

    nums_a = sorted(t for t in ta if _has_digit(t))
    nums_b = sorted(t for t in tb if _has_digit(t))
    if nums_a != nums_b:
        return False, price_ratio, [], [], [], "numeric"

    forms_a = {t for t in ta if t in ALL_FORM_WORDS}
    forms_b = {t for t in tb if t in ALL_FORM_WORDS}
    if forms_a != forms_b:
        return False, price_ratio, [], [], [], "form_word"

    unmatched_a = list(ta)
    unmatched_b = list(tb)
    aligned_pairs: list[tuple[str, str]] = []

    for tok in list(unmatched_a):
        if tok in unmatched_b:
            aligned_pairs.append((tok, tok))
            unmatched_a.remove(tok)
            unmatched_b.remove(tok)

    for tok in list(unmatched_a):
        hit = next((cand for cand in unmatched_b if tokens_align(tok, cand)), None)
        if hit is not None:
            aligned_pairs.append((tok, hit))
            unmatched_a.remove(tok)
            unmatched_b.remove(hit)

    for tok in unmatched_a + unmatched_b:
        if tok in PROTECTED_MODIFIERS:
            return False, price_ratio, aligned_pairs, unmatched_a, unmatched_b, f"protected_mod:{tok}"

    if ca["main_category"] == "Топли пијалоци" or cb["main_category"] == "Топли пијалоци":
        if "инстант" in unmatched_a or "инстант" in unmatched_b:
            return False, price_ratio, aligned_pairs, unmatched_a, unmatched_b, "protected_mod:инстант_coffee"

    for tok in unmatched_a + unmatched_b:
        if tok in block_protected:
            return False, price_ratio, aligned_pairs, unmatched_a, unmatched_b, f"same_mkt_diff:{tok}"

    return True, price_ratio, aligned_pairs, unmatched_a, unmatched_b, ""


def score_cluster_pair(
    ca: dict,
    cb: dict,
    idf_map: dict[str, float],
    block_protected: set[str],
    max_price_ratio: float = 2.2,
) -> tuple[float, float, int, str]:
    """Compute (idf_sim, price_ratio, aligned_count, veto_reason) for Layers 2-4."""
    passed, price_ratio, aligned_pairs, unmatched_a, unmatched_b, veto = hard_veto_check(
        ca, cb, block_protected, max_price_ratio=max_price_ratio
    )
    if not passed:
        return 0.0, price_ratio, len(aligned_pairs), veto
    if not aligned_pairs:
        return 0.0, price_ratio, 0, "no_overlap"

    non_generic_aligned = sum(
        1 for x, y in aligned_pairs
        if x not in GENERIC_HEAD_NOUNS or y not in GENERIC_HEAD_NOUNS
    )
    if non_generic_aligned == 0 and (unmatched_a or unmatched_b):
        return 0.0, price_ratio, len(aligned_pairs), "only_generic_overlap"

    ta = [t for t in ca["content_tokens"] if t not in SOFT_TOKENS]
    tb = [t for t in cb["content_tokens"] if t not in SOFT_TOKENS]
    w_match = sum(0.5 * (idf_map.get(x, 2.0) + idf_map.get(y, 2.0)) for x, y in aligned_pairs)
    w_a = sum(idf_map.get(x, 2.0) for x in ta)
    w_b = sum(idf_map.get(y, 2.0) for y in tb)
    idf_sim = (2.0 * w_match) / (w_a + w_b) if (w_a + w_b) > 0 else 0.0

    if ca["median_price"] and cb["median_price"]:
        if price_ratio <= 1.15:
            idf_sim = min(1.0, idf_sim + 0.03)
        elif price_ratio >= 1.65:
            idf_sim = max(0.0, idf_sim - 0.05)

    return idf_sim, price_ratio, len(aligned_pairs), ""


def smart_merge_block(
    initial_components: list[list[dict]],
    brand_idf: dict[str, float],
    max_price_ratio: float = 1.80,
    mnn_max_price_ratio: float = 1.40,
) -> tuple[list[list[dict]], list[dict]]:
    """Run Layers 2-4 on initial Layer-1 clusters inside one (manufacturer, base_qty) block."""
    if len(initial_components) < 2:
        return initial_components, []

    clusters = [build_cluster_summary(i, comp) for i, comp in enumerate(initial_components)]

    all_block_markets = set().union(*(c["all_markets"] for c in clusters))
    if len(all_block_markets) < 2 or len(clusters) > 250:
        return initial_components, []

    block_protected = discover_same_market_differentiators(clusters)

    n_cl = len(clusters)
    df: Counter[str] = Counter()
    for c in clusters:
        for t in set(c["content_tokens"]):
            df[t] += 1
    idf_map: dict[str, float] = {}
    for t, cnt in df.items():
        local_idf = math.log((n_cl + 1.0) / (cnt + 1.0)) + 1.0
        b_idf = brand_idf.get(t, local_idf)
        w = 0.6 * local_idf + 0.4 * b_idf
        if t in GENERIC_HEAD_NOUNS:
            w *= 0.35
        idf_map[t] = w

    prefix_idx: dict[str, list[int]] = defaultdict(list)
    for c in clusters:
        seen_p = {t[:3] for t in c["content_tokens"] if len(t) >= 3 and t not in SOFT_TOKENS}
        for p in seen_p:
            prefix_idx[p].append(c["idx"])

    cand_pairs: set[tuple[int, int]] = set()
    for idxs in prefix_idx.values():
        if 2 <= len(idxs) <= 150:
            for i, j in itertools.combinations(idxs, 2):
                cand_pairs.add((min(i, j), max(i, j)))

    hard_pr_cap = max(2.2, max_price_ratio)
    pair_scores: dict[tuple[int, int], tuple[float, float, int]] = {}
    best_per_node_mkt: dict[tuple[int, str], list[tuple[float, int]]] = defaultdict(list)

    for i, j in cand_pairs:
        ca, cb = clusters[i], clusters[j]
        sim, pr, aligned, veto = score_cluster_pair(ca, cb, idf_map, block_protected, max_price_ratio=hard_pr_cap)
        if veto:
            continue
        if sim >= 0.65:
            pair_scores[(i, j)] = (sim, pr, aligned)
            for m in cb["all_markets"]:
                best_per_node_mkt[(i, m)].append((sim, j))
            for m in ca["all_markets"]:
                best_per_node_mkt[(j, m)].append((sim, i))

    for k in best_per_node_mkt:
        best_per_node_mkt[k].sort(reverse=True)

    qualifying: list[tuple[float, int, int, str, float]] = []
    for (i, j), (sim, pr, aligned) in pair_scores.items():
        ca, cb = clusters[i], clusters[j]
        min_toks = min(
            len([t for t in ca["content_tokens"] if t not in SOFT_TOKENS]),
            len([t for t in cb["content_tokens"] if t not in SOFT_TOKENS]),
        )

        if sim >= 0.84 and pr <= max_price_ratio and (aligned >= 2 or (aligned == 1 and min_toks == 1 and sim >= 0.92)):
            qualifying.append((sim, i, j, "idf_high", pr))
            continue

        if sim >= 0.72 and aligned >= 2 and pr <= mnn_max_price_ratio:
            is_mnn = False
            for ma in ca["all_markets"]:
                for mb in cb["all_markets"]:
                    if ma == mb:
                        continue
                    la = best_per_node_mkt.get((i, mb), [])
                    lb = best_per_node_mkt.get((j, ma), [])
                    if la and lb and la[0][1] == j and lb[0][1] == i:
                        margin_a = la[0][0] - (la[1][0] if len(la) > 1 else 0.0)
                        margin_b = lb[0][0] - (lb[1][0] if len(lb) > 1 else 0.0)
                        if min(margin_a, margin_b) >= 0.12:
                            is_mnn = True
                            break
                if is_mnn:
                    break
            if is_mnn:
                qualifying.append((sim, i, j, "mnn_margin", pr))

    qualifying.sort(key=lambda x: x[0], reverse=True)
    parent = {c["idx"]: c["idx"] for c in clusters}
    members: dict[int, list[int]] = {c["idx"]: [c["idx"]] for c in clusters}
    accepted: list[dict] = []

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for sim, i, j, rule, pr in qualifying:
        ri, rj = find(i), find(j)
        if ri == rj:
            continue
        ok = True
        for a_idx in members[ri]:
            for b_idx in members[rj]:
                _, _, _, veto = score_cluster_pair(
                    clusters[a_idx], clusters[b_idx], idf_map, block_protected, max_price_ratio=hard_pr_cap
                )
                if veto:
                    ok = False
                    break
            if not ok:
                break
        if not ok:
            continue

        parent[rj] = ri
        members[ri].extend(members[rj])
        del members[rj]
        accepted.append({
            "sim": round(sim, 3),
            "rule": rule,
            "pr": round(pr, 2),
            "a_raw": clusters[i]["rows"][0].get("name"),
            "b_raw": clusters[j]["rows"][0].get("name"),
            "a_mkt": clusters[i]["rows"][0].get("market"),
            "b_mkt": clusters[j]["rows"][0].get("market"),
            "a_price": clusters[i]["median_price"],
            "b_price": clusters[j]["median_price"],
        })

    final_components = [
        [row for idx in idxs for row in clusters[idx]["rows"]]
        for idxs in members.values()
    ]
    return final_components, accepted
