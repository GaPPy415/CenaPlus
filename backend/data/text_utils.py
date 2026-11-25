"""Shared text normalization helpers used by the API and data pipeline."""

import re

from cyrtranslit import to_cyrillic

_DECIMAL_RE = re.compile(r"(\d)\s*[.,]\s*(\d)")

# quantity/pack tokens, stripped from search queries because `groups.product_name`
# never contains them
_QUANTITY_TOKEN_RE = re.compile(
    r"\b\d+(?:[.,]\d+)?\s*(?:кг|kg|kgs|гр|г|g|grs|мл|ml|mls|л|l|lt|ком|пар|парчиња|pcs)\b\.?",
    re.IGNORECASE,
)


def strip_quantity_tokens(text: str | None) -> str | None:
    if not text:
        return None
    cleaned = _QUANTITY_TOKEN_RE.sub(" ", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def normalize_string(value: str | None) -> str | None:
    """Lowercase, transliterate to Cyrillic, flatten punctuation and spacing.

    Must stay identical to the rules used when product names were extracted,
    otherwise a search query will not match the stored `groups.product_name`.
    """
    if not value:
        return None
    text = str(value).strip().lower()
    # cyrtranslit leaves a few Latin letters untouched; map them first so the
    # transliteration is consistent across shops.
    text = text.replace("w", "в").replace("y", "и")
    text = to_cyrillic(text, "mk")
    text = text.replace("’", "'").replace("`", "'")
    text = _DECIMAL_RE.sub(r"\1.\2", text)          # 2,8 -> 2.8 (before , becomes space)
    text = re.sub(r"[^\w\s%.]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s*%", "%", text)                # keep a space AFTER the percent sign
    # dots are only meaningful between digits (2.8); elsewhere they are separators
    text = re.sub(r"(?<!\d)\.|\.(?!\d)", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" .")
    return text or None
