import argparse
import asyncio
import sys
from typing import Optional
from pydantic import BaseModel, Field
from google import genai
import json

from backend.data.RateLimiter import (
    TEXT_MODEL_RPM_LIMIT,
    TEXT_MODEL_SCOPE,
    TEXT_MODEL_TPM_LIMIT,
    call_with_retries,
    get_rate_limiter,
)
from backend.data.db_utils import *

from backend.data.db_utils import connect_to_db

load_dotenv(find_dotenv())

from backend.data.constants import CATEGORIES, CATEGORY_DESCRIPTIONS


class ProductMainCategory(BaseModel):
    """Single product main category categorization."""

    main_category: str = Field(description="Main category from taxonomy")
    confidence: float = Field(description="Confidence 0.0-1.0", ge=0.0, le=1.0)


class ProductSubCategory(BaseModel):
    """Single product subcategory categorization."""

    sub_category: str = Field(description="Subcategory belonging to main category")
    confidence: float = Field(description="Confidence 0.0-1.0", ge=0.0, le=1.0)


class ProductCategory(BaseModel):
    """Complete product categorization with both stages."""

    main_category: str = Field(description="Main category from taxonomy")
    sub_category: str = Field(description="Subcategory belonging to main category")
    main_confidence: float = Field(
        description="Main category confidence 0.0-1.0", ge=0.0, le=1.0
    )
    sub_confidence: float = Field(
        description="Subcategory confidence 0.0-1.0", ge=0.0, le=1.0
    )


class BatchMainResponse(BaseModel):
    products: List[ProductMainCategory]


class BatchSubResponse(BaseModel):
    products: List[ProductSubCategory]


# Shared with extract_features.py: same model, same account, same rolling window.
# The window survives the process boundary, so the extractor does not start from
# zero the moment this stage exits.
rate_limiter = get_rate_limiter(
    TEXT_MODEL_RPM_LIMIT, TEXT_MODEL_TPM_LIMIT, scope=TEXT_MODEL_SCOPE
)

RETRY_ATTEMPTS = 5

# gemini-2.0-flash / 2.5-flash-lite are no longer served to this project; 3.1-flash-lite
# is the cheapest lite tier that still responds. Override with GEMINI_MODEL if needed.
DEFAULT_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite")


def estimate_tokens_main_category(products: List[dict]) -> int:
    system_tokens = 300
    categories_tokens = 200
    user_content = "\n\n".join(
        [
            f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
            for i, p in enumerate(products)
        ]
    )
    user_tokens = len(user_content) // 4
    output_tokens = len(products) * 60
    return system_tokens + categories_tokens + user_tokens + output_tokens


def estimate_tokens_sub_category(products: List[dict]) -> int:
    system_tokens = 250
    subcategories_tokens = 100
    user_content = "\n\n".join(
        [
            f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
            for i, p in enumerate(products)
        ]
    )
    user_tokens = len(user_content) // 4
    output_tokens = len(products) * 60
    return system_tokens + subcategories_tokens + user_tokens + output_tokens


def create_main_category_prompt() -> str:
    categories_block = "\n".join(
        [f"- {cat}: {desc}" for cat, desc in CATEGORY_DESCRIPTIONS.items()]
    )
    return f"""You are a product categorization expert for Macedonian supermarkets.
Categorize each product into ONE main category from the list below.

CATEGORIES:
{categories_block}

RULES:
1. Choose the most specific and relevant main category
2. If multiple categories seem to fit, choose based on the product's PRIMARY use case
3. Pay close attention to the NOT/ONLY notes in each category description
4. Confidence: 0.9-1.0 clear match, 0.7-0.89 good match, 0.5-0.69 uncertain, <0.5 needs review
5. Return categorizations IN THE SAME ORDER as input products
6. Select ONLY from the provided category names. Do not invent categories."""


def create_sub_category_prompt(main_category: str) -> str:
    subcategories = ", ".join(CATEGORIES.get(main_category, []))
    return f"""You are a product categorization expert for Macedonian supermarkets.
Products already classified as: {main_category}

Categorize each product into ONE subcategory from this list:

{subcategories}

RULES:
1. Choose most specific and relevant subcategory
2. If multiple fit, choose primary use case
3. Confidence: 0.9-1.0 clear, 0.7-0.89 good, 0.5-0.69 uncertain, <0.5 needs review
4. Return categorizations IN THE SAME ORDER as input products
5. Select ONLY from the provided list. Do not invent categories."""


async def categorize_batch_main_category(
    products_chunk: List[dict],
    client: genai.Client,
    model_id: str,
    estimated_tokens: int,
) -> List[Optional[ProductMainCategory]]:
    """Categorize one batch, returning ``None`` for products the model dropped.

    A ``None`` slot means "not categorized", *not* "low confidence": the caller
    must skip the product so a later run retries it. Filling the slot with
    ``Разно``/0.0 hid whole minutes of 429s behind plausible-looking data.
    """
    products_text = "\n\n".join(
        [
            f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
            for i, p in enumerate(products_chunk)
        ]
    )

    prompt = f"{create_main_category_prompt()}\n\n{products_text}"

    response = await call_with_retries(
        lambda: asyncio.to_thread(
            client.models.generate_content,
            model=model_id,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=0.1,
                response_mime_type="application/json",
                response_schema=BatchMainResponse,
            ),
        ),
        rate_limiter=rate_limiter,
        estimated_tokens=estimated_tokens,
        attempts=RETRY_ATTEMPTS,
        label="categorize-main",
    )

    result = BatchMainResponse(**json.loads(response.text))
    products: List[Optional[ProductMainCategory]] = list(result.products[: len(products_chunk)])
    if len(products) < len(products_chunk):
        print(
            f"   ! main batch: model returned {len(products)}/{len(products_chunk)} results; "
            f"{len(products_chunk) - len(products)} left for retry"
        )
        products.extend([None] * (len(products_chunk) - len(products)))
    return products


async def categorize_batch_sub_category(
    products_chunk: List[dict],
    main_category: str,
    client: genai.Client,
    model_id: str,
    estimated_tokens: int,
) -> List[Optional[ProductSubCategory]]:
    """Categorize one batch, returning ``None`` for products the model dropped."""
    products_text = "\n\n".join(
        [
            f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
            for i, p in enumerate(products_chunk)
        ]
    )

    prompt = f"{create_sub_category_prompt(main_category)}\n\n{products_text}"

    response = await call_with_retries(
        lambda: asyncio.to_thread(
            client.models.generate_content,
            model=model_id,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=0.1,
                response_mime_type="application/json",
                response_schema=BatchSubResponse,
            ),
        ),
        rate_limiter=rate_limiter,
        estimated_tokens=estimated_tokens,
        attempts=RETRY_ATTEMPTS,
        label="categorize-sub",
    )

    result = BatchSubResponse(**json.loads(response.text))
    products: List[Optional[ProductSubCategory]] = list(result.products[: len(products_chunk)])
    if len(products) < len(products_chunk):
        print(
            f"   ! sub batch ({main_category}): model returned {len(products)}/{len(products_chunk)} "
            f"results; {len(products_chunk) - len(products)} left for retry"
        )
        products.extend([None] * (len(products_chunk) - len(products)))
    return products


async def categorize_all_products(
    products: List[dict],
    batch_size: int = 32,
    concurrency: int = 1,
    gemini_api_key: str = None,
    model_id: str = None,
) -> List[dict]:
    if not gemini_api_key:
        gemini_api_key = os.getenv("GOOGLE_API_KEY")
        if not gemini_api_key:
            raise ValueError("GOOGLE_API_KEY not found in environment or arguments")

    client = genai.Client(api_key=gemini_api_key)
    model_id = model_id or DEFAULT_MODEL

    print(f"🚀 Starting TWO-STAGE categorization of {len(products)} products")
    print(f"   Model: {model_id}, batch size: {batch_size}, concurrency: {concurrency}")
    print(f"   Rate limit budget: {rate_limiter.rpm_limit:,} RPM / {rate_limiter.tpm_limit:,} TPM")
    print()

    batches = [
        products[i : i + batch_size] for i in range(0, len(products), batch_size)
    ]
    semaphore = asyncio.Semaphore(concurrency)
    completed = 0
    start_time = time.time()
    failures = {"main": 0, "sub": 0}

    # Stage 1: Main categories
    print("📍 Stage 1: Categorizing main categories...")
    main_categorizations: dict = {}

    async def process_main_batch(batch_idx: int, batch: List[dict]):
        nonlocal completed
        async with semaphore:
            estimated_tokens = estimate_tokens_main_category(batch)
            try:
                main_cats = await categorize_batch_main_category(
                    batch, client, model_id, estimated_tokens
                )
            except Exception as exc:  # noqa: BLE001 - one bad batch must not kill the run
                failures["main"] += len(batch)
                completed += len(batch)
                print(f"   x main batch failed ({len(batch)} products, left for retry): {exc}")
                return
            for product, cat in zip(batch, main_cats):
                main_categorizations[product["id"]] = cat
            failures["main"] += sum(1 for cat in main_cats if cat is None)
            completed += len(batch)
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            print(
                f"   {completed:,}/{len(products):,} ({completed * 100 // len(products)}%) | {rate:.1f} products/sec"
            )

    tasks = [process_main_batch(i, batch) for i, batch in enumerate(batches)]
    await asyncio.gather(*tasks)

    # Stage 2: Group by main category and categorize subcategories
    print("\n📍 Stage 2: Categorizing subcategories...")
    completed = 0
    products_by_main_cat = {}
    for product in products:
        main_result = main_categorizations.get(product["id"])
        if main_result is None:
            continue
        main_cat = main_result.main_category
        if main_cat not in products_by_main_cat:
            products_by_main_cat[main_cat] = []
        products_by_main_cat[main_cat].append(product)

    all_sub_results: dict = {}

    async def process_sub_batch(batch_idx: int, batch: List[dict], main_cat: str):
        nonlocal completed
        start_sub = time.time()
        async with semaphore:
            estimated_tokens = estimate_tokens_sub_category(batch)
            try:
                sub_cats = await categorize_batch_sub_category(
                    batch, main_cat, client, model_id, estimated_tokens
                )
            except Exception as exc:  # noqa: BLE001 - one bad batch must not kill the run
                failures["sub"] += len(batch)
                completed += len(batch)
                print(f"   x sub batch failed ({main_cat}, {len(batch)} products, left for retry): {exc}")
                return
            for product, cat in zip(batch, sub_cats):
                all_sub_results[product["id"]] = cat
            failures["sub"] += sum(1 for cat in sub_cats if cat is None)
            completed += len(batch)
            elapsed = time.time() - start_sub
            rate = completed / elapsed if elapsed > 0 else 0
            print(
                f"   {completed:,}/{len(products):,} ({completed * 100 // len(products)}%) | {rate:.1f} products/sec"
            )

    tasks = []
    for main_cat, products_in_cat in products_by_main_cat.items():
        sub_batches = [
            products_in_cat[i : i + batch_size]
            for i in range(0, len(products_in_cat), batch_size)
        ]
        for batch_idx, batch in enumerate(sub_batches):
            tasks.append(process_sub_batch(batch_idx, batch, main_cat))

    await asyncio.gather(*tasks)

    # Combine results. Products missing either stage keep no categorization, so
    # they stay in the queue for the next run instead of being stored as 0.0.
    categorized = 0
    for product in products:
        main_result = main_categorizations.get(product["id"])
        sub_result = all_sub_results.get(product["id"])
        if main_result is None or sub_result is None:
            continue
        product["categorization"] = ProductCategory(
            main_category=main_result.main_category,
            sub_category=sub_result.sub_category,
            main_confidence=main_result.confidence,
            sub_confidence=sub_result.confidence,
        ).model_dump()
        categorized += 1

    failed = len(products) - categorized
    elapsed = time.time() - start_time
    stats = rate_limiter.get_stats()

    print()
    print("=" * 70)
    print(f"✅ Categorization complete!")
    print(f"   Total products: {len(products):,}")
    print(f"   Categorized: {categorized:,}")
    print(f"   Failed (left for retry): {failed:,} "
          f"(main stage: {failures['main']:,}, sub stage: {failures['sub']:,})")
    print(f"   Total time: {elapsed / 60:.2f} minutes")
    print(f"   Average rate: {len(products) / elapsed:.1f} products/sec")
    print(f"   Total API requests: {stats['total_requests']:,}")
    print(f"   Total tokens used: {stats['total_tokens']:,}")
    print(f"   Budget used: {stats['rpm_limit']:,} RPM / {stats['tpm_limit']:,} TPM")
    print("=" * 70)

    return products


async def main():
    parser = argparse.ArgumentParser(description="Categorize products with the Gemini text model.")
    parser.add_argument("--limit", type=int, default=None, help="Categorize at most N pending products")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=128)
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    args = parser.parse_args()

    # Windows consoles default to cp1252; Macedonian output and symbols need UTF-8.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    gemini_api_key = os.getenv("GOOGLE_API_KEY")
    if not gemini_api_key:
        print("❌ ERROR: GOOGLE_API_KEY not found!")
        return

    db = connect_to_db()

    products = load_products_to_categorize(db, limit=args.limit)

    if not products:
        print("✅ No products need categorization!")
        db.close()
        return

    categorized_products = await categorize_all_products(
        products,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        gemini_api_key=gemini_api_key,
        model_id=args.model,
    )

    # Failed products have no ``categorization`` key; never write partial or
    # invented results, so the next run picks them up again.
    successful = [p for p in categorized_products if p.get("categorization")]
    failed = len(categorized_products) - len(successful)

    if successful:
        save_categorizations_to_db(db, successful)
    else:
        print("\n⚠️  Nothing categorized — database left untouched.")

    print("\n📈 Categorization Quality Analysis:")
    confidence_ranges = {
        "High (0.9-1.0)": 0,
        "Good (0.7-0.89)": 0,
        "Medium (0.5-0.69)": 0,
        "Low (<0.5)": 0,
    }

    for p in successful:
        cat = p["categorization"]
        sub_conf = cat.get("sub_confidence", 0)

        if sub_conf >= 0.9:
            confidence_ranges["High (0.9-1.0)"] += 1
        elif sub_conf >= 0.7:
            confidence_ranges["Good (0.7-0.89)"] += 1
        elif sub_conf >= 0.5:
            confidence_ranges["Medium (0.5-0.69)"] += 1
        else:
            confidence_ranges["Low (<0.5)"] += 1

    total = len(successful) or 1
    for range_name, count in confidence_ranges.items():
        print(f"    {range_name}: {count:,} ({count / total * 100:.1f}%)")
    print(f"    Failed / not stored: {failed:,} ({failed / len(categorized_products) * 100:.1f}%)")

    print("\n📋 Sample categorizations:")
    for i, p in enumerate(successful[:20]):
        cat = p["categorization"]
        print(f"\n{i + 1}. {p['name'][:60]}")
        print(f"   → {cat['main_category']} / {cat['sub_category']}")
        print(f"   Confidence: {cat.get('sub_confidence', 0):.2f}")

    db.close()
    print("\n✅ All done!")


if __name__ == "__main__":
    asyncio.run(main())
