import asyncio
import time
import os
from typing import List, Optional
from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, Field
from google import genai
import json

from backend.RateLimiter import RateLimiter
from backend.db_utils import *

from backend.db_utils import connect_to_db

load_dotenv(find_dotenv())

from constants import CATEGORIES, TAXONOMY_COMPRESSED

class ProductMainCategory(BaseModel):
    """Single product main category categorization."""
    main_category: str = Field(description="Main category from taxonomy")
    confidence: float = Field(description="Confidence 0.0-1.0", ge=0.0, le=1.0)
    reasoning: Optional[str] = Field(default=None, description="Brief explanation")

class ProductSubCategory(BaseModel):
    """Single product subcategory categorization."""
    sub_category: str = Field(description="Subcategory belonging to main category")
    confidence: float = Field(description="Confidence 0.0-1.0", ge=0.0, le=1.0)
    reasoning: Optional[str] = Field(default=None, description="Brief explanation")

class ProductCategory(BaseModel):
    """Complete product categorization with both stages."""
    main_category: str = Field(description="Main category from taxonomy")
    sub_category: str = Field(description="Subcategory belonging to main category")
    main_confidence: float = Field(description="Main category confidence 0.0-1.0", ge=0.0, le=1.0)
    sub_confidence: float = Field(description="Subcategory confidence 0.0-1.0", ge=0.0, le=1.0)
    main_reasoning: Optional[str] = Field(default=None, description="Main category reasoning")
    sub_reasoning: Optional[str] = Field(default=None, description="Subcategory reasoning")

class BatchMainCategories(BaseModel):
    """Multiple main category categorizations in a single response."""
    products: List[ProductMainCategory] = Field(description="List of main category categorizations in order")

class BatchSubCategories(BaseModel):
    """Multiple subcategory categorizations in a single response."""
    products: List[ProductSubCategory] = Field(description="List of subcategory categorizations in order")

rate_limiter = RateLimiter(rpm_limit=1900, tpm_limit=3800000)

def estimate_tokens_main_category(products: List[dict]) -> int:
    system_tokens = 300
    categories_tokens = 200
    user_content = "\n\n".join([
        f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products)
    ])
    user_tokens = len(user_content) // 4
    output_tokens = len(products) * 60
    return system_tokens + categories_tokens + user_tokens + output_tokens

def estimate_tokens_sub_category(products: List[dict]) -> int:
    system_tokens = 250
    subcategories_tokens = 100
    user_content = "\n\n".join([
        f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products)
    ])
    user_tokens = len(user_content) // 4
    output_tokens = len(products) * 60
    return system_tokens + subcategories_tokens + user_tokens + output_tokens

def create_main_category_prompt() -> str:
    main_categories = ", ".join(CATEGORIES.keys())
    return f"""You are a product categorization expert for Macedonian supermarkets.
Categorize ALL products into ONE main category from this list:

{main_categories}

RULES:
1. Choose most specific and relevant main category
2. If multiple fit, choose primary use case
3. Confidence: 0.9-1.0 clear, 0.7-0.89 good, 0.5-0.69 uncertain, <0.5 needs review
4. Return categorizations IN THE SAME ORDER as input products
5. Reasoning must be brief (1 sentence)

Return valid JSON:
{{
  "products": [
    {{
      "main_category": "string",
      "confidence": 0.0-1.0,
      "reasoning": "string"
    }}
  ]
}}

MUST select ONLY from the provided list. Do not invent categories."""

def create_sub_category_prompt(main_category: str) -> str:
    subcategories = ", ".join(CATEGORIES.get(main_category, []))
    return f"""You are a product categorization expert for Macedonian supermarkets.
Products already classified as: {main_category}

Categorize ALL products into ONE subcategory from this list:

{subcategories}

RULES:
1. Choose most specific and relevant subcategory
2. If multiple fit, choose primary use case
3. Confidence: 0.9-1.0 clear, 0.7-0.89 good, 0.5-0.69 uncertain, <0.5 needs review
4. Return categorizations IN THE SAME ORDER as input products
5. Reasoning must be brief (1 sentence)

Return valid JSON:
{{
  "products": [
    {{
      "sub_category": "string",
      "confidence": 0.0-1.0,
      "reasoning": "string"
    }}
  ]
}}

MUST select ONLY from the provided list. Do not invent categories."""

async def categorize_batch_main_category(
        products_chunk: List[dict],
        client: genai.Client,
        model_id: str
) -> List[ProductMainCategory]:
    products_text = "\n\n".join([
        f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products_chunk)
    ])

    prompt = f"{create_main_category_prompt()}\n\n{products_text}"

    try:
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=model_id,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=0.1,
                response_mime_type="application/json"
            )
        )

        result_data = json.loads(response.text)

        if "products" in result_data:
            for product_dict in result_data["products"]:
                if "confidence" not in product_dict:
                    product_dict["confidence"] = 0.5
                if "reasoning" not in product_dict:
                    product_dict["reasoning"] = ""
                if "main_category" not in product_dict:
                    product_dict["main_category"] = "Разно"

        result = BatchMainCategories(**result_data)

        if len(result.products) != len(products_chunk):
            while len(result.products) < len(products_chunk):
                result.products.append(ProductMainCategory(
                    main_category="Разно",
                    confidence=0.0,
                    reasoning="Missing from batch response"
                ))

        return result.products[:len(products_chunk)]

    except json.JSONDecodeError as e:
        return [
            ProductMainCategory(
                main_category="Разно",
                confidence=0.0,
                reasoning=f"JSON parse error: {str(e)}"
            )
            for _ in products_chunk
        ]
    except Exception as e:
        return [
            ProductMainCategory(
                main_category="Разно",
                confidence=0.0,
                reasoning=f"Error: {str(e)}"
            )
            for _ in products_chunk
        ]

async def categorize_batch_sub_category(
        products_chunk: List[dict],
        main_category: str,
        client: genai.Client,
        model_id: str
) -> List[ProductSubCategory]:
    products_text = "\n\n".join([
        f"Product {i + 1}:\nName: {p.get('name', '')}\nDescription: {p.get('description', 'Нема опис')}\nSource: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products_chunk)
    ])

    prompt = f"{create_sub_category_prompt(main_category)}\n\n{products_text}"

    try:
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=model_id,
            contents=prompt,
            config=genai.types.GenerateContentConfig(
                temperature=0.1,
                response_mime_type="application/json"
            )
        )

        result_data = json.loads(response.text)

        if "products" in result_data:
            for product_dict in result_data["products"]:
                if "confidence" not in product_dict:
                    product_dict["confidence"] = 0.5
                if "reasoning" not in product_dict:
                    product_dict["reasoning"] = ""
                if "sub_category" not in product_dict:
                    sub_cats = CATEGORIES.get(main_category, [])
                    product_dict["sub_category"] = sub_cats[0] if sub_cats else "Останато"

        result = BatchSubCategories(**result_data)

        if len(result.products) != len(products_chunk):
            sub_cats = CATEGORIES.get(main_category, [])
            default_sub = sub_cats[0] if sub_cats else "Останато"
            while len(result.products) < len(products_chunk):
                result.products.append(ProductSubCategory(
                    sub_category=default_sub,
                    confidence=0.0,
                    reasoning="Missing from batch response"
                ))

        return result.products[:len(products_chunk)]

    except json.JSONDecodeError as e:
        sub_cats = CATEGORIES.get(main_category, [])
        default_sub = sub_cats[0] if sub_cats else "Останато"
        return [
            ProductSubCategory(
                sub_category=default_sub,
                confidence=0.0,
                reasoning=f"JSON parse error: {str(e)}"
            )
            for _ in products_chunk
        ]
    except Exception as e:
        sub_cats = CATEGORIES.get(main_category, [])
        default_sub = sub_cats[0] if sub_cats else "Останато"
        return [
            ProductSubCategory(
                sub_category=default_sub,
                confidence=0.0,
                reasoning=f"Error: {str(e)}"
            )
            for _ in products_chunk
        ]


async def categorize_all_products(
        products: List[dict],
        batch_size: int = 32,
        concurrency: int = 1,
        gemini_api_key: str = None
) -> List[dict]:
    if not gemini_api_key:
        gemini_api_key = os.getenv("GOOGLE_API_KEY")
        if not gemini_api_key:
            raise ValueError("GOOGLE_API_KEY not found in environment or arguments")

    client = genai.Client(api_key=gemini_api_key)
    model_id = 'gemini-2.0-flash'

    print(f"🚀 Starting TWO-STAGE categorization of {len(products)} products")
    print(f"   Batch size: {batch_size}, Concurrency: {concurrency}")
    print()

    batches = [products[i:i + batch_size] for i in range(0, len(products), batch_size)]
    semaphore = asyncio.Semaphore(concurrency)
    completed = 0
    start_time = time.time()

    # Stage 1: Main categories
    print("📍 Stage 1: Categorizing main categories...")
    main_categorizations = {}

    async def process_main_batch(batch_idx: int, batch: List[dict]):
        nonlocal completed
        async with semaphore:
            estimated_tokens = estimate_tokens_main_category(batch)
            await rate_limiter.acquire(estimated_tokens)
            main_cats = await categorize_batch_main_category(batch, client, model_id)
            for product, cat in zip(batch, main_cats):
                main_categorizations[product['id']] = cat
            completed += len(batch)
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            print(f"   {completed:,}/{len(products):,} ({completed * 100 // len(products)}%) | {rate:.1f} products/sec")

    tasks = [process_main_batch(i, batch) for i, batch in enumerate(batches)]
    await asyncio.gather(*tasks)

    # Stage 2: Group by main category and categorize subcategories
    print("\n📍 Stage 2: Categorizing subcategories...")
    completed = 0
    products_by_main_cat = {}
    for product in products:
        main_cat = main_categorizations[product['id']].main_category
        if main_cat not in products_by_main_cat:
            products_by_main_cat[main_cat] = []
        products_by_main_cat[main_cat].append(product)

    all_sub_results = {}

    async def process_sub_batch(batch_idx: int, batch: List[dict], main_cat: str):
        nonlocal completed
        async with semaphore:
            estimated_tokens = estimate_tokens_sub_category(batch)
            await rate_limiter.acquire(estimated_tokens)
            sub_cats = await categorize_batch_sub_category(batch, main_cat, client, model_id)
            for product, cat in zip(batch, sub_cats):
                all_sub_results[product['id']] = cat
            completed += len(batch)
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            print(f"   {completed:,}/{len(products):,} ({completed * 100 // len(products)}%) | {rate:.1f} products/sec")

    tasks = []
    for main_cat, products_in_cat in products_by_main_cat.items():
        sub_batches = [products_in_cat[i:i + batch_size] for i in range(0, len(products_in_cat), batch_size)]
        for batch_idx, batch in enumerate(sub_batches):
            tasks.append(process_sub_batch(batch_idx, batch, main_cat))

    await asyncio.gather(*tasks)

    # Combine results
    for product in products:
        main_result = main_categorizations[product['id']]
        sub_result = all_sub_results[product['id']]
        product['categorization'] = ProductCategory(
            main_category=main_result.main_category,
            sub_category=sub_result.sub_category,
            main_confidence=main_result.confidence,
            sub_confidence=sub_result.confidence,
            main_reasoning=main_result.reasoning,
            sub_reasoning=sub_result.reasoning
        ).model_dump()

    elapsed = time.time() - start_time
    stats = rate_limiter.get_stats()

    print()
    print("=" * 70)
    print(f"✅ Categorization complete!")
    print(f"   Total products: {len(products):,}")
    print(f"   Total time: {elapsed / 60:.2f} minutes")
    print(f"   Average rate: {len(products) / elapsed:.1f} products/sec")
    print(f"   Total API requests: {stats['total_requests']:,}")
    print(f"   Total tokens used: {stats['total_tokens']:,}")
    print("=" * 70)

    return products


async def main():
    gemini_api_key = os.getenv("GOOGLE_API_KEY")
    if not gemini_api_key:
        print("❌ ERROR: GOOGLE_API_KEY not found!")
        return

    db = connect_to_db('products_categorized')

    products, products_markets = load_products_to_categorize(
        db,
        limit_per_table=None
    )

    if not products:
        print("✅ No products need categorization!")
        db.close()
        return

    categorized_products = await categorize_all_products(
        products,
        batch_size=16,
        concurrency=64,
        gemini_api_key=gemini_api_key
    )

    save_categorizations_to_db(db, categorized_products, products_markets)

    print("\n📈 Categorization Quality Analysis:")
    confidence_ranges = {
        'High (0.9-1.0)': 0,
        'Good (0.7-0.89)': 0,
        'Medium (0.5-0.69)': 0,
        'Low (<0.5)': 0,
        'Errors': 0
    }

    for p in categorized_products:
        cat = p['categorization']
        sub_conf = cat.get('sub_confidence', 0)

        if cat.get('sub_category') is None:
            confidence_ranges['Errors'] += 1
        elif sub_conf >= 0.9:
            confidence_ranges['High (0.9-1.0)'] += 1
        elif sub_conf >= 0.7:
            confidence_ranges['Good (0.7-0.89)'] += 1
        elif sub_conf >= 0.5:
            confidence_ranges['Medium (0.5-0.69)'] += 1
        else:
            confidence_ranges['Low (<0.5)'] += 1

    for range_name, count in confidence_ranges.items():
        pct = (count / len(categorized_products) * 100) if categorized_products else 0
        print(f"    {range_name}: {count:,} ({pct:.1f}%)")

    print("\n📋 Sample categorizations:")
    for i, p in enumerate(categorized_products[:20]):
        cat = p['categorization']
        print(f"\n{i + 1}. {p['name'][:60]}")
        print(f"   → {cat['main_category']} / {cat['sub_category']}")
        print(f"   Confidence: {cat.get('sub_confidence', 0):.2f}")
        if cat.get('sub_reasoning'):
            print(f"   Reasoning: {cat['sub_reasoning'][:80]}")

    print("\n✅ All done!")


if __name__ == "__main__":
    asyncio.run(main())