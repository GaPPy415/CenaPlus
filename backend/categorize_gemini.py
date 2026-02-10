import asyncio
import time
import os
from typing import List, Optional
from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, Field
from google import genai
import json

from backend.RateLimiter import RateLimiter
from backend.db_utils import load_products_to_categorize, save_categorizations_to_db

from backend.db_utils import connect_to_db

load_dotenv(find_dotenv())

CATEGORIES = {
    "Намирници": ['Брашно', 'Додатоци за јадења', 'Додатоци за конзервирање', 'Готови оброци', 'Јајца',
                  'Кечап и сос од домати', 'Квасец', 'Мајонез, сенф, рен, преливи, сосови', 'Маргарин',
                  'Мешавина од зачини', 'Зрнести производи', 'Пудинг и шлаг', 'Шеќер', 'Оцет', 'Сол', 'Пекара',
                  'Супи и чорби', 'Сè за торти и колачи', 'Тестенини', 'Тортиљи', 'Масло', 'Зачини', 'Пире'],
    "Здрава храна": ['Безглутенски производи', 'Растителни напитоци', 'Експандиран ориз', 'Интегрален ориз',
                     'Интегрални колачи', 'Мед', 'Овесна каша', 'Снегулки и мусли', 'Солени грицки', 'Шумливи таблети',
                     'Семки и семки со лушпа', 'Суви овошја', 'Засладувачи', 'Здрави намази',
                     'Здрави грицки и пијалоци', 'Двопек', 'Протеин од сурутка', 'Зрнести производи'],
    "Млечни производи": ['Млеко', 'Јогурт', 'Путер', 'Млечни десерти', 'Млечни пијалоци', 'Кисело млеко',
                         'Кисела павлака', 'Грчки јогурт', 'Сирења', 'Намази од сирење', 'Сурутка',
                         'Преработено сирење', 'Овошен јогурт'],
    "Овошје и зеленчук": ['Зеленчук', 'Овошје', 'Конзервиран зеленчук', 'Конзервирано овошје'],
    "Месо и риба": ['Колбаси', 'Конзервирани производи', 'Намази', 'Паштета', 'Сувомеснато и процесирано месо',
                    'Салама', 'На тенки парчиња', 'Свежа риба', 'Свежо месо', 'Виршли и колбасици'],
    "Замрзнато": ['Готови сладоледи', 'Риба и морска храна', 'Замрзнат зеленчук', 'Замрзнато тесто и пецива',
                  'Замрзнато овошје', 'Замрзнато месо', 'Пици', 'Готови оброци'],
    "Пијалоци": ['Вода', 'Кафе', 'Газирани сокови', 'Капсули за кафе', 'Енергетски пијалоци', 'Чаеви', 'Ладени чаеви',
                 'Негазирани сокови'],
    "Алкохолни пијалоци": ['Пиво', 'Јаки алкохолни пијалоци', 'Вино', 'Витамински пијалоци', 'Квас', 'Коктел',
                           'Шампањско и пенливо вино'],
    "Слатки и грицки": ['Бонбони', 'Бонбоњера', 'Чоколади', 'Чоколадни барови', 'Десерти', 'Додатоци за млеко',
                        'Грицки', 'Кекс, вафли, бисквит', 'Кремови', 'Кроасани', 'Наполитанки', 'Ролати',
                        'Гуми за џвакање', 'Слатки намази', 'Диетални и здрави слатки'],
    "Лична хигиена и козметика": ['Сапуни', 'Чистење на лицето', 'Бричеви', 'Боја за коса', 'Гелови за туширање',
                                  'Хигиена за жени', 'Дезодоранси', 'Нега за коса', 'Нега на лице', 'Нега на раце',
                                  'Нега на стапала', 'Нега за тело', 'Орална хигиена', 'Хартија конфекција',
                                  'Препарати за сончање', 'Стик и рол-он', 'Сетови за поклон', 'Лабело',
                                  'Производи за бричење', 'Стапчиња за уши', 'Кондоми', 'Парфеми'],
    "Домашна хемија": ['Детергент за садови', 'Дополнителна нега на алишта', 'Инсектициди', 'Капсули за перење алишта',
                       'Марамчиња за перење алишта', 'Омекнувач за алишта', 'Прашок за перење алишта',
                       'Течни детергенти за перење алишта', 'Освежувачи на простор', 'Машинско миење садови',
                       'Средства за чистење', 'Средства за чистење на домаќинство', 'Средства за чистење на санитарии',
                       'Нега на обувки', 'Освежувачи на тоалет', 'Опрема за чистење'],
    "Катче за бебиња": ['Детска хигиена', 'Храна за бебиња', 'Каша за деца', 'Пијалоци', 'Пелени',
                        'Замена за млеко за деца', 'Играчки'],
    "Домашни миленици": ['Антипаразитски лекови', 'Влажна храна за мачки', 'Влажна храна за кучиња', 'Грицки за мачки',
                         'Грицки за кучиња', 'Сува храна за мачки', 'Сува храна за кучиња', 'Играчки за миленици', 'Останато'],
    "Дом и градина": ['Салфети', 'Кујнски прибор и садови', 'Сијалици', 'Батерии', 'Супер лепак', 'Чепкалки за заби', 'Свеќи'],
    "Цигари": ['Цигари и никотински производи'],
    "Облека": ['Облека за жени', 'Облека за мажи', 'Облека за деца', 'Чорапи и хулахопки', 'Пижами и долна облека'],
    "Разно": ['Останато', 'Плажа и базен', 'Канцелариски материјал', 'Текстил за дома', 'Кеси и фолија']
}

TAXONOMY_COMPRESSED = "\n".join([
    f"{main}: {', '.join(subs)}"
    for main, subs in CATEGORIES.items()
])

class ProductCategory(BaseModel):
    """Single product categorization."""
    main_category: str = Field(description="Main category from taxonomy")
    sub_category: str = Field(description="Subcategory belonging to main category")
    confidence: float = Field(description="Confidence 0.0-1.0", ge=0.0, le=1.0)
    reasoning: Optional[str] = Field(default=None, description="Brief explanation")

class BatchProductCategories(BaseModel):
    """Multiple product categorizations in a single response."""
    products: List[ProductCategory] = Field(description="List of categorizations in order")

rate_limiter = RateLimiter(rpm_limit=1900, tpm_limit=3800000)

def estimate_tokens(products: List[dict]) -> int:
    """
    Estimate tokens for a batch of products.
    Approximation: 1 token ≈ 4 characters for Macedonian text.
    """
    # System prompt + taxonomy
    system_tokens = 350
    taxonomy_tokens = 800

    # User content for all products
    user_content = "\n\n".join([
        f"Product {i + 1}:\nName: {p.get('name', '')}\n"
        f"Description: {p.get('description', 'Нема опис')}\n"
        f"Source: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products)
    ])

    user_tokens = len(user_content) // 4
    output_tokens = len(products) * 100

    return system_tokens + taxonomy_tokens + user_tokens + output_tokens

def create_system_prompt() -> str:
    """Create system prompt for batch categorization."""
    return f"""You are a product categorization expert for Macedonian supermarkets.

Categorize ALL products below into ONE main category and ONE subcategory from this taxonomy:

{TAXONOMY_COMPRESSED}

RULES:
1. Choose most specific and relevant category
2. If multiple categories fit, choose primary use case
3. Confidence scoring:
   - 0.9-1.0: Clear match (e.g., "Млеко" → Млечни производи/Млеко)
   - 0.7-0.89: Good match, minor ambiguity
   - 0.5-0.69: Multiple options, chose most likely
   - <0.5: Uncertain, needs review
4. Subcategory MUST belong to chosen main category
5. Return categorizations IN THE SAME ORDER as input products

Keep reasoning brief (1 sentence).

IMPORTANT: You MUST return a valid JSON object with this EXACT structure:
{{
  "products": [
    {{
      "main_category": "category name from taxonomy",
      "sub_category": "subcategory name from taxonomy",
      "confidence": 0.95,
      "reasoning": "brief explanation"
    }}
  ]
}}

ALL fields (main_category, sub_category, confidence, reasoning) are REQUIRED for each product."""

async def categorize_batch_gemini(
        products_chunk: List[dict],
        client: genai.Client,
        model_id: str
) -> List[ProductCategory]:
    """
    Categorize a batch of products using Gemini 2.0 Flash.

    Args:
        products_chunk: List of products to categorize in one request
        client: Configured Gemini client
        model_id: Model identifier

    Returns:
        List of ProductCategory objects
    """
    products_text = "\n\n".join([
        f"Product {i + 1}:\n"
        f"Name: {p.get('name', '')}\n"
        f"Description: {p.get('description', 'Нема опис')}\n"
        f"Source categories: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products_chunk)
    ])

    prompt = f"{create_system_prompt()}\n\n{products_text}"

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

        # Parse JSON response
        result_data = json.loads(response.text)

        # Fix any products missing required fields
        if "products" in result_data:
            for product_dict in result_data["products"]:
                if "sub_category" not in product_dict:
                    product_dict["sub_category"] = "Останато"
                if "confidence" not in product_dict:
                    product_dict["confidence"] = 0.5
                if "reasoning" not in product_dict:
                    product_dict["reasoning"] = "Auto-filled missing field"
                if "main_category" not in product_dict:
                    product_dict["main_category"] = "Разно"

        # Parse into Pydantic models
        result = BatchProductCategories(**result_data)

        # Validate we got the right number of results
        if len(result.products) != len(products_chunk):
            print(f"⚠️  Warning: Expected {len(products_chunk)} results, got {len(result.products)}")

            # Pad with error entries if needed
            while len(result.products) < len(products_chunk):
                result.products.append(ProductCategory(
                    main_category="Разно",
                    sub_category="Останато",
                    confidence=0.0,
                    reasoning="Missing from batch response"
                ))

        return result.products[:len(products_chunk)]

    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error: {e}")
        try:
            print(f"Response text: {response.text[:500]}")
        except:
            print("Response not available")
        return [
            ProductCategory(
                main_category="Разно",
                sub_category="Останато",
                confidence=0.0,
                reasoning=f"JSON parse error: {str(e)}"
            )
            for _ in products_chunk
        ]
    except Exception as e:
        print(f"❌ Batch error: {e}")
        import traceback
        traceback.print_exc()
        return [
            ProductCategory(
                main_category="Разно",
                sub_category="Останато",
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
    """
    Categorize all products with batching and rate limiting.

    Args:
        products: List of product dicts
        batch_size: Number of products per API request
        concurrency: Number of concurrent batches (2-3 recommended for tier 1)
        gemini_api_key: Google API key (or set GOOGLE_API_KEY env var)

    Returns:
        List of products with 'categorization' field added
    """
    if not gemini_api_key:
        gemini_api_key = os.getenv("GOOGLE_API_KEY")
        if not gemini_api_key:
            raise ValueError("GOOGLE_API_KEY not found in environment or arguments")

    # Configure Gemini client
    client = genai.Client(api_key=gemini_api_key)
    model_id = 'gemini-2.0-flash'

    print(f"🚀 Starting categorization of {len(products)} products")
    print(f"   Model: Gemini 2.0 Flash")
    print(f"   Batch size: {batch_size} products/request")
    print(f"   Concurrency: {concurrency} concurrent batches")
    print(f"   Estimated requests: {len(products) // batch_size + 1}")
    print()

    # Split products into batches
    batches = []
    for i in range(0, len(products), batch_size):
        batches.append(products[i:i + batch_size])

    # Semaphore for concurrency control
    semaphore = asyncio.Semaphore(concurrency)

    # Progress tracking
    completed = 0
    start_time = time.time()

    async def process_batch(batch_idx: int, batch: List[dict]):
        nonlocal completed

        async with semaphore:
            # Estimate tokens and acquire rate limit
            estimated_tokens = estimate_tokens(batch)
            await rate_limiter.acquire(estimated_tokens)

            # Categorize batch
            categorizations = await categorize_batch_gemini(batch, client, model_id)

            # Assign results
            for product, cat in zip(batch, categorizations):
                product['categorization'] = cat.model_dump()

            # Update progress
            completed += len(batch)
            elapsed = time.time() - start_time
            rate = completed / elapsed if elapsed > 0 else 0
            eta = (len(products) - completed) / rate if rate > 0 else 0

            if batch_idx % 10 == 0 or completed == len(products):
                stats = rate_limiter.get_stats()
                print(f"✓ {completed:,}/{len(products):,} products "
                      f"({completed * 100 // len(products)}%) | "
                      f"{rate:.1f} products/sec | "
                      f"ETA: {eta / 60:.1f}m | "
                      f"RPM: {stats['current_rpm']}/{stats['rpm_limit']}")

            return batch

    # Process all batches concurrently
    tasks = [process_batch(i, batch) for i, batch in enumerate(batches)]
    await asyncio.gather(*tasks)

    # Final stats
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
    """Main execution function."""

    # Check for Google API key
    gemini_api_key = os.getenv("GOOGLE_API_KEY")
    if not gemini_api_key:
        print("❌ ERROR: GOOGLE_API_KEY not found!")
        return

    # Connect to database
    db = connect_to_db('products_categorized')

    # Load products
    products, products_markets = load_products_to_categorize(
        db,
        limit_per_collection=None
    )

    if not products:
        print("✅ No products need categorization!")
        db.client.close()
        return

    # Categorize all products
    categorized_products = await categorize_all_products(
        products,
        batch_size=16,
        concurrency=32,
        gemini_api_key=gemini_api_key
    )

    # Save to database
    save_categorizations_to_db(db, categorized_products, products_markets)

    # Analyze results
    print("\n📈 Categorization Quality Analysis:")
    confidence_ranges = {
        'High (0.9-1.0)': 0,
        'Good (0.7-0.89)': 0,
        'Medium (0.5-0.69)': 0,
        'Low (<0.5)': 0,
        'Errors': 0
    }

    for p in categorized_products:
        conf = p['categorization'].get('confidence', 0)
        if p['categorization'].get('main_category') is None:
            confidence_ranges['Errors'] += 1
        elif conf >= 0.9:
            confidence_ranges['High (0.9-1.0)'] += 1
        elif conf >= 0.7:
            confidence_ranges['Good (0.7-0.89)'] += 1
        elif conf >= 0.5:
            confidence_ranges['Medium (0.5-0.69)'] += 1
        else:
            confidence_ranges['Low (<0.5)'] += 1

    for range_name, count in confidence_ranges.items():
        percentage = (count / len(categorized_products) * 100) if categorized_products else 0
        print(f"   {range_name}: {count:,} ({percentage:.1f}%)")

    # Show some examples
    print("\n📋 Sample categorizations:")
    for i, p in enumerate(categorized_products[:20]):
        cat = p['categorization']
        print(f"\n{i + 1}. {p['name'][:60]}")
        print(f"   → {cat['main_category']} / {cat['sub_category']}")
        print(f"   Confidence: {cat['confidence']:.2f}")
        if cat.get('reasoning'):
            print(f"   Reasoning: {cat['reasoning'][:80]}")

    # Close database
    db.client.close()

    print("\n✅ All done!")


if __name__ == "__main__":
    asyncio.run(main())