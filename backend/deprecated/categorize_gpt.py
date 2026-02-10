import asyncio
import time
import os
from typing import List, Optional
from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

from backend.RateLimiter import RateLimiter
from backend.db_utils import load_products_to_categorize, save_categorizations_to_db

try:
    import tiktoken

    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False
    print("⚠️  tiktoken not installed. Token estimation will be approximate.")
    print("   Install with: pip install tiktoken")

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
                  'Замрзнато овошје', 'Замрзнато месо'],
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
                        'Замена за млеко за деца'],
    "Домашни миленици": ['Антипаразитски лекови', 'Влажна храна за мачки', 'Влажна храна за кучиња', 'Грицки за мачки',
                         'Грицки за кучиња', 'Сува храна за мачки', 'Сува храна за кучиња'],
    "Дом и градина": ['Кујнски прибор и садови', 'Сијалици', 'Батерии', 'Супер лепак', 'Чепкалки за заби', 'Свеќи', 'Салфети'],
    "Цигари": ['Цигари и никотински производи'],
    "Разно": ['Останато']
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


rate_limiter = RateLimiter(rpm_limit=14, tpm_limit=200000)  # Conservative limits for GPT-4o mini

def estimate_tokens(products: List[dict]) -> int:
    """
    Estimate tokens for a batch of products.

    Args:
        products: List of product dicts with 'name', 'description', 'existing_categories'

    Returns:
        Estimated total tokens (input + output)
    """
    if TIKTOKEN_AVAILABLE:
        try:
            encoding = tiktoken.encoding_for_model("gpt-5.1")
        except:
            encoding = tiktoken.get_encoding("cl100k_base")
    else:
        encoding = None

    # System prompt + taxonomy (approximately constant)
    system_tokens = 350  # System prompt
    taxonomy_tokens = 800  # Compressed taxonomy

    # User content for all products
    user_content = "\n\n".join([
        f"Product {i + 1}:\nName: {p.get('name', '')}\n"
        f"Description: {p.get('description', 'Нема опис')}\n"
        f"Source: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products)
    ])

    if encoding:
        user_tokens = len(encoding.encode(user_content))
    else:
        # Rough approximation: 1 token ≈ 4 characters for Macedonian
        user_tokens = len(user_content) // 4

    # Output tokens: ~100 per product (category info + reasoning)
    output_tokens = len(products) * 100

    total = system_tokens + taxonomy_tokens + user_tokens + output_tokens

    return total

def create_batch_prompt() -> ChatPromptTemplate:
    """Create optimized prompt for batch categorization."""

    prompt = ChatPromptTemplate.from_messages([
        ("system", """You are a product categorization expert for Macedonian supermarkets.

Categorize ALL products below into ONE main category and ONE subcategory from this taxonomy:

{taxonomy}

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

Keep reasoning brief (1 sentence)."""),

        ("user", """{products_text}

Return a JSON object with a "products" array containing categorizations for ALL products above, in order.""")
    ])

    return prompt


async def categorize_batch_gpt(
        products_chunk: List[dict],
        openai_api_key: str
) -> List[ProductCategory]:
    """
    Categorize a batch of products.

    Args:
        products_chunk: List of 3-8 products to categorize in one request
        openai_api_key: OpenAI API key

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

    llm = ChatOpenAI(
        model="gpt-5.1",
        temperature=0.1,
        api_key=openai_api_key,
        max_retries=2
    )

    structured_llm = llm.with_structured_output(BatchProductCategories)

    prompt = create_batch_prompt()
    chain = prompt | structured_llm

    try:
        # Invoke
        result = await chain.ainvoke({
            "taxonomy": TAXONOMY_COMPRESSED,
            "products_text": products_text
        })

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

        return result.products[:len(products_chunk)]  # Ensure exact match

    except Exception as e:
        print(f"❌ Batch error: {e}")
        # Return error categorizations for all products in batch
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
        batch_size: int = 5,
        concurrency: int = 16,
        openai_api_key: str = None
) -> List[dict]:
    """
    Categorize all products with batching and rate limiting.

    Args:
        products: List of product dicts
        batch_size: Number of products per API request (3-8 recommended)
        concurrency: Number of concurrent batches (5-10 recommended for GPT-4o mini)
        openai_api_key: OpenAI API key (or set OPENAI_API_KEY env var)

    Returns:
        List of products with 'categorization' field added
    """
    if not openai_api_key:
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY not found in environment or arguments")

    print(f"🚀 Starting categorization of {len(products)} products")
    print(f"   Model: GPT-5.1")
    print(f"   Batch size: {batch_size} products/request")
    print(f"   Concurrency: {concurrency} concurrent batches")
    print(f"   Estimated requests: {len(products) // batch_size + 1}")
    print(f"   Estimated cost: ~${(len(products) / 1000) * 0.15:.2f}")
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
            categorizations = await categorize_batch_gpt(batch, openai_api_key)

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
    estimated_cost = (stats['total_tokens'] / 1_000_000) * 0.15  # $0.15 per 1M tokens for gpt-4o-mini

    print()
    print("=" * 70)
    print(f"✅ Categorization complete!")
    print(f"   Total products: {len(products):,}")
    print(f"   Total time: {elapsed / 60:.2f} minutes")
    print(f"   Average rate: {len(products) / elapsed:.1f} products/sec")
    print(f"   Total API requests: {stats['total_requests']:,}")
    print(f"   Total tokens used: {stats['total_tokens']:,}")
    print(f"   Estimated cost: ${estimated_cost:.2f}")
    print("=" * 70)

    return products


async def main():
    """Main execution function."""

    # Check for OpenAI API key
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        print("❌ ERROR: OPENAI_API_KEY not found!")
        return

    # Connect to database
    db = connect_to_db('products_categorized')

    # Load products
    # For testing: limit_per_collection=50
    # For production: limit_per_collection=None
    products, products_markets = load_products_to_categorize(
        db,
        limit_per_collection=6100  # Remove this or set to None for all products
    )

    if not products:
        print("✅ No products need categorization!")
        db.client.close()
        return

    # Categorize all products
    categorized_products = await categorize_all_products(
        products,
        batch_size=64,  # products per request
        concurrency=32,  # concurrent batches
        openai_api_key=openai_api_key
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
    for i, p in enumerate(categorized_products[:5]):
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