import asyncio
import time
import os
from collections import deque
from typing import List, Optional, Tuple
from datetime import datetime

from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, Field
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import ChatPromptTemplate

try:
    import tiktoken

    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False
    print("⚠️  tiktoken not installed. Token estimation will be approximate.")
    print("   Install with: pip install tiktoken")

from backend.db_utils import connect_to_db

load_dotenv(find_dotenv())

# ============================================================================
# CATEGORIES TAXONOMY
# ============================================================================

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
    "Дом и градина": ['Кујнски прибор и садови', 'Сијалици', 'Батерии', 'Супер лепак', 'Чепкалки за заби', 'Свеќи'],
    "Цигари": ['Цигари и никотински производи'],
    "Разно": ['Останато']
}

# Compressed taxonomy for prompts (saves ~40% tokens)
TAXONOMY_COMPRESSED = "\n".join([
    f"{main}: {', '.join(subs)}"
    for main, subs in CATEGORIES.items()
])


# ============================================================================
# PYDANTIC MODELS
# ============================================================================

class ProductCategory(BaseModel):
    """Single product categorization."""
    main_category: str = Field(description="Main category from taxonomy")
    sub_category: str = Field(description="Subcategory belonging to main category")
    confidence: float = Field(description="Confidence 0.0-1.0", ge=0.0, le=1.0)
    reasoning: Optional[str] = Field(default=None, description="Brief explanation")


class BatchProductCategories(BaseModel):
    """Multiple product categorizations in a single response."""
    products: List[ProductCategory] = Field(description="List of categorizations in order")


# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    """
    Token-aware rate limiter for API calls.
    Tracks requests per minute (RPM) and tokens per minute (TPM).
    """

    def __init__(self, rpm_limit: int = 14, tpm_limit: int = 950000):
        """
        Initialize rate limiter with conservative limits.

        Gemini 2.0 Flash free tier:
        - 1500 RPM
        - 1,000,000 TPM

        We use 1400/950k to leave a safety buffer.
        """
        self.rpm_limit = rpm_limit
        self.tpm_limit = tpm_limit

        self.request_times = deque()  # Timestamps of requests
        self.token_times = deque()  # (timestamp, token_count) tuples

        self.lock = asyncio.Lock()
        self.total_requests = 0
        self.total_tokens = 0

    def _clean_old_entries(self):
        """Remove entries older than 60 seconds."""
        cutoff = time.time() - 60

        while self.request_times and self.request_times[0] < cutoff:
            self.request_times.popleft()

        while self.token_times and self.token_times[0][0] < cutoff:
            self.token_times.popleft()

    def _get_current_usage(self) -> Tuple[int, int]:
        """Get current RPM and TPM usage in the last 60 seconds."""
        self._clean_old_entries()
        rpm_used = len(self.request_times)
        tpm_used = sum(tokens for _, tokens in self.token_times)
        return rpm_used, tpm_used

    async def acquire(self, estimated_tokens: int):
        """
        Wait until we can make a request without exceeding rate limits.

        Args:
            estimated_tokens: Estimated tokens for the upcoming request
        """
        async with self.lock:
            while True:
                rpm_used, tpm_used = self._get_current_usage()

                # Check if we can make this request
                if (rpm_used < self.rpm_limit and
                        tpm_used + estimated_tokens < self.tpm_limit):
                    # Record this request
                    now = time.time()
                    self.request_times.append(now)
                    self.token_times.append((now, estimated_tokens))

                    self.total_requests += 1
                    self.total_tokens += estimated_tokens
                    return

                # Calculate wait time
                wait_time = 1.0

                if rpm_used >= self.rpm_limit and self.request_times:
                    oldest = self.request_times[0]
                    wait_time = max(wait_time, 60 - (time.time() - oldest) + 0.2)

                if tpm_used + estimated_tokens >= self.tpm_limit and self.token_times:
                    oldest_token_time = self.token_times[0][0]
                    wait_time = max(wait_time, 60 - (time.time() - oldest_token_time) + 0.2)

                print(f"⏳ Rate limit: RPM {rpm_used}/{self.rpm_limit}, "
                      f"TPM {tpm_used:,}/{self.tpm_limit:,}. "
                      f"Waiting {wait_time:.1f}s...")
                await asyncio.sleep(wait_time)

    def get_stats(self) -> dict:
        """Get usage statistics."""
        rpm_used, tpm_used = self._get_current_usage()
        return {
            'total_requests': self.total_requests,
            'total_tokens': self.total_tokens,
            'current_rpm': rpm_used,
            'current_tpm': tpm_used,
            'rpm_limit': self.rpm_limit,
            'tpm_limit': self.tpm_limit
        }


# Global rate limiter instance
rate_limiter = RateLimiter()


# ============================================================================
# TOKEN ESTIMATION
# ============================================================================

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
            encoding = tiktoken.get_encoding("cl100k_base")
        except:
            encoding = None
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


# ============================================================================
# PROMPT TEMPLATES
# ============================================================================

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


# ============================================================================
# CATEGORIZATION FUNCTIONS
# ============================================================================

async def categorize_batch_gemini(
        products_chunk: List[dict],
        google_api_key: str
) -> List[ProductCategory]:
    """
    Categorize a batch of products using Gemini 2.0 Flash.

    Args:
        products_chunk: List of 3-8 products to categorize in one request
        google_api_key: Google API key

    Returns:
        List of ProductCategory objects
    """
    # Build products text
    products_text = "\n\n".join([
        f"Product {i + 1}:\n"
        f"Name: {p.get('name', '')}\n"
        f"Description: {p.get('description', 'Нема опис')}\n"
        f"Source categories: {p.get('existing_categories', 'Нема')}"
        for i, p in enumerate(products_chunk)
    ])

    # Initialize Gemini
    llm = ChatGoogleGenerativeAI(
        model="models/gemini-2.0-flash",  # Change this line
        temperature=0.1,
        google_api_key=google_api_key,
        max_retries=2
    )

    # Bind structured output
    structured_llm = llm.with_structured_output(BatchProductCategories)

    # Create chain
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
        concurrency: int = 3,
        google_api_key: str = None
) -> List[dict]:
    """
    Categorize all products with batching and rate limiting.

    Args:
        products: List of product dicts
        batch_size: Number of products per API request (3-8 recommended)
        concurrency: Number of concurrent batches (10-20 recommended)
        google_api_key: Google API key (or set GOOGLE_API_KEY env var)

    Returns:
        List of products with 'categorization' field added
    """
    if not google_api_key:
        google_api_key = os.getenv("GOOGLE_API_KEY")
        if not google_api_key:
            raise ValueError("GOOGLE_API_KEY not found in environment or arguments")

    print(f"🚀 Starting categorization of {len(products)} products")
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
            categorizations = await categorize_batch_gemini(batch, google_api_key)

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
    print(f"   Estimated cost: $0.00 (Gemini free tier)")
    print("=" * 70)

    return products


# ============================================================================
# DATABASE INTEGRATION
# ============================================================================

def load_products_from_db(db, limit_per_collection: int = None) -> Tuple[List[dict], dict]:
    """
    Load products from MongoDB that need categorization.

    Args:
        db: MongoDB database object
        limit_per_collection: Max products per collection (None = all)

    Returns:
        (products_list, products_to_markets_mapping)
    """
    products = []
    products_markets = {}

    collections = [c for c in db.list_collection_names() if c != 'products_categorized' and c!= 'all_products']

    print(f"📂 Loading products from {len(collections)} collections...")

    for collection in collections:
        query = {}
        cursor = db[collection].find(query)

        if limit_per_collection:
            cursor = cursor.limit(limit_per_collection)

        collection_count = 0

        for product in cursor:
            # Check if already categorized
            existing = db['products_categorized'].find_one({'_id': product['_id']})
            if existing and existing.get('categorization', {}).get('main_category'):
                continue  # Skip already categorized

            # Extract description from various possible fields
            description = ""
            for field in ['description', 'category', 'categories']:
                if field in product:
                    desc_value = product[field]
                    if isinstance(desc_value, list):
                        description = ", ".join(str(x) for x in desc_value)
                    else:
                        description = str(desc_value)
                    break

            # Create normalized product
            new_product = {
                '_id': product.get('_id', ''),
                'name': product.get('name', ''),
                'description': description,
                'existing_categories': description  # Use same field for existing categories
            }

            products.append(new_product)
            products_markets[product['_id']] = collection
            collection_count += 1

        print(f"   {collection}: {collection_count} products")

    print(f"📊 Total products to categorize: {len(products)}")
    return products, products_markets


def save_categorizations_to_db(db, products: List[dict], products_markets: dict):
    """
    Save categorized products to MongoDB.

    Args:
        db: MongoDB database object
        products: List of categorized products
        products_markets: Mapping of product_id to source market
    """
    print(f"\n💾 Saving {len(products)} categorizations to database...")

    to_insert = []
    updated_count = 0

    for product in products:
        product['market'] = products_markets.get(product['_id'], 'unknown')
        product['categorized_at'] = datetime.utcnow()

        # Try to update existing
        result = db['products_categorized'].update_one(
            {'_id': product['_id']},
            {'$set': {
                'categorization': product['categorization'],
                'categorized_at': product['categorized_at']
            }},
            upsert=False
        )

        if result.matched_count > 0:
            updated_count += 1
        else:
            to_insert.append(product)

    # Bulk insert new ones
    if to_insert:
        if len(to_insert) == 1:
            db['products_categorized'].insert_one(to_insert[0])
        else:
            db['products_categorized'].insert_many(to_insert)

    print(f"   Updated: {updated_count}")
    print(f"   Inserted: {len(to_insert)}")
    print(f"   Total saved: {len(products)}")


# ============================================================================
# MAIN
# ============================================================================

async def main():
    """Main execution function."""

    # Check for Google API key
    google_api_key = os.getenv("GOOGLE_API_KEY")
    if not google_api_key:
        print("=" * 70)
        print("❌ ERROR: GOOGLE_API_KEY not found!")
        print()
        print("To get a Google API key:")
        print("1. Go to: https://aistudio.google.com/app/apikey")
        print("2. Click 'Create API Key'")
        print("3. Add to your .env file:")
        print("   GOOGLE_API_KEY=your_key_here")
        print("=" * 70)
        return

    print("=" * 70)
    print("🤖 Product Categorization System")
    print("=" * 70)
    print()

    # Connect to database
    db = connect_to_db('products_categorized')

    # Load products
    # For testing: limit_per_collection=50
    # For production: limit_per_collection=None
    products, products_markets = load_products_from_db(
        db,
        limit_per_collection=50  # Remove this or set to None for all products
    )

    if not products:
        print("✅ No products need categorization!")
        db.client.close()
        return

    # Categorize all products
    categorized_products = await categorize_all_products(
        products,
        batch_size=5,  # 5 products per request (optimal for Gemini)
        concurrency=3,  # 3 concurrent batches
        google_api_key=google_api_key
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