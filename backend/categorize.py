import asyncio
import time
import json
from typing import List, Optional, Tuple
from datetime import datetime

from pydantic import BaseModel, Field
import ollama

from backend.db_utils import connect_to_db

# ============================================================================
# CATEGORIES TAXONOMY
# ============================================================================

CATEGORIES = {
    "Намирници": ['Брашно', 'Додатоци за јадења', 'Додатоци за конзервирање', 'Готови оброци', 'Јајца',
                  'Кечап и сос од домати', 'Квасец', 'Мајонез, сенф, рен, преливи, сосови', 'Маргарин',
                  'Мешавина од зачини', 'Зрнести производи', 'Пудинг и шлаг', 'Шеќер', 'Оцет', 'Сол', 'Пекара',
                  'Супи и чорби', 'Сè за торти и колачи', 'Тестенини', 'Тортиљи', 'Масло', 'Зачини', 'Пире'],
    "Здрава храна": ['Безглутенски производи', 'Растителни напитоци', 'Експандиран ориз',
                     'Интегрален ориз', 'Интегрални колачи', 'Мед', 'Овесна каша', 'Снегулки и мусли',
                     'Солени грицки', 'Шумливи таблети', 'Семки и семки со лушпа', 'Суви овошја',
                     'Засладувачи', 'Здрави намази', 'Здрави грицки и пијалоци', 'Двопек',
                     'Протеин од сурутка', 'Зрнести производи'],
    "Млечни производи": ['Млеко', 'Јогурт', 'Путер', 'Млечни десерти', 'Млечни пијалоци', 'Кисело млеко',
                         'Кисела павлака', 'Грчки јогурт', 'Сирења', 'Намази од сирење', 'Сурутка',
                         'Преработено сирење', 'Овошен јогурт'],
    "Овошје и зеленчук": ['Зеленчук', 'Овошје', 'Конзервиран зеленчук', 'Конзервирано овошје'],
    "Месо и риба": ['Колбаси', 'Конзервирани производи', 'Намази', 'Паштета', 'Сувомеснато и процесирано месо',
                    'Салама', 'На тенки парчиња', 'Свежа риба', 'Свежо месо', 'Виршли и колбасици'],
    "Замрзнато": ['Готови сладоледи', 'Риба и морска храна', 'Замрзнат зеленчук', 'Замрзнато тесто и пецива',
                  'Замрзнато овошје', 'Замрзнато месо'],
    "Пијалоци": ['Вода', 'Кафе', 'Газирани сокови', 'Капсули за кафе', 'Енергетски пијалоци', 'Чаеви',
                 'Ладени чаеви', 'Негазирани сокови'],
    "Алкохолни пијалоци": ['Пиво', 'Јаки алкохолни пијалоци', 'Вино', 'Витамински пијалоци', 'Квас',
                           'Коктел', 'Шампањско и пенливо вино'],
    "Слатки и грицки": ['Бонбони', 'Бонбоњера', 'Чоколади', 'Чоколадни барови', 'Десерти',
                        'Додатоци за млеко', 'Грицки', 'Кекс, вафли, бисквит', 'Кремови', 'Кроасани',
                        'Наполитанки', 'Ролати', 'Гуми за џвакање', 'Слатки намази',
                        'Диетални и здрави слатки'],
    "Лична хигиена и козметика": ['Сапуни', 'Чистење на лицето', 'Бричеви', 'Боја за коса',
                                  'Гелови за туширање', 'Хигиена за жени', 'Дезодоранси', 'Нега за коса',
                                  'Нега на лице', 'Нега на раце', 'Нега на стапала', 'Нега за тело',
                                  'Орална хигиена', 'Хартија конфекција', 'Препарати за сончање',
                                  'Стик и рол-он', 'Сетови за поклон', 'Лабело', 'Производи за бричење',
                                  'Стапчиња за уши', 'Кондоми', 'Парфеми'],
    "Домашна хемија": ['Детергент за садови', 'Дополнителна нега на алишта', 'Инсектициди',
                       'Капсули за перење алишта', 'Марамчиња за перење алишта', 'Омекнувач за алишта',
                       'Прашок за перење алишта', 'Течни детергенти за перење алишта',
                       'Освежувачи на простор', 'Машинско миење садови', 'Средства за чистење',
                       'Средства за чистење на домаќинство', 'Средства за чистење на санитарии',
                       'Нега на обувки', 'Освежувачи на тоалет', 'Опрема за чистење'],
    "Катче за бебиња": ['Детска хигиена', 'Храна за бебиња', 'Каша за деца', 'Пијалоци', 'Пелени',
                        'Замена за млеко за деца'],
    "Домашни миленици": ['Антипаразитски лекови', 'Влажна храна за мачки', 'Влажна храна за кучиња',
                         'Грицки за мачки', 'Грицки за кучиња', 'Сува храна за мачки',
                         'Сува храна за кучиња'],
    "Дом и градина": ['Кујнски прибор и садови', 'Сијалици', 'Батерии', 'Супер лепак',
                      'Чепкалки за заби', 'Свеќи'],
    "Цигари": ['Цигари и никотински производи'],
    "Разно": ['Останато']
}

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
# OLLAMA CONFIGURATION
# ============================================================================

OLLAMA_MODEL = "categorizer"  # Your local model name


def get_ollama_client() -> ollama.Client:
    """Create Ollama client."""
    return ollama.Client()


# ============================================================================
# CATEGORIZATION FUNCTIONS
# ============================================================================

def build_system_prompt() -> str:
    """Build the system prompt for categorization."""
    return f"""You are a product categorization expert for Macedonian supermarkets.

Categorize products into ONE main category and ONE subcategory from this taxonomy:

{TAXONOMY_COMPRESSED}

RULES:
1. Choose most specific and relevant category
2. If multiple categories fit, choose primary use case
3. Confidence scoring:
   - 0.9-1.0: Clear match
   - 0.7-0.89: Good match, minor ambiguity
   - 0.5-0.69: Multiple options, chose most likely
   - <0.5: Uncertain, needs review
4. Subcategory MUST belong to chosen main category
5. Keep reasoning brief (1 sentence)

Respond ONLY with valid JSON matching this schema:
{{
  "main_category": "string",
  "sub_category": "string", 
  "confidence": 0.0-1.0,
  "reasoning": "string"
}}"""


def categorize_single_product_ollama(
        client: ollama.Client,
        product: dict
) -> ProductCategory:
    """
    Categorize a single product using Ollama.
    """
    prompt = f"""Name: {product.get('name', '')}
Description: {product.get('description', 'Нема опис')}
Source categories: {product.get('existing_categories', 'Нема')}"""

    try:
        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": build_system_prompt()},
                {"role": "user", "content": prompt}
            ],
            format="json",
            options={"temperature": 0.1}
        )

        # Parse JSON response
        content = response['message']['content']
        data = json.loads(content)

        return ProductCategory(
            main_category=data.get('main_category', 'Разно'),
            sub_category=data.get('sub_category', 'Останато'),
            confidence=float(data.get('confidence', 0.5)),
            reasoning=data.get('reasoning')
        )

    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error: {e}")
        return ProductCategory(
            main_category="Разно",
            sub_category="Останато",
            confidence=0.0,
            reasoning=f"JSON parse error: {str(e)}"
        )
    except Exception as e:
        print(f"❌ Ollama error: {e}")
        return ProductCategory(
            main_category="Разно",
            sub_category="Останато",
            confidence=0.0,
            reasoning=f"Error: {str(e)}"
        )


def categorize_batch_ollama(
        client: ollama.Client,
        products_chunk: List[dict]
) -> List[ProductCategory]:
    """
    Categorize a batch of products using Ollama.
    Processes one at a time since local models handle single requests better.
    """
    results = []
    for product in products_chunk:
        cat = categorize_single_product_ollama(client, product)
        results.append(cat)
    return results


async def categorize_all_products(
        products: List[dict],
        batch_size: int = 10,
        concurrency: int = 1  # Local models work best with sequential processing
) -> List[dict]:
    """
    Categorize all products using local Ollama model.
    """
    print(f"🚀 Starting categorization of {len(products)} products")
    print(f"   Model: {OLLAMA_MODEL}")
    print(f"   Batch size: {batch_size}")
    print()

    client = get_ollama_client()
    start_time = time.time()
    completed = 0

    # Process in batches for progress tracking
    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        categorizations = categorize_batch_ollama(client, batch)

        for product, cat in zip(batch, categorizations):
            product['categorization'] = cat.model_dump()

        completed += len(batch)
        elapsed = time.time() - start_time
        rate = completed / elapsed if elapsed > 0 else 0
        eta = (len(products) - completed) / rate if rate > 0 else 0

        print(f"✓ {completed:,}/{len(products):,} products "
              f"({completed * 100 // len(products)}%) | "
              f"{rate:.1f} products/sec | "
              f"ETA: {eta / 60:.1f}m")

    elapsed = time.time() - start_time
    print()
    print("=" * 70)
    print(f"✅ Categorization complete!")
    print(f"   Total products: {len(products):,}")
    print(f"   Total time: {elapsed / 60:.2f} minutes")
    print(f"   Average rate: {len(products) / elapsed:.1f} products/sec")
    print("=" * 70)

    return products


# ============================================================================
# DATABASE INTEGRATION (unchanged)
# ============================================================================

def load_products_from_db(db, limit_per_collection: int = None) -> Tuple[List[dict], dict]:
    """Load products from MongoDB that need categorization."""
    products = []
    products_markets = {}

    collections = [c for c in db.list_collection_names()
                   if c != 'products_categorized' and c != 'all_products' and not c.startswith('products')]

    print(f"📂 Loading products from {len(collections)} collections...")

    for collection in collections:
        cursor = db[collection].find({})
        if limit_per_collection:
            cursor = cursor.limit(limit_per_collection)

        collection_count = 0
        for product in cursor:
            existing = db['products_categorized'].find_one({'_id': product['_id']})
            if existing and existing.get('categorization', {}).get('main_category'):
                continue

            description = ""
            for field in ['description', 'category', 'categories']:
                if field in product:
                    desc_value = product[field]
                    if isinstance(desc_value, list):
                        description = ", ".join(str(x) for x in desc_value)
                    else:
                        description = str(desc_value)
                    break

            new_product = {
                '_id': product.get('_id', ''),
                'name': product.get('name', ''),
                'description': description,
                'existing_categories': description
            }

            products.append(new_product)
            products_markets[product['_id']] = collection
            collection_count += 1

        print(f"   {collection}: {collection_count} products")

    print(f"📊 Total products to categorize: {len(products)}")
    return products, products_markets


def save_categorizations_to_db(db, products: List[dict], products_markets: dict):
    """Save categorized products to MongoDB."""
    print(f"\n💾 Saving {len(products)} categorizations to database...")

    to_insert = []
    updated_count = 0

    for product in products:
        product['market'] = products_markets.get(product['_id'], 'unknown')
        product['categorized_at'] = datetime.utcnow()

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

    if to_insert:
        if len(to_insert) == 1:
            db['products_categorized'].insert_one(to_insert[0])
        else:
            db['products_categorized'].insert_many(to_insert)

    print(f"   Updated: {updated_count}")
    print(f"   Inserted: {len(to_insert)}")


# ============================================================================
# MAIN
# ============================================================================

async def main():
    """Main execution function."""
    print("=" * 70)
    print("🤖 Product Categorization System (Ollama)")
    print(f"   Model: {OLLAMA_MODEL}")
    print("=" * 70)
    print()

    db = connect_to_db('products_categorized')

    products, products_markets = load_products_from_db(
        db,
        limit_per_collection=20
    )

    if not products:
        print("✅ No products need categorization!")
        db.client.close()
        return

    categorized_products = await categorize_all_products(
        products,
        batch_size=10
    )

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
        pct = (count / len(categorized_products) * 100) if categorized_products else 0
        print(f"   {range_name}: {count:,} ({pct:.1f}%)")

    print("\n📋 Sample categorizations:")
    for i, p in enumerate(categorized_products[:5]):
        cat = p['categorization']
        print(f"\n{i + 1}. {p['name'][:60]}")
        print(f"   → {cat['main_category']} / {cat['sub_category']}")
        print(f"   Confidence: {cat['confidence']:.2f}")
        if cat.get('reasoning'):
            print(f"   Reasoning: {cat['reasoning'][:80]}")

    db.client.close()
    print("\n✅ All done!")


if __name__ == "__main__":
    asyncio.run(main())
