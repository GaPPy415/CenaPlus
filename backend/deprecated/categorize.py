import asyncio
import time
import json
from typing import List, Optional, Tuple
from datetime import datetime
from pydantic import BaseModel, Field
import ollama
from backend.db_utils import connect_to_db


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
                                  'Нега на лице', 'Нега на раце', 'Нега на стапала', 'Нега на тело',
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

OLLAMA_MODEL = "mkllm-7b-q5"  # Your local model name

# ============================================================================
# PYDANTIC MODELS
# ============================================================================

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
    """Complete product categorization."""
    main_category: str = Field(description="Main category from taxonomy")
    sub_category: str = Field(description="Subcategory belonging to main category")
    main_confidence: float = Field(description="Main category confidence 0.0-1.0", ge=0.0, le=1.0)
    sub_confidence: float = Field(description="Subcategory confidence 0.0-1.0", ge=0.0, le=1.0)
    main_reasoning: Optional[str] = Field(default=None, description="Main category reasoning")
    sub_reasoning: Optional[str] = Field(default=None, description="Subcategory reasoning")


def get_ollama_client() -> ollama.Client:
    """Create Ollama client."""
    return ollama.Client()


def build_main_category_prompt() -> str:
    """Build the system prompt for main category categorization."""
    main_categories = ", ".join(CATEGORIES.keys())
    return f"""Ти си експерт за категоризација на производи во македонски супермаркети.

Категоризирај ги производите во ЕДНА главна категорија од оваа листа:

{main_categories}

ПРАВИЛА:
1. Избери ја најспецифичната и најрелевантната главна категорија
2. Ако повеќе категории одговараат, избери ја примарната намена
3. Оценување на доверба:
   - 0.9-1.0: Јасно совпаѓање
   - 0.7-0.89: Добро совпаѓање, мала нејаснотија
   - 0.5-0.69: Повеќе опции, избрана најверојатна
   - <0.5: Несигурно, потребна проверка
4. Образложението треба да биде кратко (1 реченица)

ВАЖНО: Одговорот МОРА да биде валиден JSON во овој формат:
{{
  "main_category": "string",
  "confidence": 0.0-1.0,
  "reasoning": "string"
}}

МОРА да избереш категорија САМО од горната листа. Не измислувај нови категории."""


def build_sub_category_prompt(main_category: str) -> str:
    """Build the system prompt for subcategory categorization."""
    subcategories = ", ".join(CATEGORIES.get(main_category, []))
    return f"""Ти си експерт за категоризација на производи во македонски супермаркети.

Производот веќе е категоризиран во главната категорија: {main_category}

Сега категоризирај го во ЕДНА подкатегорија од оваа листа:

{subcategories}

ПРАВИЛА:
1. Избери ја најспецифичната и најрелевантната подкатегорија
2. Ако повеќе подкатегории одговараат, избери ја примарната намена
3. Оценување на доверба:
   - 0.9-1.0: Јасно совпаѓање
   - 0.7-0.89: Добро совпаѓање, мала нејаснотија
   - 0.5-0.69: Повеќе опции, избрана најверојатна
   - <0.5: Несигурно, потребна проверка
4. Образложението треба да биде кратко (1 реченица)

ВАЖНО: Одговорот МОРА да биде валиден JSON во овој формат:
{{
  "sub_category": "string",
  "confidence": 0.0-1.0,
  "reasoning": "string"
}}

МОРА да избереш подкатегорија САМО од горната листа. Не измислувај нови подкатегории."""


def categorize_main_category_ollama(
        client: ollama.Client,
        product: dict
) -> ProductMainCategory:
    """
    Categorize a single product into a main category using Ollama.
    """
    prompt = f"""Name: {product.get('name', '')}
Description: {product.get('description', 'Нема опис')}
Source categories: {product.get('existing_categories', 'Нема')}"""

    try:
        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": build_main_category_prompt()},
                {"role": "user", "content": prompt}
            ],
            format="json",
            options={"temperature": 0.1}
        )

        # Parse JSON response
        content = response['message']['content']
        data = json.loads(content)

        return ProductMainCategory(
            main_category=data.get('main_category', 'Разно'),
            confidence=float(data.get('confidence', 0.5)),
            reasoning=data.get('reasoning')
        )

    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error: {e}")
        return ProductMainCategory(
            main_category="Разно",
            confidence=0.0,
            reasoning=f"JSON parse error: {str(e)}"
        )
    except Exception as e:
        print(f"❌ Ollama error: {e}")
        return ProductMainCategory(
            main_category="Разно",
            confidence=0.0,
            reasoning=f"Error: {str(e)}"
        )


def categorize_sub_category_ollama(
        client: ollama.Client,
        product: dict,
        main_category: str
) -> ProductSubCategory:
    """
    Categorize a single product into a subcategory using Ollama.
    """
    prompt = f"""Name: {product.get('name', '')}
Description: {product.get('description', 'Нема опис')}
Source categories: {product.get('existing_categories', 'Нема')}"""

    try:
        response = client.chat(
            model=OLLAMA_MODEL,
            messages=[
                {"role": "system", "content": build_sub_category_prompt(main_category)},
                {"role": "user", "content": prompt}
            ],
            format="json",
            options={"temperature": 0.1}
        )

        # Parse JSON response
        content = response['message']['content']
        data = json.loads(content)

        return ProductSubCategory(
            sub_category=data.get('sub_category', 'Останато'),
            confidence=float(data.get('confidence', 0.5)),
            reasoning=data.get('reasoning')
        )

    except json.JSONDecodeError as e:
        print(f"❌ JSON parse error: {e}")
        return ProductSubCategory(
            sub_category="Останато",
            confidence=0.0,
            reasoning=f"JSON parse error: {str(e)}"
        )
    except Exception as e:
        print(f"❌ Ollama error: {e}")
        return ProductSubCategory(
            sub_category="Останато",
            confidence=0.0,
            reasoning=f"Error: {str(e)}"
        )


def categorize_batch_ollama(
        client: ollama.Client,
        products_chunk: List[dict]
) -> List[ProductCategory]:
    """
    Categorize a batch of products using Ollama with two-stage approach.
    Stage 1: Categorize all products into main categories
    Stage 2: Group by main category and categorize into subcategories
    """
    results = []

    # Stage 1: Main category categorization
    print("   Stage 1: Categorizing main categories...")
    main_categorizations = {}
    for product in products_chunk:
        main_cat = categorize_main_category_ollama(client, product)
        main_categorizations[product['_id']] = main_cat

    # Group products by main category
    products_by_main_cat = {}
    for product in products_chunk:
        main_cat = main_categorizations[product['_id']].main_category
        if main_cat not in products_by_main_cat:
            products_by_main_cat[main_cat] = []
        products_by_main_cat[main_cat].append(product)

    # Stage 2: Subcategory categorization grouped by main category
    print(f"   Stage 2: Categorizing subcategories for {len(products_by_main_cat)} main categories...")
    for main_cat, products_in_cat in products_by_main_cat.items():
        print(f"      Processing {len(products_in_cat)} products in '{main_cat}'")
        for product in products_in_cat:
            main_cat_result = main_categorizations[product['_id']]
            sub_cat = categorize_sub_category_ollama(client, product, main_cat)

            # Combine results
            full_categorization = ProductCategory(
                main_category=main_cat_result.main_category,
                sub_category=sub_cat.sub_category,
                main_confidence=main_cat_result.confidence,
                sub_confidence=sub_cat.confidence,
                main_reasoning=main_cat_result.reasoning,
                sub_reasoning=sub_cat.reasoning
            )
            results.append(full_categorization)

    return results


async def categorize_all_products(
        products: List[dict],
        batch_size: int = 32,
        concurrency: int = 1  # Local models work best with sequential processing
) -> List[dict]:
    """
    Categorize all products using local Ollama model with two-stage approach.
    Stage 1: Categorize into main categories
    Stage 2: Group by main category and categorize into subcategories
    """
    print(f"🚀 Starting TWO-STAGE categorization of {len(products)} products")
    print(f"   Model: {OLLAMA_MODEL}")
    print(f"   Batch size: {batch_size}")
    print()

    client = get_ollama_client()
    start_time = time.time()
    completed = 0

    # Process in batches for progress tracking
    for i in range(0, len(products), batch_size):
        batch = products[i:i + batch_size]
        print(f"\n📦 Processing batch {i // batch_size + 1}/{(len(products) + batch_size - 1) // batch_size}")
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

    collections = [
        c for c in db.list_collection_names()
        if c != 'products_categorized' and c != 'all_products' and not c.startswith('products')
    ]

    print(f"📂 Loading products from {len(collections)} collections...")

    for collection in collections:
        cursor = db[collection].find({})
        if limit_per_collection:
            cursor = cursor.limit(limit_per_collection)

        # Prefetch categorized ids for this collection in a single query
        categorized_ids = set(
            doc["_id"]
            for doc in db["products_categorized"].find(
                {"market": collection, "categorization.main_category": {"$exists": True}},
                {"_id": 1}
            )
        )

        collection_count = 0
        for product in cursor:
            if product["_id"] in categorized_ids:
                continue

            description = ""
            for field in ["description", "category", "categories"]:
                if field in product:
                    desc_value = product[field]
                    if isinstance(desc_value, list):
                        description = ", ".join(str(x) for x in desc_value)
                    else:
                        description = str(desc_value)
                    break

            new_product = {
                "_id": product.get("_id", ""),
                "name": product.get("name", ""),
                "description": description,
                "existing_categories": description
            }

            products.append(new_product)
            products_markets[product["_id"]] = collection
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
        limit_per_collection=5
    )

    if not products:
        print("✅ No products need categorization!")
        db.client.close()
        return

    categorized_products = await categorize_all_products(
        products,
        batch_size=1
    )

    # save_categorizations_to_db(db, categorized_products, products_markets)

    # Analyze results
    print("\n📈 Categorization Quality Analysis:")
    print("\nMain Category Confidence:")
    main_confidence_ranges = {
        'High (0.9-1.0)': 0,
        'Good (0.7-0.89)': 0,
        'Medium (0.5-0.69)': 0,
        'Low (<0.5)': 0,
        'Errors': 0
    }

    print("\nSubcategory Confidence:")
    sub_confidence_ranges = {
        'High (0.9-1.0)': 0,
        'Good (0.7-0.89)': 0,
        'Medium (0.5-0.69)': 0,
        'Low (<0.5)': 0,
        'Errors': 0
    }

    for p in categorized_products:
        cat = p['categorization']
        main_conf = cat.get('main_confidence', 0)
        sub_conf = cat.get('sub_confidence', 0)

        # Main category confidence
        if cat.get('main_category') is None:
            main_confidence_ranges['Errors'] += 1
        elif main_conf >= 0.9:
            main_confidence_ranges['High (0.9-1.0)'] += 1
        elif main_conf >= 0.7:
            main_confidence_ranges['Good (0.7-0.89)'] += 1
        elif main_conf >= 0.5:
            main_confidence_ranges['Medium (0.5-0.69)'] += 1
        else:
            main_confidence_ranges['Low (<0.5)'] += 1

        # Subcategory confidence
        if cat.get('sub_category') is None:
            sub_confidence_ranges['Errors'] += 1
        elif sub_conf >= 0.9:
            sub_confidence_ranges['High (0.9-1.0)'] += 1
        elif sub_conf >= 0.7:
            sub_confidence_ranges['Good (0.7-0.89)'] += 1
        elif sub_conf >= 0.5:
            sub_confidence_ranges['Medium (0.5-0.69)'] += 1
        else:
            sub_confidence_ranges['Low (<0.5)'] += 1

    print("\n  Main Categories:")
    for range_name, count in main_confidence_ranges.items():
        pct = (count / len(categorized_products) * 100) if categorized_products else 0
        print(f"    {range_name}: {count:,} ({pct:.1f}%)")

    print("\n  Subcategories:")
    for range_name, count in sub_confidence_ranges.items():
        pct = (count / len(categorized_products) * 100) if categorized_products else 0
        print(f"    {range_name}: {count:,} ({pct:.1f}%)")

    print("\n📋 Sample categorizations:")
    for i, p in enumerate(categorized_products[:100]):
        cat = p['categorization']
        print(f"\n{i + 1}. {p['name'][:60]}")
        print(f"   → {cat['main_category']} / {cat['sub_category']}")
        print(f"   Main confidence: {cat.get('main_confidence', 0):.2f} | Sub confidence: {cat.get('sub_confidence', 0):.2f}")
        if cat.get('main_reasoning'):
            print(f"   Main reasoning: {cat['main_reasoning'][:80]}")
        if cat.get('sub_reasoning'):
            print(f"   Sub reasoning: {cat['sub_reasoning'][:80]}")

    db.client.close()
    print("\n✅ All done!")


if __name__ == "__main__":
    asyncio.run(main())
