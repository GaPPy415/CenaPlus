import json
from pathlib import Path

_categories_json_path = (
    Path(__file__).resolve().parent.parent.parent / "shared" / "categories.json"
)
if not _categories_json_path.exists():
    raise FileNotFoundError(
        f"Category taxonomy not found at {_categories_json_path}. "
        "Ensure shared/categories.json exists (run from project root or check Docker COPY)."
    )
with open(_categories_json_path, encoding="utf-8") as _f:
    CATEGORIES = json.load(_f)

CATEGORY_DESCRIPTIONS = {
    "Основни намирници": "Pantry staples used for cooking: flour, sugar, salt, spices, oil, vinegar, pasta, rice, eggs, margarine. Raw ingredients, not ready-to-eat.",
    "Сосови, намази и конзерви": "Condiments, spreads and preserved goods: ketchup, mayo, mustard, sauces, dressings, jams, honey, pâtés, canned vegetables, canned fruit, canned meat/fish, soups in a can or powder.",
    "Пекара": "Baked goods: bread, bread rolls, croissants, pastries, tortillas, lavash, breadsticks, rusks, baking mixes. NOT cakes, cookies or sweet snacks.",
    "Слатки и грицки": "Sweets and snacks: chocolate, candy, gum, cookies, wafers, biscuits, napolitanke, rolls, pudding, whipped cream, chips, salted snacks, nuts, seeds, dried fruit.",
    "Млечни производи": "Dairy products: milk, yogurt, sour milk, cheese, butter, sour cream, milk-based desserts and drinks. NOT non-dairy plant milks (those go in Безалкохолни пијалоци).",
    "Месо и ладна трпеза": "Meat and deli: fresh meat (beef, pork, chicken, lamb), cured meats, salami, sausages, hot dogs. NOT canned/preserved meat (that goes in Сосови, намази и конзерви).",
    "Риба и морска храна": "Fresh and fresh-frozen fish and seafood only. NOT canned fish (that goes in Сосови, намази и конзерви).",
    "Овошје и зеленчук": "Fresh fruit and fresh vegetables only. NOT frozen, canned or preserved (those go in their respective categories).",
    "Замрзната храна": "Everything from the frozen aisle: frozen vegetables, frozen fruit, frozen meat, frozen fish, frozen pizza, frozen dough/pastry, frozen ready meals, ice cream.",
    "Топли пијалоци": "Hot beverages only: ground coffee, whole-bean coffee, instant coffee, 3-in-1 coffee sachets, coffee capsules/pods, tea (all kinds), cocoa, hot chocolate. NOT energy drinks, juices or cold beverages of any kind.",
    "Безалкохолни пијалоци": "Cold non-alcoholic drinks: still and sparkling water, carbonated soft drinks, fruit juices, iced tea, energy drinks, sports drinks, plant-based milks (soy, oat, almond).",
    "Алкохолни пијалоци": "Alcoholic drinks: beer, wine, sparkling wine/champagne, spirits, liqueurs, low-alcohol cocktail drinks.",
    "Здравје и исхрана": "Health and diet-specific products: gluten-free items, oats and muesli, protein powder, whey, vitamins, effervescent tablets, organic and wholegrain products marketed for health. Only use this when health/diet is the PRIMARY purpose.",
    "Лична хигиена": "Personal care: body wash, face wash, shampoo, conditioner, toothpaste, toothbrush, deodorant, feminine hygiene, shaving, razors, toilet paper, tissues, perfume.",
    "Домашна хемија": "Household cleaning: laundry detergent, fabric softener, dishwashing liquid, dishwasher tablets, all-purpose cleaners, bathroom/toilet cleaners, air fresheners, insecticides.",
    "Бебиња и деца": "Baby and child products: baby food, infant formula, diapers, baby skincare, children's toys.",
    "Миленици": "Pet products: dog food (dry/wet), cat food (dry/wet), pet treats, antiparasitic treatments, pet accessories.",
    "Тутун и никотин": "Tobacco and nicotine: cigarettes, rolling tobacco, nicotine pouches, vapes, e-cigarettes.",
    "Дом и домаќинство": "Home essentials: kitchen utensils, cookware, bags, cling film, foil, batteries, light bulbs, cleaning equipment, home textiles.",
    "Облека": "Clothing: adult clothing, children's clothing, socks, underwear, pyjamas.",
    "Разно": "Use ONLY if the product genuinely does not fit any other category. Includes stationery, seasonal items.",
}
