import categoriesJson from "../../shared/categories.json";
export const CATEGORIES: Record<string, string[]> = categoriesJson;

export const CATEGORY_ICONS: Record<string, string> = {
  "Основни намирници": "🌾",
  "Сосови, намази и конзерви": "🥫",
  "Пекара": "🍞",
  "Слатки и грицки": "🍫",
  "Млечни производи": "🥛",
  "Месо и ладна трпеза": "🥩",
  "Риба и морска храна": "🐟",
  "Овошје и зеленчук": "🥬",
  "Замрзната храна": "🧊",
  "Топли пијалоци": "☕",
  "Безалкохолни пијалоци": "🥤",
  "Алкохолни пијалоци": "🍷",
  "Здравје и исхрана": "💊",
  "Лична хигиена": "🧴",
  "Домашна хемија": "🧹",
  "Бебиња и деца": "👶",
  "Миленици": "🐾",
  "Тутун и никотин": "🚬",
  "Дом и домаќинство": "🏠",
  "Облека": "👕",
  "Разно": "📦",
};

export const MARKET_INFO: Record<string, { name: string; color: string }> = {
  reptil: { name: "Reptil", color: "hsl(var(--market-reptil))" },
  zito: { name: "Жито Маркет", color: "hsl(var(--market-zito))" },
  kam: { name: "КАМ", color: "hsl(var(--market-kam))" },
  ramstore: { name: "Ramstore", color: "hsl(var(--market-ramstore))" },
  vero: { name: "Веро", color: "hsl(var(--market-vero))" },
  bigshop: { name: "BigShop", color: "hsl(var(--market-bigshop))" },
  stokomak: { name: "Стокомак", color: "hsl(var(--market-stokomak))" },
};
