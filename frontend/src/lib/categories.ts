import categoriesJson from "../../../shared/categories.json";
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
  vero: { name: "Веро", color: "hsl(var(--market-vero))" },
  dim: { name: "ДИМ Маркет", color: "hsl(var(--market-dim))" },
  elida: { name: "Елида", color: "hsl(var(--market-elida))" },
  zito: { name: "Жито Маркет", color: "hsl(var(--market-zito))" },
  zur: { name: "Зур", color: "hsl(var(--market-zur))" },
  kam: { name: "КАМ", color: "hsl(var(--market-kam))" },
  stokomak: { name: "Стокомак", color: "hsl(var(--market-stokomak))" },
  bigshop: { name: "BigShop", color: "hsl(var(--market-bigshop))" },
  kipper: { name: "Kipper", color: "hsl(var(--market-kipper))" },
  ramstore: { name: "Ramstore", color: "hsl(var(--market-ramstore))" },
  reptil: { name: "Reptil", color: "hsl(var(--market-reptil))" },
  spar: { name: "SPAR", color: "hsl(var(--market-spar))" },
};
