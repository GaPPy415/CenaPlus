import type { SearchSortBy, SortBy, SortDir } from "@/lib/api";

export const PER_PAGE_OPTIONS = [12, 24, 36] as const;
export type PerPageOption = (typeof PER_PAGE_OPTIONS)[number];

export const SORT_OPTIONS: ReadonlyArray<{ value: SortBy; label: string }> = [
  { value: "name", label: "Име" },
  { value: "price", label: "Цена" },
  { value: "discount", label: "Попуст - %" },
  { value: "discount_amount", label: "Попуст - ден." },
  { value: "markets", label: "Достапност" },
];

export const SEARCH_SORT_OPTIONS: ReadonlyArray<{ value: SearchSortBy; label: string }> = [
  { value: "relevance", label: "Релевантност" },
  ...SORT_OPTIONS,
];

export const DEFAULT_SORT_DIR: Record<SortBy, SortDir> = {
  name: "asc",
  price: "asc",
  discount: "desc",
  discount_amount: "desc",
  markets: "desc",
};

export const DEFAULT_SEARCH_SORT_DIR: Record<SearchSortBy, SortDir> = {
  relevance: "desc",
  ...DEFAULT_SORT_DIR,
};

export const SORT_DIR_LABELS: Record<SortBy, { asc: string; desc: string }> = {
  name: { asc: "А → Ш (растечки)", desc: "Ш → А (опаѓачки)" },
  price: { asc: "Најевтини прво (растечки)", desc: "Најскапи прво (опаѓачки)" },
  discount: { asc: "Најмал попуст (%) прво (растечки)", desc: "Најголем попуст (%) прво (опаѓачки)" },
  discount_amount: {
    asc: "Најмал попуст (ден.) прво (растечки)",
    desc: "Најголем попуст (ден.) прво (опаѓачки)",
  },
  markets: { asc: "Најмалку продавници (растечки)", desc: "Најмногу продавници (опаѓачки)" },
};

export const SEARCH_SORT_DIR_LABELS: Record<SearchSortBy, { asc: string; desc: string }> = {
  relevance: { asc: "Најмала релевантност (растечки)", desc: "Најрелевантни прво (опаѓачки)" },
  ...SORT_DIR_LABELS,
};

export function isSortBy(value: string | null): value is SortBy {
  return (
    value === "name" ||
    value === "price" ||
    value === "discount" ||
    value === "discount_amount" ||
    value === "markets"
  );
}

export function isSearchSortBy(value: string | null): value is SearchSortBy {
  return value === "relevance" || isSortBy(value);
}

export function isSortDir(value: string | null): value is SortDir {
  return value === "asc" || value === "desc";
}
