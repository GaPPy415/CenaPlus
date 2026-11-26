import type { SortBy, SortDir } from "@/lib/api";

export const PER_PAGE_OPTIONS = [12, 24, 36] as const;
export type PerPageOption = (typeof PER_PAGE_OPTIONS)[number];

export const SORT_OPTIONS: ReadonlyArray<{ value: SortBy; label: string }> = [
  { value: "name", label: "Име" },
  { value: "price", label: "Цена" },
  { value: "discount", label: "Попуст" },
  { value: "markets", label: "Достапност" },
];

export const DEFAULT_SORT_DIR: Record<SortBy, SortDir> = {
  name: "asc",
  price: "asc",
  discount: "desc",
  markets: "desc",
};

export const SORT_DIR_LABELS: Record<SortBy, { asc: string; desc: string }> = {
  name: { asc: "А → Ш (растечки)", desc: "Ш → А (опаѓачки)" },
  price: { asc: "Најевтини прво (растечки)", desc: "Најскапи прво (опаѓачки)" },
  discount: { asc: "Најмал попуст прво (растечки)", desc: "Најголем попуст прво (опаѓачки)" },
  markets: { asc: "Најмалку продавници (растечки)", desc: "Најмногу продавници (опаѓачки)" },
};

export function isSortBy(value: string | null): value is SortBy {
  return value === "name" || value === "price" || value === "discount" || value === "markets";
}

export function isSortDir(value: string | null): value is SortDir {
  return value === "asc" || value === "desc";
}
