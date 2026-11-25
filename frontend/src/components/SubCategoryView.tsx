import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { Check, ChevronDown, SlidersHorizontal, Store, X } from "lucide-react";
import { fetchProducts } from "@/lib/api";
import ProductCard from "@/components/ProductCard";
import { MARKET_INFO } from "@/lib/categories";
import Pagination from "@/components/Pagination";

interface Props {
  mainCategory: string;
  subCategory: string;
}

const PER_PAGE_OPTIONS = [12, 24, 36] as const;
const ALL_MARKETS = Object.keys(MARKET_INFO);

const SubCategoryView = ({ mainCategory, subCategory }: Props) => {
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState<12 | 24 | 36>(12);
  const [selectedMarkets, setSelectedMarkets] = useState<string[]>([]);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  useEffect(() => {
    setPage(1);
    setSelectedMarkets([]);
    setMobileFiltersOpen(false);
  }, [mainCategory, subCategory]);

  const toggleMarket = (key: string) => {
    setSelectedMarkets((prev) =>
      prev.includes(key) ? prev.filter((m) => m !== key) : [...prev, key]
    );
    setPage(1);
  };

  const resetMarkets = () => {
    setSelectedMarkets([]);
    setPage(1);
  };

  const { data, isLoading, error, isPlaceholderData } = useQuery({
    queryKey: ["products", mainCategory, subCategory, page, perPage, selectedMarkets],
    queryFn: () => fetchProducts(mainCategory, subCategory, page, perPage, selectedMarkets),
    // Keep previous list mounted when toggling markets or paginating inside the
    // same subcategory so the page never flashes a spinner or collapses height.
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey?.[1] === mainCategory &&
      previousQuery?.queryKey?.[2] === subCategory
        ? previousData
        : undefined,
  });

  const requestedFilterKey = `${mainCategory}|${subCategory}|${page}|${perPage}|${selectedMarkets.join(",")}`;
  const renderedFilterKeyRef = useRef(requestedFilterKey);
  if (!isPlaceholderData && data) {
    renderedFilterKeyRef.current = requestedFilterKey;
  }
  const gridTransitionKey = renderedFilterKeyRef.current;

  const totalPages = data ? Math.ceil(data.total / data.per_page) : 0;

  return (
    <div className="w-full py-8">
      {/*
        Responsive 3-column symmetric layout on wide desktop (>=1440px):
        - Col 1 (1fr): left gutter with the sticky market sidebar centered between 0px and the cards.
        - Col 2 (center): product header, grid, and pagination dead-center in the viewport.
        - Col 3 (1fr): symmetric right gutter keeping Col 2 strictly centered.
        On 1024-1439px (lg), falls back to a clean 2-column sidebar + grid layout.
        Below 1024px, uses the compact collapsible market filter drawer.
      */}
      <div className="mx-auto grid w-full max-w-6xl grid-cols-1 gap-6 px-4 lg:max-w-7xl lg:grid-cols-[13.5rem_minmax(0,1fr)] min-[1440px]:max-w-none min-[1440px]:grid-cols-[1fr_minmax(0,62rem)_1fr] min-[1440px]:gap-0 min-[1440px]:px-0 min-[1680px]:grid-cols-[1fr_minmax(0,72rem)_1fr]">
        {/* Desktop left sticky sidebar: centered in the left gutter on >=1440px */}
        <aside
          aria-label="Филтер по продавници"
          className="hidden lg:sticky lg:top-24 lg:block lg:self-start min-[1440px]:col-start-1 min-[1440px]:justify-self-center min-[1440px]:px-4"
        >
          <div className="w-52 rounded-xl border border-border bg-card p-3.5 shadow-sm min-[1680px]:w-56 min-[1680px]:p-4">
            <div className="mb-3 flex items-center justify-between gap-2 border-b border-border pb-2.5">
              <h2 className="flex min-w-0 items-center gap-2 text-xs font-semibold uppercase tracking-wider text-foreground">
                <Store className="h-3.5 w-3.5 shrink-0 text-primary" aria-hidden="true" />
                <span className="truncate">Продавници</span>
              </h2>
              {selectedMarkets.length > 0 && (
                <span className="shrink-0 rounded-full bg-primary px-2 py-0.5 text-[11px] font-semibold tabular-nums text-primary-foreground">
                  {selectedMarkets.length}
                </span>
              )}
            </div>

            <div className="space-y-1">
              {ALL_MARKETS.map((key) => {
                const info = MARKET_INFO[key];
                const active = selectedMarkets.includes(key);
                return (
                  <button
                    key={key}
                    type="button"
                    onClick={() => toggleMarket(key)}
                    aria-pressed={active}
                    className={`flex min-h-[38px] w-full cursor-pointer items-center gap-2.5 rounded-lg px-2.5 py-1.5 text-left text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                      active
                        ? "bg-accent/80 text-foreground"
                        : "text-muted-foreground hover:bg-accent/50 hover:text-foreground"
                    }`}
                  >
                    <span
                      aria-hidden="true"
                      className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border transition-colors duration-150 ${
                        active ? "border-transparent text-white" : "border-border bg-background"
                      }`}
                      style={active ? { backgroundColor: info.color } : undefined}
                    >
                      {active && <Check className="h-3 w-3 stroke-[3]" />}
                    </span>
                    <span
                      aria-hidden="true"
                      className="h-2 w-2 shrink-0 rounded-full"
                      style={{ backgroundColor: info.color }}
                    />
                    <span className="truncate">{info.name}</span>
                  </button>
                );
              })}
            </div>

            <div
              aria-hidden={selectedMarkets.length === 0}
              className={`grid transition-[grid-template-rows,opacity,margin] duration-150 ease-out motion-reduce:transition-none ${
                selectedMarkets.length > 0
                  ? "mt-3 grid-rows-[1fr] opacity-100"
                  : "mt-0 grid-rows-[0fr] opacity-0"
              }`}
            >
              <div className={`overflow-hidden ${selectedMarkets.length > 0 ? "visible" : "invisible"}`}>
                <div className="border-t border-border pt-3">
                  <button
                    type="button"
                    onClick={resetMarkets}
                    className="flex min-h-[34px] w-full cursor-pointer items-center justify-center gap-1.5 rounded-lg border border-border bg-muted/50 px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors duration-150 hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    <X className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
                    <span>Ресетирај ({selectedMarkets.length})</span>
                  </button>
                </div>
              </div>
            </div>
          </div>
        </aside>

        {/* Main centered content column */}
        <div className="min-w-0 min-[1440px]:col-start-2 min-[1440px]:px-4">
          {/* Header */}
          <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="text-xs font-semibold uppercase tracking-wider text-primary">
                {mainCategory}
              </p>
              <h1 className="mt-0.5 font-heading text-2xl font-extrabold text-foreground sm:text-3xl">
                {subCategory}
              </h1>
              <p className="mt-1 min-h-[20px] text-sm tabular-nums text-muted-foreground">
                {data ? `${data.total} производи` : ""}
              </p>
            </div>

            <div className="flex flex-wrap items-center gap-3">
              {/* Mobile/tablet (< lg) progressive-disclosure market filter toggle */}
              <button
                type="button"
                onClick={() => setMobileFiltersOpen((open) => !open)}
                aria-expanded={mobileFiltersOpen}
                aria-controls="subcat-mobile-markets-panel"
                className={`inline-flex min-h-[44px] cursor-pointer items-center gap-2 rounded-lg border px-3.5 py-2 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring lg:hidden ${
                  mobileFiltersOpen || selectedMarkets.length > 0
                    ? "border-primary/40 bg-primary/10 text-foreground hover:bg-primary/15"
                    : "border-border bg-card text-foreground hover:bg-accent"
                }`}
              >
                <SlidersHorizontal className="h-4 w-4 text-primary" aria-hidden="true" />
                <span>Продавници</span>
                {selectedMarkets.length > 0 && (
                  <span className="rounded-full bg-primary px-2 py-0.5 text-xs font-semibold tabular-nums text-primary-foreground">
                    {selectedMarkets.length}
                  </span>
                )}
                <ChevronDown
                  className={`h-4 w-4 text-muted-foreground transition-transform duration-200 ease-out motion-reduce:transition-none ${
                    mobileFiltersOpen ? "rotate-180" : ""
                  }`}
                  aria-hidden="true"
                />
              </button>

              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">По страна:</span>
                {PER_PAGE_OPTIONS.map((n) => (
                  <button
                    key={n}
                    type="button"
                    onClick={() => {
                      setPerPage(n);
                      setPage(1);
                    }}
                    aria-pressed={perPage === n}
                    className={`min-h-[36px] cursor-pointer rounded-md px-3 py-1 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                      perPage === n
                        ? "bg-primary text-primary-foreground"
                        : "bg-muted text-muted-foreground hover:bg-accent"
                    }`}
                  >
                    {n}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Mobile/tablet active filter pills when drawer is collapsed */}
          {!mobileFiltersOpen && selectedMarkets.length > 0 && (
            <div
              aria-label="Активни продавници"
              className="mb-4 flex flex-wrap items-center gap-2 lg:hidden"
            >
              {selectedMarkets.map((key) => {
                const info = MARKET_INFO[key];
                if (!info) return null;
                return (
                  <button
                    key={key}
                    type="button"
                    onClick={() => toggleMarket(key)}
                    aria-label={`Отстрани ${info.name}`}
                    className="inline-flex min-h-[36px] cursor-pointer items-center gap-1.5 rounded-full border border-transparent px-3 py-1 text-xs font-medium text-primary-foreground transition-opacity duration-150 hover:opacity-90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    style={{ backgroundColor: info.color }}
                  >
                    <span>{info.name}</span>
                    <X className="h-3.5 w-3.5" aria-hidden="true" />
                  </button>
                );
              })}
              <button
                type="button"
                onClick={resetMarkets}
                className="inline-flex min-h-[36px] cursor-pointer items-center rounded-full px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
              >
                Ресетирај сè
              </button>
            </div>
          )}

          {/* Mobile/tablet collapsible markets drawer */}
          <div
            id="subcat-mobile-markets-panel"
            aria-hidden={!mobileFiltersOpen}
            className={`grid transition-[grid-template-rows,opacity] duration-200 ease-out motion-reduce:transition-none lg:hidden ${
              mobileFiltersOpen ? "mb-6 grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0"
            }`}
          >
            <div className={`overflow-hidden ${mobileFiltersOpen ? "visible" : "invisible"}`}>
              <div className="rounded-xl border border-border bg-card p-4 shadow-sm">
                <div className="mb-3 flex items-center justify-between gap-2">
                  <span className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                    <Store className="h-3.5 w-3.5 text-primary" aria-hidden="true" />
                    Продавници
                  </span>
                  {selectedMarkets.length > 0 && (
                    <button
                      type="button"
                      onClick={resetMarkets}
                      className="inline-flex cursor-pointer items-center gap-1 rounded-md px-2 py-1 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                    >
                      <X className="h-3.5 w-3.5" aria-hidden="true" />
                      Ресетирај ({selectedMarkets.length})
                    </button>
                  )}
                </div>
                <div className="flex flex-wrap gap-2">
                  {ALL_MARKETS.map((key) => {
                    const info = MARKET_INFO[key];
                    const active = selectedMarkets.includes(key);
                    return (
                      <button
                        key={key}
                        type="button"
                        onClick={() => toggleMarket(key)}
                        aria-pressed={active}
                        className={`inline-flex min-h-[36px] cursor-pointer items-center gap-1.5 rounded-full border px-3 py-1 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                          active
                            ? "border-transparent text-primary-foreground"
                            : "border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground"
                        }`}
                        style={active ? { backgroundColor: info.color } : undefined}
                      >
                        <span
                          aria-hidden="true"
                          className="h-2 w-2 shrink-0 rounded-full"
                          style={{
                            backgroundColor: active ? "currentColor" : info.color,
                          }}
                        />
                        {info.name}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>
          </div>

          {/* Loading skeleton (initial category load only) */}
          {isLoading && !data && (
            <div
              className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3"
              aria-busy="true"
            >
              <span className="sr-only">Се вчитуваат производите…</span>
              {Array.from({ length: 6 }).map((_, index) => (
                <div
                  key={index}
                  className="h-80 animate-pulse rounded-xl border border-border bg-muted/50"
                />
              ))}
            </div>
          )}

          {/* Error */}
          {error && !data && (
            <div className="py-20 text-center">
              <p className="font-medium text-destructive">Грешка при вчитување</p>
              <p className="mt-1 text-sm text-muted-foreground">
                Проверете дали API серверот е активен
              </p>
            </div>
          )}

          {/* Products grid */}
          {data && (
            <>
              {data.data.length === 0 ? (
                <p className="rounded-lg border border-border bg-muted/30 p-6 text-center text-sm text-muted-foreground">
                  Нема производи за избраните продавници во оваа поткатегорија.
                </p>
              ) : (
                <div
                  key={gridTransitionKey}
                  aria-busy={isPlaceholderData}
                  className={`grid grid-cols-1 gap-4 transition-opacity duration-150 motion-reduce:animate-none motion-reduce:transition-none sm:grid-cols-2 xl:grid-cols-3 ${
                    isPlaceholderData ? "opacity-65" : "animate-grid-swap opacity-100"
                  }`}
                >
                  {data.data.map((group) => (
                    <ProductCard key={group.group_id} group={group} />
                  ))}
                </div>
              )}

              <Pagination
                page={page}
                totalPages={totalPages}
                onPageChange={(next) => {
                  setPage(next);
                  window.scrollTo({ top: 0 });
                }}
              />
            </>
          )}
        </div>
      </div>
    </div>
  );
};

export default SubCategoryView;
