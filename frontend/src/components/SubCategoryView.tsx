import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { fetchProducts } from "@/lib/api";
import ProductCard from "@/components/ProductCard";
import Pagination from "@/components/Pagination";
import {
  MarketFilterActivePills,
  MarketFilterMobileButton,
  MarketFilterMobileDrawer,
  MarketFilterSidebar,
  OnSaleFilterButton,
} from "@/components/MarketFilter";

interface Props {
  mainCategory: string;
  subCategory: string;
}

const PER_PAGE_OPTIONS = [12, 24, 36] as const;

const SubCategoryView = ({ mainCategory, subCategory }: Props) => {
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState<12 | 24 | 36>(12);
  const [selectedMarkets, setSelectedMarkets] = useState<string[]>([]);
  const [onSaleOnly, setOnSaleOnly] = useState(false);
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  useEffect(() => {
    setPage(1);
    setSelectedMarkets([]);
    setOnSaleOnly(false);
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

  const toggleOnSale = () => {
    setOnSaleOnly((prev) => !prev);
    setPage(1);
  };

  const resetAllFilters = () => {
    setSelectedMarkets([]);
    setOnSaleOnly(false);
    setPage(1);
  };

  const { data, isLoading, error, isPlaceholderData } = useQuery({
    queryKey: ["products", mainCategory, subCategory, page, perPage, selectedMarkets, onSaleOnly],
    queryFn: () =>
      fetchProducts(mainCategory, subCategory, page, perPage, selectedMarkets, onSaleOnly),
    // Keep previous list mounted when toggling markets or paginating inside the
    // same subcategory so the page never flashes a spinner or collapses height.
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey?.[1] === mainCategory &&
      previousQuery?.queryKey?.[2] === subCategory
        ? previousData
        : undefined,
  });

  const requestedFilterKey = `${mainCategory}|${subCategory}|${page}|${perPage}|${selectedMarkets.join(",")}|${onSaleOnly ? "sale" : "all"}`;
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
        <MarketFilterSidebar
          selectedMarkets={selectedMarkets}
          onToggleMarket={toggleMarket}
          onResetMarkets={resetMarkets}
          onSaleOnly={onSaleOnly}
          onToggleOnSale={toggleOnSale}
          onResetAll={resetAllFilters}
        />

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
              {/* Mobile/tablet (< lg) 1-tap on-sale toggle + progressive-disclosure market filter */}
              <OnSaleFilterButton active={onSaleOnly} onToggle={toggleOnSale} />
              <MarketFilterMobileButton
                open={mobileFiltersOpen}
                selectedCount={selectedMarkets.length}
                panelId="subcat-mobile-markets-panel"
                onToggle={() => setMobileFiltersOpen((open) => !open)}
              />

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
          <MarketFilterActivePills
            open={mobileFiltersOpen}
            selectedMarkets={selectedMarkets}
            onToggleMarket={toggleMarket}
            onResetMarkets={resetMarkets}
          />

          {/* Mobile/tablet collapsible markets drawer */}
          <MarketFilterMobileDrawer
            open={mobileFiltersOpen}
            panelId="subcat-mobile-markets-panel"
            selectedMarkets={selectedMarkets}
            onToggleMarket={toggleMarket}
            onResetMarkets={resetMarkets}
          />

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
                <div className="rounded-lg border border-border bg-muted/30 p-6 text-center">
                  <p className="text-sm text-muted-foreground">
                    {onSaleOnly
                      ? "Моментално нема производи на акција за избраните филтри."
                      : "Нема производи за избраните продавници во оваа поткатегорија."}
                  </p>
                  {(onSaleOnly || selectedMarkets.length > 0) && (
                    <button
                      type="button"
                      onClick={resetAllFilters}
                      className="mt-3 inline-flex min-h-[38px] cursor-pointer items-center rounded-lg border border-border bg-card px-3.5 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    >
                      Прикажи ги сите производи
                    </button>
                  )}
                </div>
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
