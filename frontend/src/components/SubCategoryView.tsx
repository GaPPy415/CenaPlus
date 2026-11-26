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
  const [perPage, setPerPage] = useState<12 | 24 | 36>(24);
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
        Responsive 2-column catalog layout:
        - <1024px: single column with compact mobile filter bar & drawer.
        - >=1024px: sticky left filter sidebar + fluid product grid (2 -> 3 -> 4 cols)
          up to max-w-[1600px] so wide monitors use screen real estate effectively.
      */}
      <div className="mx-auto grid w-full max-w-[1600px] grid-cols-1 gap-6 px-4 sm:px-6 lg:grid-cols-[13.5rem_minmax(0,1fr)] xl:grid-cols-[14rem_minmax(0,1fr)] xl:gap-8 xl:px-8">
        {/* Desktop left sticky sidebar */}
        <MarketFilterSidebar
          selectedMarkets={selectedMarkets}
          onToggleMarket={toggleMarket}
          onResetMarkets={resetMarkets}
          onSaleOnly={onSaleOnly}
          onToggleOnSale={toggleOnSale}
          onResetAll={resetAllFilters}
        />

        {/* Main catalog column */}
        <div className="min-w-0">
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
              className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 min-[1440px]:grid-cols-4"
              aria-busy="true"
            >
              <span className="sr-only">Се вчитуваат производите…</span>
              {Array.from({ length: 8 }).map((_, index) => (
                <div
                  key={index}
                  className="h-72 animate-pulse rounded-xl border border-border bg-muted/50"
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
                  className={`grid grid-cols-1 gap-4 transition-opacity duration-150 motion-reduce:animate-none motion-reduce:transition-none sm:grid-cols-2 xl:grid-cols-3 min-[1440px]:grid-cols-4 ${
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
