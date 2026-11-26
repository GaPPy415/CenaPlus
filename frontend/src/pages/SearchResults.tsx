import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { searchProducts, type SearchSortBy, type SortDir } from "@/lib/api";
import {
  DEFAULT_SEARCH_SORT_DIR,
  isSearchSortBy,
  isSortDir,
  PER_PAGE_OPTIONS,
  SEARCH_SORT_OPTIONS,
  type PerPageOption,
} from "@/lib/catalog";
import { PerPageControl, SortControl } from "@/components/CatalogControls";
import CatalogShell from "@/components/CatalogShell";
import { MarketFilterMobileButton, OnSaleFilterButton } from "@/components/MarketFilter";

const SEARCH_MAX_RESULTS = 500;

const SearchResults = () => {
  const [params, setParams] = useSearchParams();
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  const query = (params.get("q") ?? "").trim();
  const page = Math.max(1, parseInt(params.get("page") ?? "1", 10) || 1);
  const perPageParam = parseInt(params.get("per_page") ?? "24", 10);
  const perPage: PerPageOption = (PER_PAGE_OPTIONS as readonly number[]).includes(perPageParam)
    ? (perPageParam as PerPageOption)
    : 24;
  const selectedMarkets = params.getAll("market").filter(Boolean);
  const onSaleOnly = params.get("on_sale") === "1";

  const rawSort = params.get("sort");
  const sortBy: SearchSortBy = isSearchSortBy(rawSort) ? rawSort : "relevance";
  const rawOrder = params.get("order");
  const sortDir: SortDir = isSortDir(rawOrder) ? rawOrder : DEFAULT_SEARCH_SORT_DIR[sortBy];

  useEffect(() => {
    setMobileFiltersOpen(false);
  }, [query]);

  const updateParams = (patch: Record<string, string | null>, scrollToTop = false) => {
    const next = new URLSearchParams(params);
    Object.entries(patch).forEach(([key, value]) => {
      if (value === null) next.delete(key);
      else next.set(key, value);
    });
    setParams(next);
    if (scrollToTop) window.scrollTo({ top: 0 });
  };

  const setListParam = (key: string, values: string[]) => {
    const next = new URLSearchParams(params);
    next.delete(key);
    values.forEach((v) => next.append(key, v));
    next.set("page", "1");
    setParams(next);
  };

  const toggleMarket = (key: string) => {
    const nextMarkets = selectedMarkets.includes(key)
      ? selectedMarkets.filter((m) => m !== key)
      : [...selectedMarkets, key];
    setListParam("market", nextMarkets);
  };

  const toggleOnSale = () => {
    updateParams({ on_sale: onSaleOnly ? null : "1", page: "1" });
  };

  const resetAllFilters = () => {
    const next = new URLSearchParams(params);
    next.delete("market");
    next.delete("on_sale");
    next.set("page", "1");
    setParams(next);
  };

  const handleSortByChange = (nextSort: SearchSortBy) => {
    updateParams({ sort: nextSort, order: DEFAULT_SEARCH_SORT_DIR[nextSort], page: "1" });
  };

  const handleToggleSortDir = () => {
    updateParams({ sort: sortBy, order: sortDir === "asc" ? "desc" : "asc", page: "1" });
  };

  const { data, isLoading, isError, isPlaceholderData, refetch } = useQuery({
    queryKey: [
      "search",
      query,
      page,
      perPage,
      selectedMarkets,
      onSaleOnly,
      sortBy,
      sortDir,
    ],
    queryFn: () =>
      searchProducts(query, page, perPage, selectedMarkets, onSaleOnly, sortBy, sortDir),
    enabled: query.length > 0,
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey?.[1] === query ? previousData : undefined,
  });

  if (!query) {
    return (
      <main className="mx-auto w-full max-w-[1600px] px-4 py-16 text-center sm:px-6 xl:px-8">
        <p className="text-muted-foreground">Внесете поим за пребарување.</p>
      </main>
    );
  }

  const filterKey = `${query}|${page}|${perPage}|${selectedMarkets.join(",")}|${onSaleOnly ? "sale" : "all"}|${sortBy}:${sortDir}`;
  const totalPages = data ? Math.ceil(data.total / data.per_page) : 0;
  const hasFilters = onSaleOnly || selectedMarkets.length > 0;

  return (
    <CatalogShell
      selectedMarkets={selectedMarkets}
      onToggleMarket={toggleMarket}
      onResetMarkets={() => setListParam("market", [])}
      onSaleOnly={onSaleOnly}
      onToggleOnSale={toggleOnSale}
      onResetSidebarFilters={resetAllFilters}
      mobileMarketsOpen={mobileFiltersOpen}
      mobilePanelId="search-mobile-markets-panel"
      items={data?.data}
      isLoading={isLoading}
      isError={isError}
      isPlaceholderData={isPlaceholderData}
      filterKey={filterKey}
      page={page}
      totalPages={totalPages}
      onPageChange={(next) => updateParams({ page: String(next) }, true)}
      showCategory
      emptyMessage={
        hasFilters
          ? `Нема резултати за „${query}" со избраните филтри.`
          : `Нема резултати за „${query}"`
      }
      onResetAllFilters={hasFilters ? resetAllFilters : undefined}
      errorTitle="Грешка при пребарување"
      onRetry={() => refetch()}
    >
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="font-heading text-2xl font-extrabold text-foreground sm:text-3xl">
            Резултати за „{query}"
          </h1>
          <p className="mt-1 min-h-[20px] text-sm tabular-nums text-muted-foreground">
            {data
              ? data.total >= SEARCH_MAX_RESULTS
                ? `Прикажани првите ${SEARCH_MAX_RESULTS} резултати`
                : `${data.total} резултати`
              : ""}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <OnSaleFilterButton active={onSaleOnly} onToggle={toggleOnSale} />
          <MarketFilterMobileButton
            open={mobileFiltersOpen}
            selectedCount={selectedMarkets.length}
            panelId="search-mobile-markets-panel"
            onToggle={() => setMobileFiltersOpen((open) => !open)}
          />
          <SortControl
            sortBy={sortBy}
            sortDir={sortDir}
            onSortByChange={handleSortByChange}
            onToggleDir={handleToggleSortDir}
            options={SEARCH_SORT_OPTIONS}
          />
          <PerPageControl
            perPage={perPage}
            onChange={(next) => updateParams({ per_page: String(next), page: "1" })}
          />
        </div>
      </div>
    </CatalogShell>
  );
};

export default SearchResults;
