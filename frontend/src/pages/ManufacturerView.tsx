import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import {
  ArrowLeft,
  Boxes,
  ChevronDown,
  RefreshCw,
  SlidersHorizontal,
  Store,
  Tags,
  X,
} from "lucide-react";

import {
  DEFAULT_SORT_DIR,
  isSortBy,
  isSortDir,
  PER_PAGE_OPTIONS,
  type PerPageOption,
} from "@/lib/catalog";
import { PerPageControl, SortControl } from "@/components/CatalogControls";
import CatalogShell from "@/components/CatalogShell";
import { MarketFilterMobileButton, OnSaleFilterButton } from "@/components/MarketFilter";
import { fetchManufacturer, type SortBy, type SortDir } from "@/lib/api";
import { formatDateTime } from "@/lib/utils";

const subCategoryKey = (mainCategory: string, subCategory: string) =>
  `${mainCategory}::${subCategory}`;

const ManufacturerView = () => {
  const [params, setParams] = useSearchParams();
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [mobileMarketsOpen, setMobileMarketsOpen] = useState(false);

  const name = params.get("manufacturer_name") ?? "";
  const page = Math.max(1, parseInt(params.get("page") ?? "1", 10) || 1);
  const perPageParam = parseInt(params.get("per_page") ?? "24", 10);
  const perPage: PerPageOption = (PER_PAGE_OPTIONS as readonly number[]).includes(perPageParam)
    ? (perPageParam as PerPageOption)
    : 24;
  const selectedSubs = params.getAll("sub").filter(Boolean);
  const selectedMarkets = params.getAll("market").filter(Boolean);
  const onSaleOnly = params.get("on_sale") === "1";

  const rawSort = params.get("sort");
  const sortBy: SortBy = isSortBy(rawSort) ? rawSort : "name";
  const rawOrder = params.get("order");
  const sortDir: SortDir = isSortDir(rawOrder) ? rawOrder : DEFAULT_SORT_DIR[sortBy];

  useEffect(() => {
    setFiltersOpen(false);
    setMobileMarketsOpen(false);
  }, [name]);

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

  const toggleSubCategory = (key: string) => {
    const nextSubs = selectedSubs.includes(key)
      ? selectedSubs.filter((item) => item !== key)
      : [...selectedSubs, key];
    setListParam("sub", nextSubs);
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

  const resetSidebarFilters = () => {
    const next = new URLSearchParams(params);
    next.delete("market");
    next.delete("on_sale");
    next.set("page", "1");
    setParams(next);
  };

  const resetAllFilters = () => {
    const next = new URLSearchParams(params);
    next.delete("sub");
    next.delete("market");
    next.delete("on_sale");
    next.set("page", "1");
    setParams(next);
  };

  const handleSortByChange = (nextSort: SortBy) => {
    updateParams({ sort: nextSort, order: DEFAULT_SORT_DIR[nextSort], page: "1" });
  };

  const handleToggleSortDir = () => {
    updateParams({ sort: sortBy, order: sortDir === "asc" ? "desc" : "asc", page: "1" });
  };

  const { data, isLoading, isError, isPlaceholderData, refetch } = useQuery({
    queryKey: [
      "manufacturer",
      name,
      page,
      perPage,
      selectedSubs,
      selectedMarkets,
      onSaleOnly,
      sortBy,
      sortDir,
    ],
    queryFn: () =>
      fetchManufacturer(
        name,
        page,
        perPage,
        selectedSubs,
        selectedMarkets,
        onSaleOnly,
        sortBy,
        sortDir
      ),
    enabled: Boolean(name),
    retry: 1,
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey?.[1] === name ? previousData : undefined,
  });

  const filterKey = `${name}|${page}|${perPage}|${selectedSubs.join(",")}|${selectedMarkets.join(",")}|${onSaleOnly ? "sale" : "all"}|${sortBy}:${sortDir}`;

  const duplicateSubNames = useMemo(() => {
    const counts = new Map<string, number>();
    for (const item of data?.subcategories ?? []) {
      counts.set(item.sub_category, (counts.get(item.sub_category) ?? 0) + 1);
    }
    return new Set(
      Array.from(counts.entries())
        .filter(([, count]) => count > 1)
        .map(([sub]) => sub)
    );
  }, [data?.subcategories]);

  const hasSubFilter = Boolean(data && data.subcategories.length > 1);
  const hasAnyFilter = onSaleOnly || selectedMarkets.length > 0 || selectedSubs.length > 0;

  const activeSubItems = useMemo(() => {
    if (!data || selectedSubs.length === 0) return [];
    return data.subcategories.filter((item) => {
      const key = subCategoryKey(item.main_category, item.sub_category);
      return selectedSubs.includes(key) || selectedSubs.includes(item.sub_category);
    });
  }, [data, selectedSubs]);

  const formatSubLabel = (mainCategory: string, subCategory: string) =>
    duplicateSubNames.has(subCategory) ? `${subCategory} (${mainCategory})` : subCategory;

  const totalPages = data ? Math.ceil(data.total / data.per_page) : 0;

  if (!name) {
    return (
      <main className="container mx-auto max-w-6xl px-4 py-12">
        <h1 className="font-heading text-2xl font-bold">Недостасува производител</h1>
        <p className="mt-2 text-muted-foreground">
          Страницата бара параметар <code>manufacturer_name</code>.
        </p>
        <Link
          to="/"
          className="mt-6 inline-flex min-h-[44px] items-center gap-2 text-sm font-medium text-primary hover:underline"
        >
          <ArrowLeft className="h-4 w-4" aria-hidden="true" /> Кон почетна
        </Link>
      </main>
    );
  }

  return (
    <CatalogShell
      selectedMarkets={selectedMarkets}
      onToggleMarket={toggleMarket}
      onResetMarkets={() => setListParam("market", [])}
      onSaleOnly={onSaleOnly}
      onToggleOnSale={toggleOnSale}
      onResetSidebarFilters={resetSidebarFilters}
      mobileMarketsOpen={mobileMarketsOpen}
      mobilePanelId="manufacturer-mobile-markets-panel"
      pillsClassName="mt-3"
      drawerOpenMarginClass="mt-3"
      items={data?.data}
      isLoading={isLoading}
      isError={isError}
      isPlaceholderData={isPlaceholderData}
      filterKey={filterKey}
      page={page}
      totalPages={totalPages}
      onPageChange={(next) => updateParams({ page: String(next) }, true)}
      showCategory
      gridClassName="mt-4"
      emptyMessage={
        onSaleOnly
          ? "Моментално нема производи на акција за избраните филтри."
          : hasAnyFilter
            ? "Нема производи за избраните филтри од овој производител."
            : "Нема производи од овој производител во моментов."
      }
      onResetAllFilters={hasAnyFilter ? resetAllFilters : undefined}
      errorTitle="Не можевме да ги вчитаме производите"
      errorDescription="Проверете дали API серверот е активен, или производителот повеќе не постои."
      onRetry={() => refetch()}
    >
      {/* Breadcrumb */}
          <nav
            aria-label="Патека"
            className="mb-5 flex flex-wrap items-center gap-1.5 text-sm text-muted-foreground"
          >
            <Link to="/" className="hover:text-foreground hover:underline">
              Дома
            </Link>
            <span aria-hidden="true">/</span>
            <span className="text-foreground">Производител</span>
          </nav>

          {/* Manufacturer header */}
          <header className="rounded-xl border border-border border-l-4 border-l-primary bg-card p-5 shadow-sm">
            <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-primary">
              <Store className="h-3.5 w-3.5" aria-hidden="true" />
              Производител
            </p>
            {isLoading && !data ? (
              <div
                className="mt-1.5 h-8 w-48 animate-pulse rounded-md bg-muted sm:h-9"
                aria-hidden="true"
              />
            ) : (
              <h1 className="mt-1 font-heading text-2xl font-extrabold capitalize text-foreground sm:text-3xl">
                {data?.manufacturer.display_name ?? name}
              </h1>
            )}
            <p className="mt-1 text-sm text-muted-foreground">
              Сите производи од овој производител, низ сите категории
            </p>

            {data ? (
              <dl className="mt-4 grid grid-cols-2 gap-3 sm:max-w-2xl sm:grid-cols-4">
                <div>
                  <dt className="flex items-center gap-1 text-xs text-muted-foreground">
                    <Boxes className="h-3 w-3" aria-hidden="true" /> Производи
                  </dt>
                  <dd className="text-lg font-bold tabular-nums text-foreground">
                    {data.manufacturer.groups}
                  </dd>
                </div>
                <div>
                  <dt className="flex items-center gap-1 text-xs text-muted-foreground">
                    <Tags className="h-3 w-3" aria-hidden="true" /> Категории
                  </dt>
                  <dd className="text-lg font-bold tabular-nums text-foreground">
                    {data.manufacturer.categories}
                  </dd>
                </div>
                <div>
                  <dt className="flex items-center gap-1 text-xs text-muted-foreground">
                    <Store className="h-3 w-3" aria-hidden="true" /> Продавници
                  </dt>
                  <dd className="text-lg font-bold tabular-nums text-foreground">
                    {data.manufacturer.markets}
                  </dd>
                </div>
                <div>
                  <dt className="flex items-center gap-1 text-xs text-muted-foreground">
                    <RefreshCw className="h-3 w-3" aria-hidden="true" /> Последно ажурирање
                  </dt>
                  <dd className="text-base font-bold tabular-nums text-foreground sm:text-lg">
                    {formatDateTime(data.manufacturer.last_updated) ?? "—"}
                  </dd>
                </div>
              </dl>
            ) : isLoading ? (
              <div
                className="mt-4 grid grid-cols-2 gap-3 sm:max-w-2xl sm:grid-cols-4"
                aria-hidden="true"
              >
                {Array.from({ length: 4 }).map((_, idx) => (
                  <div key={idx} className="space-y-1.5">
                    <div className="h-3.5 w-16 animate-pulse rounded bg-muted" />
                    <div className="h-6 w-10 animate-pulse rounded bg-muted" />
                  </div>
                ))}
              </div>
            ) : null}
          </header>

          {/* Controls toolbar */}
          {data && (data.total > 0 || hasAnyFilter || hasSubFilter || data.manufacturer.groups > 0) && (
            <div className="mt-6 flex flex-wrap items-center justify-between gap-3">
              <div className="flex flex-wrap items-center gap-3">
                {hasSubFilter && (
                  <button
                    type="button"
                    onClick={() => setFiltersOpen((open) => !open)}
                    aria-expanded={filtersOpen}
                    aria-controls="manufacturer-subcategories-panel"
                    className={`inline-flex min-h-[44px] cursor-pointer items-center gap-2 rounded-lg border px-3.5 py-2 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                      filtersOpen || selectedSubs.length > 0
                        ? "border-primary/40 bg-primary/10 text-foreground hover:bg-primary/15"
                        : "border-border bg-card text-foreground hover:bg-accent"
                    }`}
                  >
                    <SlidersHorizontal className="h-4 w-4 text-primary" aria-hidden="true" />
                    <span>Поткатегории</span>
                    <span
                      className={`rounded-full px-2 py-0.5 text-xs font-semibold tabular-nums ${
                        selectedSubs.length > 0
                          ? "bg-primary text-primary-foreground"
                          : "bg-muted text-muted-foreground"
                      }`}
                    >
                      {selectedSubs.length > 0 ? selectedSubs.length : data.subcategories.length}
                    </span>
                    <ChevronDown
                      className={`h-4 w-4 text-muted-foreground transition-transform duration-200 ease-out motion-reduce:transition-none ${
                        filtersOpen ? "rotate-180" : ""
                      }`}
                      aria-hidden="true"
                    />
                  </button>
                )}

                <OnSaleFilterButton active={onSaleOnly} onToggle={toggleOnSale} />

                <MarketFilterMobileButton
                  open={mobileMarketsOpen}
                  selectedCount={selectedMarkets.length}
                  panelId="manufacturer-mobile-markets-panel"
                  onToggle={() => setMobileMarketsOpen((open) => !open)}
                />

                <p className="text-sm tabular-nums text-muted-foreground">
                  {hasAnyFilter
                    ? `Прикажани ${data.total} од ${data.manufacturer.groups} производи`
                    : `${data.total} производи`}
                </p>
              </div>

              {data.total > 0 && (
                <div className="flex flex-wrap items-center gap-3">
                  <SortControl
                    sortBy={sortBy}
                    sortDir={sortDir}
                    onSortByChange={handleSortByChange}
                    onToggleDir={handleToggleSortDir}
                  />
                  <PerPageControl
                    perPage={perPage}
                    onChange={(next) => updateParams({ per_page: String(next), page: "1" })}
                  />
                </div>
              )}
            </div>
          )}

          {/* Active subcategory pills when drawer is collapsed */}
          {hasSubFilter && !filtersOpen && activeSubItems.length > 0 && (
            <div
              aria-label="Активни филтри по поткатегорија"
              className="mt-3 flex flex-wrap items-center gap-2"
            >
              {activeSubItems.map((item) => {
                const key = subCategoryKey(item.main_category, item.sub_category);
                return (
                  <button
                    key={key}
                    type="button"
                    onClick={() => toggleSubCategory(key)}
                    aria-label={`Отстрани филтер ${item.sub_category}`}
                    className="inline-flex min-h-[36px] cursor-pointer items-center gap-1.5 rounded-full bg-primary/10 px-3 py-1 text-xs font-medium text-primary transition-colors duration-150 hover:bg-primary/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    <span>{formatSubLabel(item.main_category, item.sub_category)}</span>
                    <span className="tabular-nums opacity-75">({item.count})</span>
                    <X className="h-3.5 w-3.5" aria-hidden="true" />
                  </button>
                );
              })}
              <button
                type="button"
                onClick={() => setListParam("sub", [])}
                className="inline-flex min-h-[36px] cursor-pointer items-center gap-1 rounded-full px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors duration-150 hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                Ресетирај сè
              </button>
            </div>
          )}

          {/* Collapsible subcategory filter drawer */}
          {hasSubFilter && data && (
            <div
              id="manufacturer-subcategories-panel"
              aria-hidden={!filtersOpen}
              className={`grid transition-[grid-template-rows,opacity] duration-200 ease-out motion-reduce:transition-none ${
                filtersOpen ? "mt-3 grid-rows-[1fr] opacity-100" : "grid-rows-[0fr] opacity-0"
              }`}
            >
              <div className={`overflow-hidden ${filtersOpen ? "visible" : "invisible"}`}>
                <section
                  aria-label="Филтер по поткатегорија"
                  className="rounded-xl border border-border bg-card/70 p-4 shadow-sm"
                >
                  <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                    <p className="text-xs font-medium text-muted-foreground">
                      Изберете една или повеќе поткатегории за да ги филтрирате резултатите
                    </p>
                    {selectedSubs.length > 0 && (
                      <button
                        type="button"
                        onClick={() => setListParam("sub", [])}
                        className="inline-flex cursor-pointer items-center gap-1 rounded-md px-2 py-1 text-xs font-medium text-muted-foreground transition-colors duration-150 hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                      >
                        <X className="h-3.5 w-3.5" aria-hidden="true" />
                        Ресетирај ({selectedSubs.length})
                      </button>
                    )}
                  </div>

                  <div className="flex flex-wrap items-center gap-2">
                    <button
                      type="button"
                      onClick={() => setListParam("sub", [])}
                      aria-pressed={selectedSubs.length === 0}
                      className={`inline-flex min-h-[36px] cursor-pointer items-center gap-1.5 rounded-full border px-3 py-1 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                        selectedSubs.length === 0
                          ? "border-transparent bg-primary text-primary-foreground"
                          : "border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground"
                      }`}
                    >
                      <span>Сите</span>
                      <span
                        className={`rounded-full px-1.5 py-0.5 text-xs tabular-nums ${
                          selectedSubs.length === 0
                            ? "bg-primary-foreground/20 text-primary-foreground"
                            : "bg-muted text-muted-foreground"
                        }`}
                      >
                        {data.manufacturer.groups}
                      </span>
                    </button>

                    {data.subcategories.map((item) => {
                      const key = subCategoryKey(item.main_category, item.sub_category);
                      const active =
                        selectedSubs.includes(key) || selectedSubs.includes(item.sub_category);
                      const label = formatSubLabel(item.main_category, item.sub_category);

                      return (
                        <button
                          key={key}
                          type="button"
                          onClick={() => toggleSubCategory(key)}
                          aria-pressed={active}
                          title={`${item.main_category} / ${item.sub_category}`}
                          className={`inline-flex min-h-[36px] cursor-pointer items-center gap-1.5 rounded-full border px-3 py-1 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                            active
                              ? "border-transparent bg-primary text-primary-foreground"
                              : "border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground"
                          }`}
                        >
                          <span>{label}</span>
                          <span
                            className={`rounded-full px-1.5 py-0.5 text-xs tabular-nums ${
                              active
                                ? "bg-primary-foreground/20 text-primary-foreground"
                                : "bg-muted text-muted-foreground"
                            }`}
                          >
                            {item.count}
                          </span>
                        </button>
                      );
                    })}
                  </div>
                </section>
              </div>
            </div>
          )}
    </CatalogShell>
  );
};

export default ManufacturerView;
