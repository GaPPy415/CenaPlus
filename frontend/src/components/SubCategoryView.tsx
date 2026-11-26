import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { fetchProducts, type SortBy, type SortDir } from "@/lib/api";
import { CATEGORIES, CATEGORY_ICONS } from "@/lib/categories";
import { DEFAULT_SORT_DIR, type PerPageOption } from "@/lib/catalog";
import { PerPageControl, SortControl } from "@/components/CatalogControls";
import CatalogShell from "@/components/CatalogShell";
import { MarketFilterMobileButton, OnSaleFilterButton } from "@/components/MarketFilter";

interface Props {
  mainCategory: string;
  subCategory: string | null;
}

const SubCategoryView = ({ mainCategory, subCategory }: Props) => {
  const navigate = useNavigate();
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState<PerPageOption>(24);
  const [selectedMarkets, setSelectedMarkets] = useState<string[]>([]);
  const [onSaleOnly, setOnSaleOnly] = useState(false);
  const [sortBy, setSortBy] = useState<SortBy>("name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  // Full filter reset only when switching main categories; switching subcategory pills keeps filters.
  useEffect(() => {
    setPage(1);
    setSelectedMarkets([]);
    setOnSaleOnly(false);
    setSortBy("name");
    setSortDir("asc");
    setMobileFiltersOpen(false);
  }, [mainCategory]);

  useEffect(() => {
    setPage(1);
  }, [subCategory]);

  const selectSubCategory = (nextSub: string | null) => {
    if (nextSub === subCategory) return;
    const url = nextSub
      ? `/?category=${encodeURIComponent(mainCategory)}&sub=${encodeURIComponent(nextSub)}`
      : `/?category=${encodeURIComponent(mainCategory)}`;
    navigate(url);
  };

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

  const handleSortByChange = (next: SortBy) => {
    setSortBy(next);
    setSortDir(DEFAULT_SORT_DIR[next]);
    setPage(1);
  };

  const handleToggleSortDir = () => {
    setSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
    setPage(1);
  };

  const { data, isLoading, isError, isPlaceholderData } = useQuery({
    queryKey: [
      "products",
      mainCategory,
      subCategory ?? "__all__",
      page,
      perPage,
      selectedMarkets,
      onSaleOnly,
      sortBy,
      sortDir,
    ],
    queryFn: () =>
      fetchProducts(
        mainCategory,
        subCategory,
        page,
        perPage,
        selectedMarkets,
        onSaleOnly,
        sortBy,
        sortDir
      ),
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey?.[1] === mainCategory ? previousData : undefined,
  });

  const subCountMap = useMemo(() => {
    const map = new Map<string, number>();
    for (const item of data?.subcategories ?? []) {
      map.set(item.sub_category, item.count);
    }
    return map;
  }, [data?.subcategories]);

  const knownSubcategories = CATEGORIES[mainCategory] ?? [];
  const filterKey = `${mainCategory}|${subCategory ?? "__all__"}|${page}|${perPage}|${selectedMarkets.join(",")}|${onSaleOnly ? "sale" : "all"}|${sortBy}:${sortDir}`;
  const totalPages = data ? Math.ceil(data.total / data.per_page) : 0;
  const hasFilters = onSaleOnly || selectedMarkets.length > 0;
  const icon = CATEGORY_ICONS[mainCategory] || "📁";

  return (
    <CatalogShell
      selectedMarkets={selectedMarkets}
      onToggleMarket={toggleMarket}
      onResetMarkets={resetMarkets}
      onSaleOnly={onSaleOnly}
      onToggleOnSale={toggleOnSale}
      onResetSidebarFilters={resetAllFilters}
      mobileMarketsOpen={mobileFiltersOpen}
      mobilePanelId="subcat-mobile-markets-panel"
      items={data?.data}
      isLoading={isLoading}
      isError={isError}
      isPlaceholderData={isPlaceholderData}
      filterKey={filterKey}
      page={page}
      totalPages={totalPages}
      onPageChange={(next) => {
        setPage(next);
        window.scrollTo({ top: 0 });
      }}
      emptyMessage={
        onSaleOnly
          ? "Моментално нема производи на акција за избраните филтри."
          : subCategory
            ? "Нема производи за избраните продавници во оваа поткатегорија."
            : "Нема производи за избраните продавници во оваа категорија."
      }
      onResetAllFilters={hasFilters ? resetAllFilters : undefined}
    >
      {/* Breadcrumb */}
      <nav
        aria-label="Патека"
        className="mb-3 flex flex-wrap items-center gap-1.5 text-sm text-muted-foreground"
      >
        <Link to="/" className="hover:text-foreground hover:underline">
          Дома
        </Link>
        <span aria-hidden="true">/</span>
        {subCategory ? (
          <>
            <button
              type="button"
              onClick={() => selectSubCategory(null)}
              className="hover:text-foreground hover:underline"
            >
              {mainCategory}
            </button>
            <span aria-hidden="true">/</span>
            <span className="text-foreground">{subCategory}</span>
          </>
        ) : (
          <span className="text-foreground">{mainCategory}</span>
        )}
      </nav>

      {/* Header + toolbar */}
      <div className="mb-4 flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="flex items-center gap-2.5 font-heading text-2xl font-extrabold text-foreground sm:text-3xl">
            <span className="text-2xl sm:text-3xl" aria-hidden="true">
              {icon}
            </span>
            <span>{subCategory ?? mainCategory}</span>
          </h1>
          <p className="mt-1 min-h-[20px] text-sm tabular-nums text-muted-foreground">
            {data ? `${data.total} производи` : ""}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <OnSaleFilterButton active={onSaleOnly} onToggle={toggleOnSale} />
          <MarketFilterMobileButton
            open={mobileFiltersOpen}
            selectedCount={selectedMarkets.length}
            panelId="subcat-mobile-markets-panel"
            onToggle={() => setMobileFiltersOpen((open) => !open)}
          />
          <SortControl
            sortBy={sortBy}
            sortDir={sortDir}
            onSortByChange={handleSortByChange}
            onToggleDir={handleToggleSortDir}
          />
          <PerPageControl
            perPage={perPage}
            onChange={(next) => {
              setPerPage(next);
              setPage(1);
            }}
          />
        </div>
      </div>

      {/* Subcategory pill bar */}
      {knownSubcategories.length > 0 && (
        <div
          role="tablist"
          aria-label="Подкатегории"
          className="mb-6 flex flex-wrap gap-2"
        >
          <button
            type="button"
            role="tab"
            aria-selected={subCategory === null}
            onClick={() => selectSubCategory(null)}
            className={`inline-flex items-center gap-1.5 rounded-full border px-3.5 py-1.5 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
              subCategory === null
                ? "border-primary bg-primary text-primary-foreground shadow-sm"
                : "border-border bg-card text-foreground hover:border-primary/50 hover:bg-accent"
            }`}
          >
            <span>Сите</span>
            {data && (
              <span
                className={`rounded-full px-1.5 py-0.5 text-xs tabular-nums leading-none ${
                  subCategory === null
                    ? "bg-primary-foreground/20 text-primary-foreground"
                    : "bg-muted text-muted-foreground"
                }`}
              >
                {data.category_total}
              </span>
            )}
          </button>

          {knownSubcategories.map((sub) => {
            const active = subCategory === sub;
            const count = data ? (subCountMap.get(sub) ?? 0) : undefined;
            const isEmpty = count === 0;
            return (
              <button
                key={sub}
                type="button"
                role="tab"
                aria-selected={active}
                onClick={() => selectSubCategory(sub)}
                className={`inline-flex items-center gap-1.5 rounded-full border px-3.5 py-1.5 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                  active
                    ? "border-primary bg-primary text-primary-foreground shadow-sm"
                    : `border-border bg-card text-foreground hover:border-primary/50 hover:bg-accent${
                        isEmpty ? " opacity-60" : ""
                      }`
                }`}
              >
                <span>{sub}</span>
                {count !== undefined && (
                  <span
                    className={`rounded-full px-1.5 py-0.5 text-xs tabular-nums leading-none ${
                      active
                        ? "bg-primary-foreground/20 text-primary-foreground"
                        : "bg-muted text-muted-foreground"
                    }`}
                  >
                    {count}
                  </span>
                )}
              </button>
            );
          })}
        </div>
      )}
    </CatalogShell>
  );
};

export default SubCategoryView;
