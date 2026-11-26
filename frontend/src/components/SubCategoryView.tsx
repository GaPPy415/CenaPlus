import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { fetchProducts, type SortBy, type SortDir } from "@/lib/api";
import { DEFAULT_SORT_DIR, type PerPageOption } from "@/lib/catalog";
import { PerPageControl, SortControl } from "@/components/CatalogControls";
import CatalogShell from "@/components/CatalogShell";
import { MarketFilterMobileButton, OnSaleFilterButton } from "@/components/MarketFilter";

interface Props {
  mainCategory: string;
  subCategory: string;
}

const SubCategoryView = ({ mainCategory, subCategory }: Props) => {
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState<PerPageOption>(24);
  const [selectedMarkets, setSelectedMarkets] = useState<string[]>([]);
  const [onSaleOnly, setOnSaleOnly] = useState(false);
  const [sortBy, setSortBy] = useState<SortBy>("name");
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const [mobileFiltersOpen, setMobileFiltersOpen] = useState(false);

  useEffect(() => {
    setPage(1);
    setSelectedMarkets([]);
    setOnSaleOnly(false);
    setSortBy("name");
    setSortDir("asc");
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
      subCategory,
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
      previousQuery?.queryKey?.[1] === mainCategory &&
      previousQuery?.queryKey?.[2] === subCategory
        ? previousData
        : undefined,
  });

  const filterKey = `${mainCategory}|${subCategory}|${page}|${perPage}|${selectedMarkets.join(",")}|${onSaleOnly ? "sale" : "all"}|${sortBy}:${sortDir}`;
  const totalPages = data ? Math.ceil(data.total / data.per_page) : 0;
  const hasFilters = onSaleOnly || selectedMarkets.length > 0;

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
          : "Нема производи за избраните продавници во оваа поткатегорија."
      }
      onResetAllFilters={hasFilters ? resetAllFilters : undefined}
    >
      {/* Header + toolbar */}
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
    </CatalogShell>
  );
};

export default SubCategoryView;
