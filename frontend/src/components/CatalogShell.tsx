import type { ReactNode } from "react";
import type { GroupedProduct } from "@/lib/api";
import CatalogGrid from "@/components/CatalogGrid";
import {
  MarketFilterActivePills,
  MarketFilterMobileDrawer,
  MarketFilterSidebar,
} from "@/components/MarketFilter";

interface CatalogShellProps {
  selectedMarkets: string[];
  onToggleMarket: (key: string) => void;
  onResetMarkets: () => void;
  onSaleOnly: boolean;
  onToggleOnSale: () => void;
  onResetSidebarFilters: () => void;
  mobileMarketsOpen: boolean;
  mobilePanelId: string;
  pillsClassName?: string;
  drawerOpenMarginClass?: string;
  items: GroupedProduct[] | undefined;
  isLoading: boolean;
  isError: boolean;
  isPlaceholderData: boolean;
  filterKey: string;
  page: number;
  totalPages: number;
  onPageChange: (next: number) => void;
  showCategory?: boolean;
  gridClassName?: string;
  emptyMessage: string;
  onResetAllFilters?: () => void;
  errorTitle?: string;
  errorDescription?: string;
  onRetry?: () => void;
  children: ReactNode;
}

/** Shared responsive catalog layout (market sidebar + mobile drawer + product grid + pagination). */
const CatalogShell = ({
  selectedMarkets,
  onToggleMarket,
  onResetMarkets,
  onSaleOnly,
  onToggleOnSale,
  onResetSidebarFilters,
  mobileMarketsOpen,
  mobilePanelId,
  pillsClassName,
  drawerOpenMarginClass,
  items,
  isLoading,
  isError,
  isPlaceholderData,
  filterKey,
  page,
  totalPages,
  onPageChange,
  showCategory = false,
  gridClassName,
  emptyMessage,
  onResetAllFilters,
  errorTitle,
  errorDescription,
  onRetry,
  children,
}: CatalogShellProps) => (
  <main className="w-full py-8">
    <div className="mx-auto grid w-full max-w-[1600px] grid-cols-1 gap-6 px-4 sm:px-6 lg:grid-cols-[13.5rem_minmax(0,1fr)] xl:grid-cols-[14rem_minmax(0,1fr)] xl:gap-8 xl:px-8">
      <MarketFilterSidebar
        selectedMarkets={selectedMarkets}
        onToggleMarket={onToggleMarket}
        onResetMarkets={onResetMarkets}
        onSaleOnly={onSaleOnly}
        onToggleOnSale={onToggleOnSale}
        onResetAll={onResetSidebarFilters}
      />

      <div className="min-w-0">
        {children}

        <MarketFilterActivePills
          open={mobileMarketsOpen}
          selectedMarkets={selectedMarkets}
          onToggleMarket={onToggleMarket}
          onResetMarkets={onResetMarkets}
          className={pillsClassName}
        />
        <MarketFilterMobileDrawer
          open={mobileMarketsOpen}
          panelId={mobilePanelId}
          selectedMarkets={selectedMarkets}
          onToggleMarket={onToggleMarket}
          onResetMarkets={onResetMarkets}
          openMarginClass={drawerOpenMarginClass}
        />

        <CatalogGrid
          items={items}
          isLoading={isLoading}
          isError={isError}
          isPlaceholderData={isPlaceholderData}
          filterKey={filterKey}
          page={page}
          totalPages={totalPages}
          onPageChange={onPageChange}
          showCategory={showCategory}
          className={gridClassName}
          emptyMessage={emptyMessage}
          onResetFilters={onResetAllFilters}
          errorTitle={errorTitle}
          errorDescription={errorDescription}
          onRetry={onRetry}
        />
      </div>
    </div>
  </main>
);

export default CatalogShell;
