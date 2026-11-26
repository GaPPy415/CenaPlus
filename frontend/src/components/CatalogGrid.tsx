import { useRef } from "react";
import { RefreshCw } from "lucide-react";
import type { GroupedProduct } from "@/lib/api";
import ProductCard from "@/components/ProductCard";
import Pagination from "@/components/Pagination";

interface CatalogGridProps {
  items: GroupedProduct[] | undefined;
  isLoading: boolean;
  isError: boolean;
  isPlaceholderData: boolean;
  filterKey: string;
  page: number;
  totalPages: number;
  onPageChange: (next: number) => void;
  showCategory?: boolean;
  emptyMessage: string;
  onResetFilters?: () => void;
  errorTitle?: string;
  errorDescription?: string;
  onRetry?: () => void;
  className?: string;
}

const CatalogGrid = ({
  items,
  isLoading,
  isError,
  isPlaceholderData,
  filterKey,
  page,
  totalPages,
  onPageChange,
  showCategory = false,
  emptyMessage,
  onResetFilters,
  errorTitle = "Грешка при вчитување",
  errorDescription = "Проверете дали API серверот е активен",
  onRetry,
  className = "",
}: CatalogGridProps) => {
  const renderedFilterKeyRef = useRef(filterKey);
  if (!isPlaceholderData && items !== undefined) {
    renderedFilterKeyRef.current = filterKey;
  }
  const gridTransitionKey = renderedFilterKeyRef.current;

  if (isLoading && items === undefined) {
    return (
      <div
        className={`grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-3 min-[1440px]:grid-cols-4 ${className}`}
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
    );
  }

  if (isError && items === undefined) {
    return (
      <div className={`py-16 text-center ${className}`}>
        <p className="font-medium text-destructive">{errorTitle}</p>
        <p className="mt-1 text-sm text-muted-foreground">{errorDescription}</p>
        {onRetry && (
          <button
            type="button"
            onClick={onRetry}
            className="mt-4 inline-flex min-h-[44px] cursor-pointer items-center gap-2 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <RefreshCw className="h-4 w-4" aria-hidden="true" /> Обиди се повторно
          </button>
        )}
      </div>
    );
  }

  if (items === undefined) return null;

  return (
    <>
      {items.length === 0 ? (
        <div className={`rounded-lg border border-border bg-muted/30 p-6 text-center ${className}`}>
          <p className="text-sm text-muted-foreground">{emptyMessage}</p>
          {onResetFilters && (
            <button
              type="button"
              onClick={onResetFilters}
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
          } ${className}`}
        >
          {items.map((group) => (
            <ProductCard key={group.group_id} group={group} showCategory={showCategory} />
          ))}
        </div>
      )}

      <Pagination page={page} totalPages={totalPages} onPageChange={onPageChange} />
    </>
  );
};

export default CatalogGrid;
