import { useState } from "react";
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from "lucide-react";

interface PaginationProps {
  page: number;
  totalPages: number;
  onPageChange: (page: number) => void;
}

/**
 * Shared page controls (first/prev, numbered with ellipsis, next/last, go-to-page).
 * Returns null for a single page so callers don't need the guard.
 */
const Pagination = ({ page, totalPages, onPageChange }: PaginationProps) => {
  const [pageInput, setPageInput] = useState("");

  if (totalPages <= 1) return null;

  const pages: (number | "...")[] = [];
  if (totalPages <= 7) {
    for (let i = 1; i <= totalPages; i++) pages.push(i);
  } else {
    pages.push(1);
    if (page > 3) pages.push("...");
    for (let i = Math.max(2, page - 1); i <= Math.min(totalPages - 1, page + 1); i++) {
      pages.push(i);
    }
    if (page < totalPages - 2) pages.push("...");
    pages.push(totalPages);
  }

  const goTo = (value: string) => {
    const parsed = parseInt(value, 10);
    if (parsed >= 1 && parsed <= totalPages) onPageChange(parsed);
  };

  return (
    <nav aria-label="Пагинација" className="mt-8 flex flex-col items-center gap-3">
      <div className="flex items-center gap-1">
        <button
          type="button"
          onClick={() => onPageChange(1)}
          disabled={page === 1}
          aria-label="Прва страна"
          className="rounded-lg bg-muted p-2 text-muted-foreground transition-colors hover:bg-accent disabled:opacity-30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <ChevronsLeft className="h-4 w-4" aria-hidden="true" />
        </button>
        <button
          type="button"
          onClick={() => onPageChange(Math.max(1, page - 1))}
          disabled={page === 1}
          aria-label="Претходна страна"
          className="rounded-lg bg-muted p-2 text-muted-foreground transition-colors hover:bg-accent disabled:opacity-30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <ChevronLeft className="h-4 w-4" aria-hidden="true" />
        </button>

        {pages.map((value, index) =>
          value === "..." ? (
            <span key={`ellipsis-${index}`} className="px-2 text-sm text-muted-foreground" aria-hidden="true">
              …
            </span>
          ) : (
            <button
              key={value}
              type="button"
              onClick={() => onPageChange(value)}
              aria-label={`Страна ${value}`}
              aria-current={page === value ? "page" : undefined}
              className={`h-9 w-9 rounded-lg text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                page === value
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted text-muted-foreground hover:bg-accent"
              }`}
            >
              {value}
            </button>
          )
        )}

        <button
          type="button"
          onClick={() => onPageChange(Math.min(totalPages, page + 1))}
          disabled={page === totalPages}
          aria-label="Следна страна"
          className="rounded-lg bg-muted p-2 text-muted-foreground transition-colors hover:bg-accent disabled:opacity-30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <ChevronRight className="h-4 w-4" aria-hidden="true" />
        </button>
        <button
          type="button"
          onClick={() => onPageChange(totalPages)}
          disabled={page === totalPages}
          aria-label="Последна страна"
          className="rounded-lg bg-muted p-2 text-muted-foreground transition-colors hover:bg-accent disabled:opacity-30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <ChevronsRight className="h-4 w-4" aria-hidden="true" />
        </button>
      </div>

      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <label htmlFor="go-to-page">Оди на страна</label>
        <input
          id="go-to-page"
          type="number"
          min={1}
          max={totalPages}
          value={pageInput}
          onChange={(e) => setPageInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") goTo(pageInput);
          }}
          onBlur={() => goTo(pageInput)}
          placeholder={String(page)}
          className="h-8 w-16 rounded-md border border-border bg-background text-center text-sm font-medium text-foreground outline-none focus:ring-2 focus:ring-primary [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
        />
        <span>од {totalPages}</span>
      </div>
    </nav>
  );
};

export default Pagination;
