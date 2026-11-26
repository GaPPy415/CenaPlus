import { ArrowDownWideNarrow, ArrowUpNarrowWide, ChevronDown } from "lucide-react";
import type { SortBy, SortDir } from "@/lib/api";
import {
  PER_PAGE_OPTIONS,
  SORT_DIR_LABELS,
  SORT_OPTIONS,
  type PerPageOption,
} from "@/lib/catalog";

interface SortControlProps {
  sortBy: SortBy;
  sortDir: SortDir;
  onSortByChange: (next: SortBy) => void;
  onToggleDir: () => void;
}

export const SortControl = ({
  sortBy,
  sortDir,
  onSortByChange,
  onToggleDir,
}: SortControlProps) => {
  const dirLabel = SORT_DIR_LABELS[sortBy][sortDir];

  return (
    <div className="flex items-center gap-1.5">
      <label htmlFor="catalog-sort-select" className="text-sm text-muted-foreground">
        Подреди:
      </label>
      <div className="relative">
        <select
          id="catalog-sort-select"
          value={sortBy}
          onChange={(e) => onSortByChange(e.target.value as SortBy)}
          className="min-h-[36px] cursor-pointer appearance-none rounded-md border border-border bg-card py-1 pl-3 pr-8 text-sm font-medium text-foreground transition-colors duration-150 hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          {SORT_OPTIONS.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
        <ChevronDown
          className="pointer-events-none absolute right-2 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground"
          aria-hidden="true"
        />
      </div>
      <button
        type="button"
        onClick={onToggleDir}
        title={dirLabel}
        aria-label={`Насока на подредување: ${dirLabel}. Кликни за промена.`}
        aria-pressed={sortDir === "desc"}
        className="inline-flex min-h-[36px] min-w-[36px] cursor-pointer items-center justify-center rounded-md border border-border bg-card px-2 text-foreground transition-colors duration-150 hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        {sortDir === "asc" ? (
          <ArrowUpNarrowWide className="h-4 w-4 text-primary" aria-hidden="true" />
        ) : (
          <ArrowDownWideNarrow className="h-4 w-4 text-primary" aria-hidden="true" />
        )}
      </button>
    </div>
  );
};

interface PerPageControlProps {
  perPage: PerPageOption;
  onChange: (next: PerPageOption) => void;
}

export const PerPageControl = ({ perPage, onChange }: PerPageControlProps) => (
  <div className="flex items-center gap-1.5">
    <span className="text-sm text-muted-foreground">По страна:</span>
    {PER_PAGE_OPTIONS.map((option) => (
      <button
        key={option}
        type="button"
        onClick={() => onChange(option)}
        aria-pressed={perPage === option}
        className={`min-h-[36px] cursor-pointer rounded-md px-3 py-1 text-sm font-medium tabular-nums transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
          perPage === option
            ? "bg-primary text-primary-foreground"
            : "bg-muted text-muted-foreground hover:bg-accent"
        }`}
      >
        {option}
      </button>
    ))}
  </div>
);
