import { Check, ChevronDown, Percent, SlidersHorizontal, Store, X } from "lucide-react";
import { MARKET_INFO } from "@/lib/categories";

const ALL_MARKETS = Object.keys(MARKET_INFO).sort((a, b) =>
  MARKET_INFO[a].name.localeCompare(MARKET_INFO[b].name, "mk")
);

interface SidebarProps {
  selectedMarkets: string[];
  onToggleMarket: (key: string) => void;
  onResetMarkets: () => void;
  onSaleOnly?: boolean;
  onToggleOnSale?: () => void;
  onResetAll?: () => void;
}

export const MarketFilterSidebar = ({
  selectedMarkets,
  onToggleMarket,
  onResetMarkets,
  onSaleOnly = false,
  onToggleOnSale,
  onResetAll,
}: SidebarProps) => {
  const activeFilterCount = selectedMarkets.length + (onSaleOnly ? 1 : 0);
  const handleReset = onResetAll ?? onResetMarkets;

  return (
    <aside
      aria-label="Филтри"
      className="hidden lg:sticky lg:top-24 lg:block lg:self-start"
    >
      <div className="w-full rounded-xl border border-border bg-card p-3.5 shadow-sm xl:p-4">
        {onToggleOnSale && (
          <div className="mb-3 border-b border-border pb-3">
            <button
              type="button"
              role="switch"
              aria-checked={onSaleOnly}
              onClick={onToggleOnSale}
              className={`flex min-h-[42px] w-full cursor-pointer items-center justify-between gap-2 rounded-lg border px-2.5 py-2 text-left text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                onSaleOnly
                  ? "border-sale-border bg-sale-soft text-sale-soft-foreground"
                  : "border-border bg-background text-foreground hover:bg-accent/50"
              }`}
            >
              <span className="flex min-w-0 items-center gap-2">
                <Percent
                  className={`h-4 w-4 shrink-0 ${
                    onSaleOnly ? "text-sale-soft-foreground" : "text-sale"
                  }`}
                  aria-hidden="true"
                />
                <span className="truncate">Само на акција</span>
              </span>
              <span
                aria-hidden="true"
                className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors duration-150 motion-reduce:transition-none ${
                  onSaleOnly ? "bg-sale" : "bg-muted-foreground/25"
                }`}
              >
                <span
                  className={`inline-block h-3.5 w-3.5 rounded-full bg-white shadow-sm transition-transform duration-150 motion-reduce:transition-none ${
                    onSaleOnly ? "translate-x-[18px]" : "translate-x-[3px]"
                  }`}
                />
              </span>
            </button>
          </div>
        )}

        <div className="mb-3 flex items-center justify-between gap-2 border-b border-border pb-2.5">
          <h2 className="flex min-w-0 items-center gap-2 text-xs font-semibold uppercase tracking-wider text-foreground">
            <Store className="h-3.5 w-3.5 shrink-0 text-primary" aria-hidden="true" />
            <span className="truncate">Продавници</span>
          </h2>
          {selectedMarkets.length > 0 && (
            <span className="shrink-0 rounded-full bg-primary px-2 py-0.5 text-[11px] font-semibold tabular-nums text-primary-foreground">
              {selectedMarkets.length}
            </span>
          )}
        </div>

        <div className="space-y-1">
          {ALL_MARKETS.map((key) => {
            const info = MARKET_INFO[key];
            const active = selectedMarkets.includes(key);
            return (
              <button
                key={key}
                type="button"
                onClick={() => onToggleMarket(key)}
                aria-pressed={active}
                className={`flex min-h-[38px] w-full cursor-pointer items-center gap-2.5 rounded-lg px-2.5 py-1.5 text-left text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                  active
                    ? "bg-accent/80 text-foreground"
                    : "text-muted-foreground hover:bg-accent/50 hover:text-foreground"
                }`}
              >
                <span
                  aria-hidden="true"
                  className={`flex h-4 w-4 shrink-0 items-center justify-center rounded border transition-colors duration-150 ${
                    active ? "border-transparent text-white" : "border-border bg-background"
                  }`}
                  style={active ? { backgroundColor: info.color } : undefined}
                >
                  {active && <Check className="h-3 w-3 stroke-[3]" />}
                </span>
                <span
                  aria-hidden="true"
                  className="h-2 w-2 shrink-0 rounded-full"
                  style={{ backgroundColor: info.color }}
                />
                <span className="truncate">{info.name}</span>
              </button>
            );
          })}
        </div>

        <div
          aria-hidden={activeFilterCount === 0}
          className={`grid transition-[grid-template-rows,opacity,margin] duration-150 ease-out motion-reduce:transition-none ${
            activeFilterCount > 0
              ? "mt-3 grid-rows-[1fr] opacity-100"
              : "mt-0 grid-rows-[0fr] opacity-0"
          }`}
        >
          <div className={`overflow-hidden ${activeFilterCount > 0 ? "visible" : "invisible"}`}>
            <div className="border-t border-border pt-3">
              <button
                type="button"
                onClick={handleReset}
                className="flex min-h-[34px] w-full cursor-pointer items-center justify-center gap-1.5 rounded-lg border border-border bg-muted/50 px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors duration-150 hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <X className="h-3.5 w-3.5 shrink-0" aria-hidden="true" />
                <span>Ресетирај ({activeFilterCount})</span>
              </button>
            </div>
          </div>
        </div>
      </div>
    </aside>
  );
};

interface OnSaleButtonProps {
  active: boolean;
  onToggle: () => void;
  className?: string;
}

export const OnSaleFilterButton = ({
  active,
  onToggle,
  className = "lg:hidden",
}: OnSaleButtonProps) => (
  <button
    type="button"
    onClick={onToggle}
    aria-pressed={active}
    className={`inline-flex min-h-[44px] cursor-pointer items-center gap-2 rounded-lg border px-3.5 py-2 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
      active
        ? "border-sale-border bg-sale-soft text-sale-soft-foreground hover:bg-sale-soft/80"
        : "border-border bg-card text-foreground hover:bg-accent"
    } ${className}`}
  >
    <Percent
      className={`h-4 w-4 ${active ? "text-sale-soft-foreground" : "text-sale"}`}
      aria-hidden="true"
    />
    <span>На акција</span>
  </button>
);

interface MobileButtonProps {
  open: boolean;
  selectedCount: number;
  panelId: string;
  onToggle: () => void;
}

export const MarketFilterMobileButton = ({
  open,
  selectedCount,
  panelId,
  onToggle,
}: MobileButtonProps) => (
  <button
    type="button"
    onClick={onToggle}
    aria-expanded={open}
    aria-controls={panelId}
    className={`inline-flex min-h-[44px] cursor-pointer items-center gap-2 rounded-lg border px-3.5 py-2 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring lg:hidden ${
      open || selectedCount > 0
        ? "border-primary/40 bg-primary/10 text-foreground hover:bg-primary/15"
        : "border-border bg-card text-foreground hover:bg-accent"
    }`}
  >
    <SlidersHorizontal className="h-4 w-4 text-primary" aria-hidden="true" />
    <span>Продавници</span>
    {selectedCount > 0 && (
      <span className="rounded-full bg-primary px-2 py-0.5 text-xs font-semibold tabular-nums text-primary-foreground">
        {selectedCount}
      </span>
    )}
    <ChevronDown
      className={`h-4 w-4 text-muted-foreground transition-transform duration-200 ease-out motion-reduce:transition-none ${
        open ? "rotate-180" : ""
      }`}
      aria-hidden="true"
    />
  </button>
);

interface ActivePillsProps {
  open: boolean;
  selectedMarkets: string[];
  onToggleMarket: (key: string) => void;
  onResetMarkets: () => void;
  className?: string;
}

export const MarketFilterActivePills = ({
  open,
  selectedMarkets,
  onToggleMarket,
  onResetMarkets,
  className = "mb-4",
}: ActivePillsProps) => {
  if (open || selectedMarkets.length === 0) return null;
  return (
    <div
      aria-label="Активни продавници"
      className={`flex flex-wrap items-center gap-2 lg:hidden ${className}`}
    >
      {selectedMarkets.map((key) => {
        const info = MARKET_INFO[key];
        if (!info) return null;
        return (
          <button
            key={key}
            type="button"
            onClick={() => onToggleMarket(key)}
            aria-label={`Отстрани ${info.name}`}
            className="inline-flex min-h-[36px] cursor-pointer items-center gap-1.5 rounded-full border border-transparent px-3 py-1 text-xs font-medium text-primary-foreground transition-opacity duration-150 hover:opacity-90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            style={{ backgroundColor: info.color }}
          >
            <span>{info.name}</span>
            <X className="h-3.5 w-3.5" aria-hidden="true" />
          </button>
        );
      })}
      <button
        type="button"
        onClick={onResetMarkets}
        className="inline-flex min-h-[36px] cursor-pointer items-center rounded-full px-2.5 py-1 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
      >
        Ресетирај сè
      </button>
    </div>
  );
};

interface MobileDrawerProps {
  open: boolean;
  panelId: string;
  selectedMarkets: string[];
  onToggleMarket: (key: string) => void;
  onResetMarkets: () => void;
  openMarginClass?: string;
}

export const MarketFilterMobileDrawer = ({
  open,
  panelId,
  selectedMarkets,
  onToggleMarket,
  onResetMarkets,
  openMarginClass = "mb-6",
}: MobileDrawerProps) => (
  <div
    id={panelId}
    aria-hidden={!open}
    className={`grid transition-[grid-template-rows,opacity] duration-200 ease-out motion-reduce:transition-none lg:hidden ${
      open ? `${openMarginClass} grid-rows-[1fr] opacity-100` : "grid-rows-[0fr] opacity-0"
    }`}
  >
    <div className={`overflow-hidden ${open ? "visible" : "invisible"}`}>
      <div className="rounded-xl border border-border bg-card p-4 shadow-sm">
        <div className="mb-3 flex items-center justify-between gap-2">
          <span className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            <Store className="h-3.5 w-3.5 text-primary" aria-hidden="true" />
            Продавници
          </span>
          {selectedMarkets.length > 0 && (
            <button
              type="button"
              onClick={onResetMarkets}
              className="inline-flex cursor-pointer items-center gap-1 rounded-md px-2 py-1 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
            >
              <X className="h-3.5 w-3.5" aria-hidden="true" />
              Ресетирај ({selectedMarkets.length})
            </button>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          {ALL_MARKETS.map((key) => {
            const info = MARKET_INFO[key];
            const active = selectedMarkets.includes(key);
            return (
              <button
                key={key}
                type="button"
                onClick={() => onToggleMarket(key)}
                aria-pressed={active}
                className={`inline-flex min-h-[36px] cursor-pointer items-center gap-1.5 rounded-full border px-3 py-1 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                  active
                    ? "border-transparent text-primary-foreground"
                    : "border-border bg-background text-muted-foreground hover:bg-accent hover:text-foreground"
                }`}
                style={active ? { backgroundColor: info.color } : undefined}
              >
                <span
                  aria-hidden="true"
                  className="h-2 w-2 shrink-0 rounded-full"
                  style={{
                    backgroundColor: active ? "currentColor" : info.color,
                  }}
                />
                {info.name}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  </div>
);
