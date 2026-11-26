import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { searchProducts } from "@/lib/api";
import { getGroupSaleSummary } from "@/lib/sale";
import ProductCard from "@/components/ProductCard";
import { Loader2, Percent } from "lucide-react";

const SearchResults = () => {
  const [searchParams] = useSearchParams();
  const query = searchParams.get("q") || "";
  const [onSaleOnly, setOnSaleOnly] = useState(false);

  useEffect(() => {
    setOnSaleOnly(false);
  }, [query]);

  const { data, isLoading, error } = useQuery({
    queryKey: ["search", query],
    queryFn: () => searchProducts(query),
    enabled: query.length > 0,
  });

  const saleGroups = useMemo(
    () => (data ? data.data.filter((g) => getGroupSaleSummary(g.products) !== null) : []),
    [data]
  );

  const visibleGroups = onSaleOnly ? saleGroups : (data?.data ?? []);

  return (
    <div className="mx-auto w-full max-w-[1600px] px-4 py-8 sm:px-6 xl:px-8">
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="font-heading font-extrabold text-2xl text-foreground">
            Резултати за „{query}"
          </h1>
          {data && (
            <p className="text-sm text-muted-foreground mt-0.5 tabular-nums">
              {onSaleOnly
                ? `${visibleGroups.length} на акција од ${data.data.length} резултати`
                : `${data.data.length} резултати`}
            </p>
          )}
        </div>

        {saleGroups.length > 0 && (
          <button
            type="button"
            onClick={() => setOnSaleOnly((prev) => !prev)}
            aria-pressed={onSaleOnly}
            className={`inline-flex min-h-[40px] cursor-pointer items-center gap-2 rounded-lg border px-3.5 py-1.5 text-sm font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
              onSaleOnly
                ? "border-sale-border bg-sale-soft text-sale-soft-foreground hover:bg-sale-soft/80"
                : "border-border bg-card text-foreground hover:bg-accent"
            }`}
          >
            <Percent
              className={`h-4 w-4 ${onSaleOnly ? "text-sale-soft-foreground" : "text-sale"}`}
              aria-hidden="true"
            />
            <span>Само на акција</span>
            <span
              className={`rounded-full px-1.5 py-0.5 text-xs font-semibold tabular-nums ${
                onSaleOnly
                  ? "bg-sale text-sale-foreground"
                  : "bg-muted text-muted-foreground"
              }`}
            >
              {saleGroups.length}
            </span>
          </button>
        )}
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="w-8 h-8 animate-spin text-primary" />
        </div>
      )}

      {error && (
        <div className="text-center py-20">
          <p className="text-destructive font-medium">Грешка при пребарување</p>
          <p className="text-sm text-muted-foreground mt-1">
            Проверете дали API серверот е активен
          </p>
        </div>
      )}

      {data && visibleGroups.length === 0 && (
        <div className="text-center py-20">
          <p className="text-muted-foreground">Нема резултати за „{query}"</p>
        </div>
      )}

      {data && visibleGroups.length > 0 && (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {visibleGroups.map((group) => (
            <ProductCard key={group.group_id} group={group} />
          ))}
        </div>
      )}
    </div>
  );
};

export default SearchResults;

