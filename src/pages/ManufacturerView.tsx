import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { ArrowLeft, Boxes, RefreshCw, Store, Tags } from "lucide-react";

import ProductCard from "@/components/ProductCard";
import Pagination from "@/components/Pagination";
import { fetchManufacturer } from "@/lib/api";

const PER_PAGE_OPTIONS = [12, 24, 36] as const;

const ManufacturerView = () => {
  const [params, setParams] = useSearchParams();
  const name = params.get("manufacturer_name") ?? "";
  const page = Math.max(1, parseInt(params.get("page") ?? "1", 10) || 1);
  const perPageParam = parseInt(params.get("per_page") ?? "12", 10);
  const perPage = (PER_PAGE_OPTIONS as readonly number[]).includes(perPageParam)
    ? (perPageParam as 12 | 24 | 36)
    : 12;

  const updateParams = (patch: Record<string, string>) => {
    const next = new URLSearchParams(params);
    Object.entries(patch).forEach(([key, value]) => next.set(key, value));
    setParams(next);
    window.scrollTo({ top: 0 });
  };

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ["manufacturer", name, page, perPage],
    queryFn: () => fetchManufacturer(name, page, perPage),
    enabled: Boolean(name),
    retry: 1,
  });

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
    <main className="container mx-auto max-w-6xl px-4 py-8">
      {/* Breadcrumb */}
      <nav aria-label="Патека" className="mb-5 flex flex-wrap items-center gap-1.5 text-sm text-muted-foreground">
        <Link to="/" className="hover:text-foreground hover:underline">
          Дома
        </Link>
        <span aria-hidden="true">/</span>
        <span className="text-foreground">Производител</span>
      </nav>

      {/* Manufacturer header — deliberately different from a subcategory header */}
      <header className="rounded-xl border border-border border-l-4 border-l-primary bg-card p-5 shadow-sm">
        <p className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wider text-primary">
          <Store className="h-3.5 w-3.5" aria-hidden="true" />
          Производител
        </p>
        <h1 className="mt-1 font-heading text-2xl font-extrabold capitalize text-foreground sm:text-3xl">
          {data?.manufacturer.display_name ?? name}
        </h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Сите производи од овој производител, низ сите категории
        </p>

        {data && (
          <dl className="mt-4 grid grid-cols-3 gap-3 sm:max-w-md">
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
          </dl>
        )}
      </header>

      {/* Per page */}
      {data && data.total > 0 && (
        <div className="mt-6 flex items-center justify-end gap-2">
          <span className="text-sm text-muted-foreground">По страна:</span>
          {PER_PAGE_OPTIONS.map((option) => (
            <button
              key={option}
              type="button"
              onClick={() => updateParams({ per_page: String(option), page: "1" })}
              aria-pressed={perPage === option}
              className={`rounded-md px-3 py-1 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring ${
                perPage === option
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted text-muted-foreground hover:bg-accent"
              }`}
            >
              {option}
            </button>
          ))}
        </div>
      )}

      {/* Loading skeleton */}
      {isLoading && (
        <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3" aria-busy="true">
          <span className="sr-only">Се вчитуваат производите…</span>
          {Array.from({ length: 6 }).map((_, index) => (
            <div key={index} className="h-80 animate-pulse rounded-xl border border-border bg-muted/50" />
          ))}
        </div>
      )}

      {/* Error */}
      {isError && (
        <div className="mt-10 text-center">
          <p className="font-medium text-destructive">Не можевме да ги вчитаме производите</p>
          <p className="mt-1 text-sm text-muted-foreground">
            Проверете дали API серверот е активен, или производителот повеќе не постои.
          </p>
          <button
            type="button"
            onClick={() => refetch()}
            className="mt-4 inline-flex min-h-[44px] items-center gap-2 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <RefreshCw className="h-4 w-4" aria-hidden="true" /> Обиди се повторно
          </button>
        </div>
      )}

      {/* Products */}
      {data && (
        <>
          {data.data.length === 0 ? (
            <p className="mt-10 rounded-lg border border-border bg-muted/30 p-6 text-center text-sm text-muted-foreground">
              Нема производи од овој производител во моментов.
            </p>
          ) : (
            <div className="mt-4 grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {data.data.map((group) => (
                <ProductCard key={group.group_id} group={group} showCategory />
              ))}
            </div>
          )}

          <Pagination
            page={page}
            totalPages={totalPages}
            onPageChange={(next) => updateParams({ page: String(next) })}
          />
        </>
      )}
    </main>
  );
};

export default ManufacturerView;
