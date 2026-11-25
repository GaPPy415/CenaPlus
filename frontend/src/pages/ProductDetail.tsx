import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  ChevronRight,
  ExternalLink,
  ImageOff,
  RefreshCw,
  Store,
  TrendingDown,
} from "lucide-react";

import { MARKET_INFO } from "@/lib/categories";
import { fetchProductDetail, type ProductOffer } from "@/lib/api";

const UNIT_LABELS: Record<string, string> = {
  g: "г",
  kg: "кг",
  ml: "мл",
  l: "л",
  pcs: "пар",
};

const formatNumber = (value: number) =>
  Number.isInteger(value) ? String(value) : String(Number(value.toFixed(3)));

const formatQuantity = (value: number | null, unit: string | null) => {
  if (value === null || !unit) return null;
  return `${formatNumber(value)} ${UNIT_LABELS[unit] ?? unit}`;
};

const formatDate = (value: string | null) => {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toLocaleDateString("mk-MK");
};

const marketOf = (key: string) =>
  MARKET_INFO[key] ?? { name: key, color: "hsl(var(--muted-foreground))" };

const OfferRow = ({ offer }: { offer: ProductOffer }) => {
  const market = marketOf(offer.market);
  const updated = formatDate(offer.last_updated);

  return (
    <li className="price-row gap-3">
      <a
        href={offer.link}
        target="_blank"
        rel="noopener noreferrer"
        aria-label={`Отвори ${offer.name} во ${market.name}`}
        className="flex min-h-[44px] min-w-0 flex-1 items-center gap-3 rounded-md px-1 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <span
          aria-hidden="true"
          className="h-2.5 w-2.5 flex-shrink-0 rounded-full"
          style={{ backgroundColor: market.color }}
        />
        <span className="min-w-0">
          <span className="flex items-center gap-1.5 text-sm font-medium text-card-foreground">
            {market.name}
            <ExternalLink className="h-3 w-3 text-muted-foreground" aria-hidden="true" />
          </span>
          <span className="block truncate text-xs text-muted-foreground" title={offer.name}>
            {offer.name}
          </span>
        </span>
        {offer.kratok_rok && (
          <span className="ml-1 inline-flex items-center gap-1 rounded-full border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 text-[11px] font-medium text-amber-700 dark:text-amber-400">
            <AlertTriangle className="h-3 w-3" aria-hidden="true" />
            краток рок
          </span>
        )}
      </a>

      <div className="flex flex-shrink-0 flex-col items-end text-right">
        <span className="text-sm font-semibold tabular-nums text-card-foreground">
          {offer.price} ден.
        </span>
        {offer.singular_price && (
          <span className="text-xs tabular-nums text-muted-foreground">{offer.singular_price}</span>
        )}
        <span className="mt-0.5 inline-flex items-center gap-1 text-[11px] text-muted-foreground">
          {offer.in_stock ? (
            <>
              <CheckCircle2 className="h-3 w-3 text-emerald-600 dark:text-emerald-400" aria-hidden="true" />
              Има залиха
            </>
          ) : (
            <>
              <AlertTriangle className="h-3 w-3 text-destructive" aria-hidden="true" />
              Нема залиха
            </>
          )}
          {updated && <span className="text-muted-foreground/70">· {updated}</span>}
        </span>
      </div>
    </li>
  );
};

const ProductDetail = () => {
  const [searchParams] = useSearchParams();
  const groupId = searchParams.get("g_id");

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ["product", groupId],
    queryFn: () => fetchProductDetail(groupId as string),
    enabled: Boolean(groupId),
    retry: 1,
  });

  if (!groupId) {
    return (
      <main className="container mx-auto max-w-5xl px-4 py-12">
        <h1 className="font-heading text-2xl font-bold">Недостасува производ</h1>
        <p className="mt-2 text-muted-foreground">
          Страницата бара параметар <code>g_id</code>.
        </p>
        <Link to="/" className="mt-6 inline-flex items-center gap-2 text-sm font-medium text-primary hover:underline">
          <ArrowLeft className="h-4 w-4" aria-hidden="true" /> Кон почетна
        </Link>
      </main>
    );
  }

  if (isLoading) {
    return (
      <main className="container mx-auto max-w-5xl px-4 py-8" aria-busy="true" aria-live="polite">
        <span className="sr-only">Се вчитува производот…</span>
        <div className="h-4 w-56 animate-pulse rounded bg-muted" />
        <div className="mt-6 grid gap-6 md:grid-cols-[240px_1fr]">
          <div className="h-56 animate-pulse rounded-xl bg-muted" />
          <div className="space-y-3">
            <div className="h-8 w-3/4 animate-pulse rounded bg-muted" />
            <div className="h-4 w-1/2 animate-pulse rounded bg-muted" />
            <div className="h-24 animate-pulse rounded-xl bg-muted" />
          </div>
        </div>
        <div className="mt-8 space-y-2">
          {Array.from({ length: 5 }).map((_, index) => (
            <div key={index} className="h-14 animate-pulse rounded-lg bg-muted" />
          ))}
        </div>
      </main>
    );
  }

  if (isError || !data) {
    return (
      <main className="container mx-auto max-w-5xl px-4 py-12">
        <h1 className="font-heading text-2xl font-bold">Производот не е достапен</h1>
        <p className="mt-2 text-muted-foreground">
          Можеби групата е пресметана повторно и овој идентификатор повеќе не постои.
        </p>
        <div className="mt-6 flex flex-wrap gap-3">
          <button
            type="button"
            onClick={() => refetch()}
            className="inline-flex min-h-[44px] items-center gap-2 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <RefreshCw className="h-4 w-4" aria-hidden="true" /> Обиди се повторно
          </button>
          <Link
            to="/"
            className="inline-flex min-h-[44px] items-center gap-2 rounded-lg border border-border px-4 text-sm font-medium transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <ArrowLeft className="h-4 w-4" aria-hidden="true" /> Кон почетна
          </Link>
        </div>
      </main>
    );
  }

  const { group, offers, stats } = data;
  const available = offers.filter((offer) => offer.in_stock);
  const unavailable = offers.filter((offer) => !offer.in_stock);
  const image = offers.find((offer) => offer.image)?.image;
  const quantity = formatQuantity(group.quantity_value, group.quantity_unit);
  const categoryHref = `/?category=${encodeURIComponent(group.main_category)}&sub=${encodeURIComponent(group.sub_category)}`;

  return (
    <main className="container mx-auto max-w-5xl px-4 py-8">
      {/* Breadcrumb */}
      <nav aria-label="Патека" className="mb-5 flex flex-wrap items-center gap-1.5 text-sm text-muted-foreground">
        <Link to="/" className="hover:text-foreground hover:underline">
          Дома
        </Link>
        <span aria-hidden="true">/</span>
        <Link to={categoryHref} className="hover:text-foreground hover:underline">
          {group.main_category}
        </Link>
        <span aria-hidden="true">/</span>
        <Link to={categoryHref} className="hover:text-foreground hover:underline">
          {group.sub_category}
        </Link>
      </nav>

      <Link
        to={categoryHref}
        className="mb-5 inline-flex min-h-[44px] items-center gap-2 text-sm font-medium text-muted-foreground transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <ArrowLeft className="h-4 w-4" aria-hidden="true" /> Назад кон {group.sub_category}
      </Link>

      {/* Hero */}
      <div className="grid gap-6 md:grid-cols-[240px_1fr]">
        <div className="flex h-56 items-center justify-center overflow-hidden rounded-xl border border-border bg-muted/30">
          {image ? (
            <img
              src={image}
              alt={group.group_name}
              loading="lazy"
              className="max-h-full max-w-full object-contain"
            />
          ) : (
            <ImageOff className="h-10 w-10 text-muted-foreground/40" aria-hidden="true" />
          )}
        </div>

        <div>
          <h1 className="font-heading text-2xl font-extrabold leading-tight text-foreground sm:text-3xl">
            {group.group_name}
          </h1>

          <div className="mt-2 flex flex-wrap items-center gap-2">
            {group.manufacturer && (
              <Link
                to={`/manufacturer?manufacturer_name=${encodeURIComponent(group.manufacturer)}`}
                aria-label={`Сите производи од ${group.manufacturer_display_name ?? group.manufacturer}`}
                className="inline-flex items-center gap-1 rounded-full border border-border bg-muted/50 px-2.5 py-0.5 text-xs font-medium text-foreground transition-colors hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <Store className="h-3 w-3" aria-hidden="true" />
                {group.manufacturer_display_name ?? group.manufacturer}
                <ChevronRight className="h-3 w-3" aria-hidden="true" />
              </Link>
            )}
            {quantity && (
              <span className="rounded-full border border-border bg-muted/50 px-2.5 py-0.5 text-xs font-medium tabular-nums text-foreground">
                {quantity}
              </span>
            )}
            {group.kratok_rok && (
              <span className="inline-flex items-center gap-1 rounded-full border border-amber-500/40 bg-amber-500/10 px-2.5 py-0.5 text-xs font-medium text-amber-700 dark:text-amber-400">
                <AlertTriangle className="h-3 w-3" aria-hidden="true" />
                краток рок
              </span>
            )}
          </div>

          <dl className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-3">
            <div className="rounded-lg border border-border bg-card p-3">
              <dt className="text-xs text-muted-foreground">Најевтина</dt>
              <dd className="mt-0.5 text-lg font-bold tabular-nums text-card-foreground">
                {stats.cheapest_price !== null ? `${stats.cheapest_price} ден.` : "—"}
              </dd>
            </div>
            <div className="rounded-lg border border-border bg-card p-3">
              <dt className="flex items-center gap-1 text-xs text-muted-foreground">
                <Store className="h-3 w-3" aria-hidden="true" /> Продавници
              </dt>
              <dd className="mt-0.5 text-lg font-bold tabular-nums text-card-foreground">
                {stats.markets}
              </dd>
            </div>
            <div className="rounded-lg border border-border bg-card p-3">
              <dt className="flex items-center gap-1 text-xs text-muted-foreground">
                <TrendingDown className="h-3 w-3" aria-hidden="true" /> Заштеда
              </dt>
              <dd className="mt-0.5 text-lg font-bold tabular-nums text-card-foreground">
                {stats.savings ? `${stats.savings} ден.` : "—"}
              </dd>
            </div>
          </dl>
        </div>
      </div>

      {/* Offers */}
      <section aria-labelledby="available-heading" className="mt-8">
        <h2 id="available-heading" className="font-heading text-lg font-semibold">
          Достапно во {stats.in_stock} {stats.in_stock === 1 ? "продавница" : "продавници"}
        </h2>
        {available.length > 0 ? (
          <ul className="product-card mt-3 divide-y divide-border overflow-hidden">
            {available.map((offer) => (
              <OfferRow key={offer.product_id} offer={offer} />
            ))}
          </ul>
        ) : (
          <p className="mt-3 rounded-lg border border-border bg-muted/30 p-4 text-sm text-muted-foreground">
            Моментално нема залиха во ниту една продавница.
          </p>
        )}
      </section>

      {unavailable.length > 0 && (
        <section aria-labelledby="unavailable-heading" className="mt-8">
          <h2 id="unavailable-heading" className="font-heading text-lg font-semibold text-muted-foreground">
            Нема залиха во {unavailable.length} {unavailable.length === 1 ? "продавница" : "продавници"}
          </h2>
          <ul className="mt-3 divide-y divide-border overflow-hidden rounded-xl border border-border opacity-80">
            {unavailable.map((offer) => (
              <OfferRow key={offer.product_id} offer={offer} />
            ))}
          </ul>
        </section>
      )}

      <p className="mt-8 text-xs text-muted-foreground">
        Цените се преземаат од продавниците и може да се променат. Последно ажурирање е прикажано
        кај секоја продавница.
      </p>
    </main>
  );
};

export default ProductDetail;
