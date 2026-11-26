export interface PricedOffer {
  price: number;
  regular_price?: number | null;
  in_stock?: boolean;
}

export interface GroupSaleSummary {
  maxPercent: number;
  hasMultipleRates: boolean;
  /** Compact Macedonian discount label: "-25%" or "до -25%". */
  label: string;
}

export function isOnSale(
  price: number | null | undefined,
  regularPrice: number | null | undefined
): boolean {
  return (
    typeof price === "number" &&
    typeof regularPrice === "number" &&
    price > 0 &&
    regularPrice > price
  );
}

export function getDiscountPercent(
  price: number | null | undefined,
  regularPrice: number | null | undefined
): number | null {
  if (!isOnSale(price, regularPrice)) return null;
  const pct = Math.round((1 - (price as number) / (regularPrice as number)) * 100);
  return Math.max(1, Math.min(99, pct));
}

export function getGroupSaleSummary(offers: PricedOffer[]): GroupSaleSummary | null {
  const rates: number[] = [];
  for (const offer of offers) {
    if (offer.in_stock === false) continue;
    const pct = getDiscountPercent(offer.price, offer.regular_price);
    if (pct !== null) rates.push(pct);
  }
  if (rates.length === 0) return null;

  const maxPercent = Math.max(...rates);
  const distinctRates = new Set(rates);
  const hasMultipleRates = distinctRates.size > 1;
  return {
    maxPercent,
    hasMultipleRates,
    label: hasMultipleRates ? `до -${maxPercent}%` : `-${maxPercent}%`,
  };
}
