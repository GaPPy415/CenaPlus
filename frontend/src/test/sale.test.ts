import { describe, expect, it } from "vitest";
import { getDiscountPercent, getGroupSaleSummary, isOnSale } from "@/lib/sale";

describe("sale helpers", () => {
  it("detects valid discounts only when regular_price > price", () => {
    expect(isOnSale(89, 119)).toBe(true);
    expect(isOnSale(119, 119)).toBe(false);
    expect(isOnSale(119, 89)).toBe(false);
    expect(isOnSale(89, null)).toBe(false);
    expect(isOnSale(89, undefined)).toBe(false);
  });

  it("computes rounded discount percent and avoids -0%", () => {
    expect(getDiscountPercent(89, 119)).toBe(25);
    expect(getDiscountPercent(50, 100)).toBe(50);
    expect(getDiscountPercent(999, 1000)).toBe(1);
    expect(getDiscountPercent(100, 100)).toBeNull();
    expect(getDiscountPercent(100, null)).toBeNull();
  });

  it("summarizes group sales cleanly and ignores out-of-stock offers", () => {
    expect(
      getGroupSaleSummary([
        { price: 89, regular_price: 119, in_stock: true },
        { price: 95, regular_price: null, in_stock: true },
      ])
    ).toEqual({ maxPercent: 25, hasMultipleRates: false, label: "-25%" });

    expect(
      getGroupSaleSummary([
        { price: 80, regular_price: 100, in_stock: true },
        { price: 70, regular_price: 100, in_stock: true },
      ])
    ).toEqual({ maxPercent: 30, hasMultipleRates: true, label: "до -30%" });

    expect(
      getGroupSaleSummary([
        { price: 70, regular_price: 100, in_stock: false },
        { price: 95, regular_price: null, in_stock: true },
      ])
    ).toBeNull();
  });
});
