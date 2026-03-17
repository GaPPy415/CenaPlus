import { describe, it, expect } from "vitest";
import { CATEGORIES } from "@/lib/categories";
import categoriesJson from "../../shared/categories.json";

describe("categories-sync", () => {
  it("TypeScript CATEGORIES matches shared JSON exactly", () => {
    expect(CATEGORIES).toEqual(categoriesJson);
  });

  it("has exactly 21 main categories", () => {
    expect(Object.keys(CATEGORIES)).toHaveLength(21);
  });

  it("every main category has a non-empty subcategory array", () => {
    for (const [key, value] of Object.entries(CATEGORIES)) {
      expect(Array.isArray(value), `${key} should be an array`).toBe(true);
      expect(value.length, `${key} should not be empty`).toBeGreaterThan(0);
    }
  });

  it("every subcategory value is a string", () => {
    for (const [key, subs] of Object.entries(CATEGORIES)) {
      for (const sub of subs) {
        expect(typeof sub, `${key} > ${sub} should be string`).toBe("string");
      }
    }
  });
});
