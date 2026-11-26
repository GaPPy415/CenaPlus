const API_BASE = import.meta.env.VITE_API_BASE_URL || "/api";

export interface Product {
  product_id: string;
  name: string;
  price: number;
  regular_price?: number | null;
  singular_price: string | null;
  market: string;
  in_stock: boolean;
  image: string | null;
  link: string | null;
}

export interface GroupedProduct {
  group_id: string;
  group_name: string;
  main_category: string;
  sub_category: string;
  products: Product[];
  similarity?: number;
}

export interface ProductsResponse {
  total: number;
  page: number;
  per_page: number;
  data: GroupedProduct[];
}

export interface ProductOffer {
  product_id: string;
  market: string;
  name: string;
  price: number;
  regular_price: number | null;
  singular_price: string | null;
  in_stock: boolean;
  image: string | null;
  link: string | null;
  last_updated: string | null;
  /** Newest `last_updated` across all of this market's products (store data freshness). */
  market_last_updated: string | null;
  kratok_rok: boolean;
}

export interface ProductDetail {
  group: {
    group_id: string;
    group_name: string;
    main_category: string;
    sub_category: string;
    product_name: string | null;
    quantity_value: number | null;
    quantity_unit: string | null;
    is_fluid: boolean | null;
    manufacturer: string | null;
    manufacturer_display_name: string | null;
    kratok_rok: boolean;
  };
  offers: ProductOffer[];
  stats: {
    offers: number;
    markets: number;
    in_stock: number;
    out_of_stock: number;
    cheapest_price: number | null;
    priciest_price: number | null;
    savings: number | null;
  };
}

export type SortBy = "name" | "price" | "discount" | "discount_amount" | "markets";
export type SearchSortBy = "relevance" | SortBy;
export type SortDir = "asc" | "desc";

export interface CategorySubCount {
  sub_category: string;
  count: number;
}

export interface CategoryProductsResponse extends ProductsResponse {
  category_total: number;
  subcategories: CategorySubCount[];
}

export async function fetchProducts(
  mainCategory: string,
  subCategory: string | null,
  page = 1,
  perPage: 12 | 24 | 36 = 24,
  markets: string[] = [],
  onSale = false,
  sort: SortBy = "name",
  order: SortDir = "asc"
): Promise<CategoryProductsResponse> {
  const path = subCategory
    ? `${encodeURIComponent(mainCategory)}/${encodeURIComponent(subCategory)}`
    : encodeURIComponent(mainCategory);
  let url = `${API_BASE}/${path}?page=${page}&per_page=${perPage}&sort=${encodeURIComponent(sort)}&order=${encodeURIComponent(order)}`;
  for (const m of markets) {
    url += `&market=${encodeURIComponent(m)}`;
  }
  if (onSale) {
    url += `&on_sale=true`;
  }
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch products: ${res.status}`);
  return res.json();
}

export async function searchProducts(
  query: string,
  page = 1,
  perPage: 12 | 24 | 36 = 24,
  markets: string[] = [],
  onSale = false,
  sort: SearchSortBy = "relevance",
  order: SortDir = "desc"
): Promise<ProductsResponse> {
  let url = `${API_BASE}/search?q=${encodeURIComponent(query)}&page=${page}&per_page=${perPage}&sort=${encodeURIComponent(sort)}&order=${encodeURIComponent(order)}`;
  for (const m of markets) {
    url += `&market=${encodeURIComponent(m)}`;
  }
  if (onSale) {
    url += `&on_sale=true`;
  }
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return res.json();
}

export interface ManufacturerSubCategory {
  main_category: string;
  sub_category: string;
  count: number;
}

export interface ManufacturerListing {
  manufacturer: {
    name: string;
    display_name: string;
    groups: number;
    categories: number;
    markets: number;
    last_updated: string | null;
  };
  subcategories: ManufacturerSubCategory[];
  total: number;
  page: number;
  per_page: number;
  data: GroupedProduct[];
}

export async function fetchManufacturer(
  manufacturerName: string,
  page = 1,
  perPage: 12 | 24 | 36 = 24,
  subCategories: string[] = [],
  markets: string[] = [],
  onSale = false,
  sort: SortBy = "name",
  order: SortDir = "asc"
): Promise<ManufacturerListing> {
  let url = `${API_BASE}/manufacturer?manufacturer_name=${encodeURIComponent(manufacturerName)}&page=${page}&per_page=${perPage}&sort=${encodeURIComponent(sort)}&order=${encodeURIComponent(order)}`;
  for (const sc of subCategories) {
    url += `&sub_category=${encodeURIComponent(sc)}`;
  }
  for (const m of markets) {
    url += `&market=${encodeURIComponent(m)}`;
  }
  if (onSale) {
    url += `&on_sale=true`;
  }
  const res = await fetch(url);
  if (res.status === 404) throw new Error("Manufacturer not found");
  if (!res.ok) throw new Error(`Failed to fetch manufacturer: ${res.status}`);
  return res.json();
}

export async function fetchProductDetail(groupId: string): Promise<ProductDetail> {
  const url = `${API_BASE}/product?g_id=${encodeURIComponent(groupId)}`;
  const res = await fetch(url);
  if (res.status === 404) throw new Error("Product not found");
  if (!res.ok) throw new Error(`Failed to fetch product: ${res.status}`);
  return res.json();
}

