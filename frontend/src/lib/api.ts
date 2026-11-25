const API_BASE = import.meta.env.VITE_API_BASE_URL || "/api";

export interface Product {
  product_id: string;
  name: string;
  price: number;
  singular_price: string | null;
  market: string;
  in_stock: boolean;
  image: string;
  link: string;
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

export interface SearchResponse {
  data: GroupedProduct[];
}

export interface ProductOffer {
  product_id: string;
  market: string;
  name: string;
  price: number;
  singular_price: string | null;
  in_stock: boolean;
  image: string | null;
  link: string;
  last_updated: string | null;
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

export async function fetchProducts(
  mainCategory: string,
  subCategory: string,
  page = 1,
  perPage: 12 | 24 | 36 = 12,
  markets: string[] = []
): Promise<ProductsResponse> {
  let url = `${API_BASE}/${encodeURIComponent(mainCategory)}/${encodeURIComponent(subCategory)}?page=${page}&per_page=${perPage}`;
  for (const m of markets) {
    url += `&market=${encodeURIComponent(m)}`;
  }
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch products: ${res.status}`);
  return res.json();
}

export async function searchProducts(query: string): Promise<SearchResponse> {
  const url = `${API_BASE}/search?q=${encodeURIComponent(query)}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return res.json();
}

export interface ManufacturerListing {
  manufacturer: {
    name: string;
    display_name: string;
    groups: number;
    categories: number;
    markets: number;
  };
  total: number;
  page: number;
  per_page: number;
  data: GroupedProduct[];
}

export async function fetchManufacturer(
  manufacturerName: string,
  page = 1,
  perPage: 12 | 24 | 36 = 12
): Promise<ManufacturerListing> {
  const url = `${API_BASE}/manufacturer?manufacturer_name=${encodeURIComponent(manufacturerName)}&page=${page}&per_page=${perPage}`;
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

