const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

export interface Product {
  product_id: string;
  name: string;
  price: number;
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
}

export interface ProductsResponse {
  total: number;
  page: number;
  per_page: number;
  data: GroupedProduct[];
}

export async function fetchProducts(
  mainCategory: string,
  subCategory: string,
  page = 1,
  perPage: 12 | 24 | 36 = 12
): Promise<ProductsResponse> {
  const url = `${API_BASE}/${encodeURIComponent(mainCategory)}/${encodeURIComponent(subCategory)}?page=${page}&per_page=${perPage}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch products: ${res.status}`);
  return res.json();
}
