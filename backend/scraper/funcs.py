import time

header_lines = ["стока-",
"производ Продажна",
"ценаЕдинична",
"ценаОпис на",
"стокаДостапност",
"во продажен",
"објектРедовна",
"ценаЦена со",
"попуст",
"Попуст(%)Вид на",
"продажно",
"поттикнувањеВреметраење",
"на промоција",
"или попустДатум и време на последно ажурирање на цените:"]

import re
from typing import List, Dict

NAME_KEYWORDS = ("назив", "стока", "производ")
PRICE_KEYWORDS = ("продажна", "продажна цена", "цена")
DESC_KEYWORDS = ("опис", "опис на стока")
SINGULAR_KEYWORDS = ("единечна", "единечна цена", "единична", "единична цена")


def _col_index_from_headers(headers: List[str], keywords: tuple) -> int | None:
    headers_norm = [str(h).strip().lower() for h in headers]
    for i, h in enumerate(headers_norm):
        for kw in keywords:
            if kw in h:
                return i
    # fallback: try partial matches
    for i, h in enumerate(headers_norm):
        for kw in keywords:
            if any(tok in h for tok in kw.split()):
                return i
    return None

def _clean_price(s: str) -> int | None:
    if not s:
        return None
    m = re.search(r'(\d{1,6})(?:\s*ден\.?)?', str(s))
    if m:
        return int(m.group(1))
    # try float then int
    m = re.search(r'(\d+[.,]\d+)', str(s))
    if m:
        return int(float(m.group(1).replace(',', '.')))
    return None

def extract_name_price_with_camelot(pdf_path: str) -> List[Dict]:
    try:
        import camelot
    except Exception:
        return []
    results = []
    for flavor in ("lattice", "stream"):
        try:
            tables = camelot.read_pdf(pdf_path, pages="all", flavor=flavor)
        except Exception:
            tables = []
        for t in tables:
            df = t.df.copy()
            if df.empty:
                continue
            headers = df.iloc[0].tolist()
            name_col = _col_index_from_headers(headers, NAME_KEYWORDS)
            price_col = _col_index_from_headers(headers, PRICE_KEYWORDS)
            desc_col = _col_index_from_headers(headers, DESC_KEYWORDS)
            sing_col = _col_index_from_headers(headers, SINGULAR_KEYWORDS)

            data_rows = df.iloc[1:] if isinstance(name_col, int) and isinstance(price_col, int) else df
            for _, row in data_rows.iterrows():
                try:
                    name = str(row[name_col]).replace('\n', ' ').strip() if name_col is not None else None
                    price = _clean_price(row[price_col]) if price_col is not None else None
                    description = str(row[desc_col]).replace('\n', ' ').strip() if desc_col is not None else ""
                    singular_price = None
                    if sing_col is not None:
                        raw = row[sing_col]
                        singular_price = str(raw).replace('\n', ' ').strip() if raw not in (None, "") else None
                except Exception:
                    continue

                if name and price is not None:
                    full_name = f"{name} - {description}" if description else name
                    results.append({"name": full_name, "price": price, "singular_price": singular_price})
        if results:
            return results
    return results

def extract_name_price_with_pdfplumber(pdf_path: str) -> List[Dict]:
    try:
        import pdfplumber
    except Exception:
        return []
    results = []
    with pdfplumber.open(pdf_path) as pdf:
        for index, page in enumerate(pdf.pages):
            try:
                tables = page.extract_tables()
            except Exception:
                tables = []
            for table in tables:
                if not table or not any(table):
                    continue
                headers = [str(x or "").strip() for x in table[0]]
                name_col = _col_index_from_headers(headers, NAME_KEYWORDS)
                price_col = _col_index_from_headers(headers, PRICE_KEYWORDS)
                desc_col = _col_index_from_headers(headers, DESC_KEYWORDS)
                sing_col = _col_index_from_headers(headers, SINGULAR_KEYWORDS)
                start = 1 if (name_col is not None and price_col is not None) else 0
                for row in table[start:]:
                    try:
                        name = str(row[name_col]).strip() if name_col is not None and name_col < len(row) else None
                        price = _clean_price(row[price_col]) if price_col is not None and price_col < len(row) else None
                        description = str(row[desc_col]).strip() if desc_col is not None and desc_col < len(row) else ""
                        singular_price = None
                        if sing_col is not None and sing_col < len(row):
                            raw = row[sing_col]
                            singular_price = str(raw).replace('\n', ' ').strip() if raw not in (None, "") else None
                    except Exception:
                        continue

                    if name and price is not None:
                        name = name.replace('\n', ' ').strip()
                        description = description.replace('\n', ' ').strip()
                        full_name = f"{name} - {description}" if description else name
                        results.append({"name": full_name, "price": price, "singular_price": singular_price})
    return results

def extract_name_price(pdf_path: str) -> Dict:
    start = time.time()
    rows = extract_name_price_with_camelot(pdf_path)
    if not rows:
        rows = extract_name_price_with_pdfplumber(pdf_path)
    result = {}
    for row in rows:
        result[row["name"]] = [row["price"], row["singular_price"]]
    print(f"Extracted {len(result)} products from {pdf_path} in {round(time.time() - start, 2)} seconds.")
    return result

# Example usage
if __name__ == "__main__":
    df = extract_name_price("6.pdf")
    print(f"Found {len(df)} rows")
    # print(df)
    # print(df[df["name"].str.contains("КОЛАЧ")])
    # optionally save:
    # df.to_csv('products_name_price.csv', index=False, encoding='utf-8-sig')