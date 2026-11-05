"""S2.9: manufacturer alias merging via complete-linkage clustering.

Why not pairwise `keep,drop`:
  * pairs form chains, so applying them depends on order and silently skips rows;
  * similarity is not transitive (`виталиа`~`виталија` and `виталија`~`вита` would
    put `виталиа` and `вита` in one cluster), so single-linkage over-merges.

So: propose edges by similarity, cluster them with **complete linkage** (every
cross-pair inside a cluster must clear the floor), and emit one row per cluster.

Merging keeps the absorbed rows and sets `manufacturers.merged_into`, so nothing
is deleted and an unmerge is a one-line UPDATE.

Usage:
    python -m backend.data.manufacturer_merges                       # summary
    python -m backend.data.manufacturer_merges --csv clusters.csv    # review file
    python -m backend.data.manufacturer_merges --apply clusters.csv  # merge approved
    python -m backend.data.manufacturer_merges --pair "бимилк,битолско" --apply

CSV format for --apply: `canonical,aliases` (aliases pipe-separated); extra columns ignored.
After applying, re-run `group_products.py --apply` because manufacturer_id is part of the key.
"""

from __future__ import annotations

import argparse
import csv
import difflib
import itertools
import os
import sys
from collections import defaultdict

from dotenv import find_dotenv, load_dotenv
from psycopg2.extras import RealDictCursor

from backend.data.db_utils import connect_to_db
from backend.data.extract_features import normalize_string

MANUFACTURERS_SQL = """
    SELECT m.id, m.canonical_name, count(p.id) AS products
    FROM manufacturers m
    LEFT JOIN products p ON p.manufacturer_id = m.id
    WHERE m.merged_into IS NULL
    GROUP BY m.id, m.canonical_name
    ORDER BY products DESC
"""


def propose_edges(rows: list[dict], threshold: float) -> list[tuple[str, str, float]]:
    """Similarity edges, blocked by name prefix so this stays O(n) rather than O(n²)."""
    buckets: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        name = row["canonical_name"]
        tokens = sorted(name.split())
        buckets[tokens[0][:4] if tokens else name[:4]].append(name)

    edges = []
    seen = set()
    for names in buckets.values():
        for name_a, name_b in itertools.combinations(sorted(set(names)), 2):
            if (name_a, name_b) in seen:
                continue
            seen.add((name_a, name_b))
            if abs(len(name_a) - len(name_b)) > max(len(name_a), len(name_b)) * 0.5:
                continue
            ratio = difflib.SequenceMatcher(None, name_a, name_b).ratio()
            if ratio >= threshold:
                edges.append((name_a, name_b, ratio))
    edges.sort(key=lambda edge: -edge[2])
    return edges


def complete_linkage(edges: list[tuple[str, str, float]], floor: float) -> list[set[str]]:
    """Merge edges into clusters, but only when every cross-pair clears `floor`."""
    cache: dict[tuple[str, str], float] = {}

    def ratio(a: str, b: str) -> float:
        key = (a, b) if a < b else (b, a)
        if key not in cache:
            cache[key] = difflib.SequenceMatcher(None, a, b).ratio()
        return cache[key]

    cluster_of: dict[str, set[str]] = {}
    for name_a, name_b, _ in edges:
        cluster_of.setdefault(name_a, {name_a})
        cluster_of.setdefault(name_b, {name_b})

    changed = True
    while changed:
        changed = False
        for name_a, name_b, _ in edges:
            cluster_a, cluster_b = cluster_of[name_a], cluster_of[name_b]
            if cluster_a is cluster_b:
                continue
            if min(ratio(x, y) for x in cluster_a for y in cluster_b) < floor:
                continue
            cluster_a |= cluster_b
            for member in cluster_b:
                cluster_of[member] = cluster_a
            changed = True

    unique = {id(cluster): cluster for cluster in cluster_of.values()}
    return list(unique.values())


def choose_canonical(cluster: set[str], products: dict[str, int]) -> str:
    """Most products wins; ties go to the shorter, then lexicographically first name."""
    return sorted(cluster, key=lambda name: (-products.get(name, 0), len(name), name))[0]


def build_clusters(rows: list[dict], threshold: float, floor: float) -> list[dict]:
    products = {row["canonical_name"]: row["products"] for row in rows}
    edges = propose_edges(rows, threshold)
    clusters = complete_linkage(edges, floor)

    cache: dict[tuple[str, str], float] = {}

    def ratio(a: str, b: str) -> float:
        key = (a, b) if a < b else (b, a)
        if key not in cache:
            cache[key] = difflib.SequenceMatcher(None, a, b).ratio()
        return cache[key]

    out = []
    for cluster in clusters:
        if len(cluster) < 2:
            continue
        canonical = choose_canonical(cluster, products)
        aliases = sorted(cluster - {canonical}, key=lambda name: -products.get(name, 0))
        weakest = min(ratio(x, y) for x, y in itertools.combinations(sorted(cluster), 2))
        out.append({
            "canonical": canonical,
            "aliases": aliases,
            "names": len(cluster),
            "products": sum(products.get(name, 0) for name in cluster),
            "min_similarity": round(weakest, 4),
        })
    out.sort(key=lambda item: (-item["names"], -item["products"]))
    return out


def apply_clusters(conn, clusters: list[dict]) -> tuple[int, int]:
    """Merge each cluster. Rows are kept; absorbed ones get merged_into."""
    merged_names = 0
    merged_clusters = 0
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for cluster in clusters:
            names = [cluster["canonical"], *cluster["aliases"]]
            cur.execute("SELECT id, canonical_name FROM manufacturers WHERE canonical_name = ANY(%s)",
                        (names,))
            found = {row["canonical_name"]: row["id"] for row in cur.fetchall()}
            canonical_id = found.get(cluster["canonical"])
            if canonical_id is None:
                continue
            touched = 0
            for alias in cluster["aliases"]:
                alias_id = found.get(alias)
                if alias_id is None or alias_id == canonical_id:
                    continue
                cur.execute("UPDATE products SET manufacturer_id = %s WHERE manufacturer_id = %s",
                            (canonical_id, alias_id))
                cur.execute("UPDATE groups SET manufacturer_id = %s WHERE manufacturer_id = %s",
                            (canonical_id, alias_id))
                cur.execute("UPDATE manufacturer_aliases SET manufacturer_id = %s WHERE manufacturer_id = %s",
                            (canonical_id, alias_id))
                cur.execute("""
                    INSERT INTO manufacturer_aliases (alias, manufacturer_id) VALUES (%s, %s)
                    ON CONFLICT (alias) DO UPDATE SET manufacturer_id = EXCLUDED.manufacturer_id
                """, (alias, canonical_id))
                cur.execute("UPDATE manufacturers SET merged_into = %s WHERE id = %s",
                            (canonical_id, alias_id))
                merged_names += 1
                touched += 1
            if touched:
                merged_clusters += 1
    conn.commit()
    return merged_clusters, merged_names


def read_cluster_csv(path: str) -> list[dict]:
    clusters = []
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.reader(fh):
            if len(row) < 2 or row[0].strip().lower() == "canonical":
                continue
            aliases = [a.strip() for a in row[1].split("|") if a.strip()]
            if aliases:
                clusters.append({"canonical": row[0].strip(), "aliases": aliases})
    return clusters


def main() -> int:
    parser = argparse.ArgumentParser(description="Cluster and merge manufacturer name variants.")
    parser.add_argument("--threshold", type=float, default=0.86, help="Edge similarity floor")
    parser.add_argument("--linkage-floor", type=float, default=0.86,
                        help="Every cross-pair in a cluster must reach this")
    parser.add_argument("--limit", type=int, default=15, help="How many clusters to print")
    parser.add_argument("--csv", type=str, default=None, help="Write the cluster review file")
    parser.add_argument("--apply", nargs="?", const="", default=None,
                        help="Apply a cluster CSV, or manual --pair entries when empty")
    parser.add_argument("--pair", action="append", default=[], help='Manual merge "keep,drop"')
    args = parser.parse_args()

    load_dotenv(find_dotenv())
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass

    conn = connect_to_db()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(MANUFACTURERS_SQL)
    rows = [dict(row) for row in cur.fetchall()]
    print(f"active manufacturers: {len(rows):,} | products with a manufacturer: "
          f"{sum(row['products'] for row in rows):,}")

    clusters = build_clusters(rows, args.threshold, args.linkage_floor)
    merged_names = sum(len(cluster["aliases"]) for cluster in clusters)
    print(f"clusters: {len(clusters):,} covering {merged_names:,} names "
          f"(edge floor {args.threshold}, linkage floor {args.linkage_floor})")

    print(f"\n{'names':>5} {'products':>9} {'min_sim':>8}  canonical <- aliases")
    for cluster in clusters[: args.limit]:
        aliases = "|".join(cluster["aliases"])
        print(f"{cluster['names']:>5} {cluster['products']:>9,} {cluster['min_similarity']:>8.3f}  "
              f"{cluster['canonical']} <- {aliases[:70]}")

    if args.csv:
        with open(args.csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["canonical", "aliases", "names", "products", "min_similarity"])
            for cluster in clusters:
                writer.writerow([cluster["canonical"], "|".join(cluster["aliases"]),
                                 cluster["names"], cluster["products"], cluster["min_similarity"]])
        print(f"\nwrote {len(clusters):,} clusters to {args.csv}")

    if args.apply is not None:
        to_apply: list[dict] = []
        for entry in args.pair:
            if "," not in entry:
                print(f"  ignoring malformed --pair {entry!r}")
                continue
            keep, drop = entry.split(",", 1)
            to_apply.append({"canonical": keep.strip(), "aliases": [drop.strip()]})
        if args.apply:
            to_apply.extend(read_cluster_csv(args.apply))
        if not to_apply:
            print("\nnothing to apply")
            return 0
        merged_clusters, merged = apply_clusters(conn, to_apply)
        print(f"\nmerged {merged} name(s) across {merged_clusters} cluster(s)")
        cur.execute("SELECT count(*) AS n FROM manufacturers WHERE merged_into IS NULL")
        print("active manufacturers now:", cur.fetchone()["n"])
        print("re-run: python -m backend.data.group_products --apply")

    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
