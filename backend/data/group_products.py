from backend.data.db_utils import *
from backend.data.constants import CATEGORIES

GROUPING_THRESHOLDS = {
    'Основни намирници': {
        'Брашно и квасец': 0.97,
        'Сол': 0.96
    },
    'Млечни производи': {
        'Млеко': 0.99,
        'Јогурт и кисело млеко': 0.985,
        'Сирење и кашкавал': 0.964,
    },
    'Безалкохолни пијалоци': {
        'Вода (газирана и негазирана)': 0.98,
        'Газирани сокови': 0.97,
        'Негазирани сокови': 0.965,
    }
}


if __name__ == "__main__":
    conn = connect_to_db()
    for main_category, sub_categories in GROUPING_THRESHOLDS.items():
        for sub_category in sub_categories:
            threshold = GROUPING_THRESHOLDS[main_category][sub_category]
            print(f"Grouping products for main category '{main_category}' and sub-category '{sub_category}' with threshold {threshold}...")
            group_products_by_category(conn, main_category, sub_category, threshold)
    print("Finished grouping products with specific thresholds. Now processing remaining categories with default threshold 0.95...")
    for main_category in CATEGORIES.keys():
        if main_category == 'Разно':
            print(f"Skipping grouping for main category '{main_category}'")
            continue
        for sub_category in CATEGORIES[main_category]:
            if sub_category == 'Останато':
                print(f"Skipping grouping for sub-category '{sub_category}' in main category '{main_category}'")
                continue
            if main_category not in GROUPING_THRESHOLDS or sub_category not in GROUPING_THRESHOLDS[main_category]:
                print(f"No specific threshold for '{main_category}' -> '{sub_category}', using default 0.95")
                group_products_by_category(conn, main_category, sub_category, 0.95)
    conn.close()
