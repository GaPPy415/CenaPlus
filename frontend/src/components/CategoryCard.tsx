import { Link } from "react-router-dom";
import { CATEGORY_ICONS } from "@/lib/categories";

interface CategoryCardProps {
  category: string;
  subcategories: string[];
}

export const CategoryCard = ({ category, subcategories }: CategoryCardProps) => {
  return (
    <Link
      to={`/?category=${encodeURIComponent(category)}`}
      className="category-card block text-left"
    >
      <span className="mb-2 block text-3xl" aria-hidden="true">
        {CATEGORY_ICONS[category] || "📁"}
      </span>
      <span className="font-heading text-sm font-semibold leading-tight text-card-foreground">
        {category}
      </span>
      <span className="mt-1 block text-xs text-muted-foreground">
        {subcategories.length} подкатегории
      </span>
    </Link>
  );
};
