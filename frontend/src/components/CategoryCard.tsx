import * as React from "react";
import { useNavigate } from "react-router-dom";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogTrigger,
} from "@/components/ui/dialog";
import { CATEGORY_ICONS } from "@/lib/categories";

interface CategoryCardProps {
  category: string;
  subcategories: string[];
}

export const CategoryCard = ({ category, subcategories }: CategoryCardProps) => {
  const navigate = useNavigate();
  const [isOpen, setIsOpen] = React.useState(false);

  const handleSubcategoryClick = (subcategory: string) => {
    navigate(`/?category=${encodeURIComponent(category)}&sub=${encodeURIComponent(subcategory)}`);
    setIsOpen(false);
  };

  return (
    <Dialog open={isOpen} onOpenChange={setIsOpen}>
      <DialogTrigger asChild>
        <button className="category-card text-left" onClick={() => setIsOpen(true)}>
          <span className="text-3xl block mb-2">{CATEGORY_ICONS[category] || "📁"}</span>
          <span className="font-heading font-semibold text-sm text-card-foreground leading-tight">
            {category}
          </span>
          <span className="text-xs text-muted-foreground mt-1 block">
            {subcategories.length} подкатегории
          </span>
        </button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <span className="text-2xl">{CATEGORY_ICONS[category] || "📁"}</span>
            {category}
          </DialogTitle>
          <DialogDescription>
            Избери подкатегорија за да видиш продукти
          </DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mt-4 max-h-[60vh] overflow-y-auto">
          {subcategories.map((sub) => (
            <button
              key={sub}
              onClick={() => handleSubcategoryClick(sub)}
              className="w-full text-left px-4 py-3 text-sm rounded-lg hover:bg-accent transition-colors border border-border hover:border-primary/50"
            >
              {sub}
            </button>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
};
