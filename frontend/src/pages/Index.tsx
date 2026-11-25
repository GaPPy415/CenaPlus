import { useSearchParams } from "react-router-dom";
import { CATEGORIES, CATEGORY_ICONS } from "@/lib/categories";
import SubCategoryView from "@/components/SubCategoryView";
import { CategoryCard } from "@/components/CategoryCard";

const Index = () => {
  const [searchParams] = useSearchParams();
  const selectedMain = searchParams.get("category");
  const selectedSub = searchParams.get("sub");

  if (selectedMain && selectedSub) {
    return <SubCategoryView mainCategory={selectedMain} subCategory={selectedSub} />;
  }

  return (
    <div className="container max-w-6xl mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="font-heading font-extrabold text-3xl text-foreground">
          Категории
        </h1>
        <p className="text-muted-foreground mt-1">
          Споредувај цени од повеќе маркети на едно место
        </p>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
        {Object.keys(CATEGORIES).map((cat) => (
          <CategoryCard key={cat} category={cat} subcategories={CATEGORIES[cat]} />
        ))}
      </div>
    </div>
  );
};

export default Index;
