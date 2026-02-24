import { useSearchParams } from "react-router-dom";
import { CATEGORIES, CATEGORY_ICONS } from "@/lib/categories";
import SubCategoryView from "@/components/SubCategoryView";

const Index = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedMain = searchParams.get("category");
  const selectedSub = searchParams.get("sub");

  const handleMainClick = (cat: string) => {
    if (selectedMain === cat) {
      setSearchParams({});
    } else {
      setSearchParams({ category: cat });
    }
  };

  const handleSubClick = (sub: string) => {
    if (selectedMain) {
      setSearchParams({ category: selectedMain, sub });
    }
  };

  if (selectedMain && selectedSub) {
    return <SubCategoryView mainCategory={selectedMain} subCategory={selectedSub} />;
  }

  return (
    <div className="container max-w-6xl mx-auto px-4 py-8">
      <div className="mb-8">
        <h1 className="font-heading font-extrabold text-3xl text-foreground">
          {selectedMain ? selectedMain : "Категории"}
        </h1>
        <p className="text-muted-foreground mt-1">
          {selectedMain
            ? "Избери подкатегорија"
            : "Споредувај цени од повеќе маркети на едно место"}
        </p>
      </div>

      {!selectedMain ? (
        /* Main categories grid */
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 gap-3">
          {Object.keys(CATEGORIES).map((cat) => (
            <button
              key={cat}
              onClick={() => handleMainClick(cat)}
              className="category-card text-left"
            >
              <span className="text-3xl block mb-2">{CATEGORY_ICONS[cat] || "📁"}</span>
              <span className="font-heading font-semibold text-sm text-card-foreground leading-tight">
                {cat}
              </span>
              <span className="text-xs text-muted-foreground mt-1 block">
                {CATEGORIES[cat].length} подкатегории
              </span>
            </button>
          ))}
        </div>
      ) : (
        /* Subcategories list */
        <div className="flex flex-wrap gap-2">
          {CATEGORIES[selectedMain]?.map((sub) => (
            <button
              key={sub}
              onClick={() => handleSubClick(sub)}
              className="subcategory-chip"
            >
              {sub}
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default Index;
