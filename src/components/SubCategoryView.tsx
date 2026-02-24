import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { fetchProducts } from "@/lib/api";
import ProductCard from "@/components/ProductCard";
import { Loader2, ChevronLeft, ChevronRight } from "lucide-react";

interface Props {
  mainCategory: string;
  subCategory: string;
}

const PER_PAGE_OPTIONS = [12, 24, 36] as const;

const SubCategoryView = ({ mainCategory, subCategory }: Props) => {
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState<12 | 24 | 36>(12);

  const { data, isLoading, error } = useQuery({
    queryKey: ["products", mainCategory, subCategory, page, perPage],
    queryFn: () => fetchProducts(mainCategory, subCategory, page, perPage),
  });

  const totalPages = data ? Math.ceil(data.total / data.per_page) : 0;

  return (
    <div className="container max-w-6xl mx-auto px-4 py-8">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 flex-wrap gap-4">
        <div>
          <h1 className="font-heading font-extrabold text-2xl text-foreground">
            {subCategory}
          </h1>
          {data && (
            <p className="text-sm text-muted-foreground mt-0.5">
              {data.total} производи
            </p>
          )}
        </div>

        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">По страна:</span>
          {PER_PAGE_OPTIONS.map((n) => (
            <button
              key={n}
              onClick={() => { setPerPage(n); setPage(1); }}
              className={`px-3 py-1 rounded-md text-sm font-medium transition-colors ${
                perPage === n
                  ? "bg-primary text-primary-foreground"
                  : "bg-muted text-muted-foreground hover:bg-accent"
              }`}
            >
              {n}
            </button>
          ))}
        </div>
      </div>

      {/* Loading */}
      {isLoading && (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="w-8 h-8 animate-spin text-primary" />
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="text-center py-20">
          <p className="text-destructive font-medium">Грешка при вчитување</p>
          <p className="text-sm text-muted-foreground mt-1">
            Проверете дали API серверот е активен
          </p>
        </div>
      )}

      {/* Products grid */}
      {data && (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {data.data.map((group) => (
              <ProductCard key={group.group_id} group={group} />
            ))}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-center gap-2 mt-8">
              <button
                onClick={() => setPage(p => Math.max(1, p - 1))}
                disabled={page === 1}
                className="p-2 rounded-lg bg-muted text-muted-foreground hover:bg-accent disabled:opacity-30 transition-colors"
              >
                <ChevronLeft className="w-5 h-5" />
              </button>
              <span className="text-sm font-medium text-foreground px-4">
                {page} / {totalPages}
              </span>
              <button
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                disabled={page === totalPages}
                className="p-2 rounded-lg bg-muted text-muted-foreground hover:bg-accent disabled:opacity-30 transition-colors"
              >
                <ChevronRight className="w-5 h-5" />
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default SubCategoryView;
