import { useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { searchProducts } from "@/lib/api";
import ProductCard from "@/components/ProductCard";
import { Loader2 } from "lucide-react";

const SearchResults = () => {
  const [searchParams] = useSearchParams();
  const query = searchParams.get("q") || "";

  const { data, isLoading, error } = useQuery({
    queryKey: ["search", query],
    queryFn: () => searchProducts(query),
    enabled: query.length > 0,
  });

  return (
    <div className="container max-w-6xl mx-auto px-4 py-8">
      <div className="mb-6">
        <h1 className="font-heading font-extrabold text-2xl text-foreground">
          Резултати за „{query}"
        </h1>
        {data && (
          <p className="text-sm text-muted-foreground mt-0.5">
            {data.data.length} резултати
          </p>
        )}
      </div>

      {isLoading && (
        <div className="flex items-center justify-center py-20">
          <Loader2 className="w-8 h-8 animate-spin text-primary" />
        </div>
      )}

      {error && (
        <div className="text-center py-20">
          <p className="text-destructive font-medium">Грешка при пребарување</p>
          <p className="text-sm text-muted-foreground mt-1">
            Проверете дали API серверот е активен
          </p>
        </div>
      )}

      {data && data.data.length === 0 && (
        <div className="text-center py-20">
          <p className="text-muted-foreground">Нема резултати за „{query}"</p>
        </div>
      )}

      {data && data.data.length > 0 && (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          {data.data.map((group) => (
            <ProductCard key={group.group_id} group={group} />
          ))}
        </div>
      )}
    </div>
  );
};

export default SearchResults;

