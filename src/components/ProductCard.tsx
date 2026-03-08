import { MARKET_INFO } from "@/lib/categories";
import type { GroupedProduct } from "@/lib/api";
import { ExternalLink } from "lucide-react";

interface ProductCardProps {
  group: GroupedProduct;
}

const ProductCard = ({ group }: ProductCardProps) => {
  const sorted = [...group.products].sort((a, b) => a.price - b.price);
  const bestPrice = sorted[0]?.price;
  const firstImage = sorted.find(p => p.image)?.image;

  return (
    <div className="product-card animate-fade-in flex flex-col">
      {/* Image */}
      <div className="flex items-center justify-center p-4 bg-muted/30 h-48 overflow-hidden rounded-t-xl">
        {firstImage ? (
          <img
            src={firstImage}
            alt={group.group_name}
            className="max-h-full max-w-full object-contain"
            loading="lazy"
          />
        ) : (
          <div className="text-4xl text-muted-foreground/30">📦</div>
        )}
      </div>

      {/* Name */}
      <div className="px-4 pt-3 pb-2">
        <h3 className="font-heading font-semibold text-sm leading-tight text-card-foreground line-clamp-2">
          {group.group_name}
        </h3>
        {sorted.length > 1 && (
          <p className="text-xs text-muted-foreground mt-1">
            {sorted.length} продавници
          </p>
        )}
      </div>

      {/* Prices */}
      <div className="mt-auto">
        {sorted.map((product) => {
          const market = MARKET_INFO[product.market] || { name: product.market, color: "hsl(var(--muted-foreground))" };
          const isBest = product.price === bestPrice && sorted.length > 1;

          return (
            <a
              key={product.product_id}
              href={product.link}
              target="_blank"
              rel="noopener noreferrer"
              className="price-row group/row hover:bg-muted/50 transition-colors relative"
            >
              <div className="flex items-center gap-2 min-w-0">
                <span
                  className="w-2.5 h-2.5 rounded-full flex-shrink-0"
                  style={{ backgroundColor: market.color }}
                />
                <span className="text-sm truncate text-card-foreground">
                  {market.name}
                </span>
                <ExternalLink className="w-3 h-3 text-muted-foreground opacity-0 group-hover/row:opacity-100 transition-opacity flex-shrink-0" />
              </div>
              <span className={`text-sm font-semibold tabular-nums flex-shrink-0 ${isBest ? 'price-best' : 'text-card-foreground'}`}>
                {product.price} ден.
              </span>

              {/* Tooltip */}
              <div className="absolute left-0 right-0 bottom-full mb-1 z-50 pointer-events-none opacity-0 group-hover/row:opacity-100 transition-opacity duration-100">
                <div className="bg-popover text-popover-foreground text-xs rounded-md border shadow-md px-3 py-2 w-max max-w-[280px]">
                  <p className="font-medium leading-snug">{product.name}</p>
                  {product.singular_price && (
                    <p className="text-muted-foreground mt-1">Ед. цена: {product.singular_price}</p>
                  )}
                  <p className={`mt-1 ${product.in_stock ? 'text-green-600' : 'text-destructive'}`}>
                    {product.in_stock ? 'Има залиха' : 'Нема залиха'}
                  </p>
                </div>
              </div>
            </a>
          );
        })}
      </div>
    </div>
  );
};

export default ProductCard;
