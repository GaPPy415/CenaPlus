import { MARKET_INFO } from "@/lib/categories";
import type { GroupedProduct } from "@/lib/api";
import { ExternalLink, ShoppingCart, Plus, Minus } from "lucide-react";
import { useCart } from "@/context/CartContext";

interface ProductCardProps {
  group: GroupedProduct;
}

const ProductCard = ({ group }: ProductCardProps) => {
  const { items, addItem, updateQuantity } = useCart();
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
          const cartItem = items.find((item) => item.product.product_id === product.product_id);
          const quantity = cartItem?.quantity || 0;

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
              <div className="flex items-center gap-3 flex-shrink-0">
                <span className={`text-sm font-semibold tabular-nums ${isBest ? 'price-best' : 'text-card-foreground'}`}>
                  {product.price} ден.
                </span>
                <div 
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                  }}
                  className="flex items-center justify-end w-[84px] h-8"
                >
                  {quantity > 0 ? (
                    <div className="flex items-center h-full w-full bg-background border rounded-full shadow-sm overflow-hidden transition-all">
                      <button
                        onClick={() => updateQuantity(product.product_id, quantity - 1)}
                        className="flex-1 h-full flex items-center justify-center hover:bg-muted/80 active:bg-muted text-muted-foreground hover:text-foreground transition-colors"
                        aria-label="Decrease quantity"
                      >
                        <Minus className="w-3 h-3" />
                      </button>
                      <span className="text-xs font-medium tabular-nums text-center min-w-[1.5rem]">
                        {quantity}
                      </span>
                      <button
                        onClick={() => updateQuantity(product.product_id, quantity + 1)}
                        className="flex-1 h-full flex items-center justify-center hover:bg-muted/80 active:bg-muted text-muted-foreground hover:text-foreground transition-colors"
                        aria-label="Increase quantity"
                      >
                        <Plus className="w-3 h-3" />
                      </button>
                    </div>
                  ) : (
                    <button
                      onClick={() => addItem(product)}
                      className="h-full w-full px-3 text-xs font-medium bg-primary text-primary-foreground hover:bg-primary/90 shadow-sm rounded-full transition-all flex items-center justify-center gap-1.5 opacity-0 group-hover/row:opacity-100 focus-visible:opacity-100 translate-x-2 group-hover/row:translate-x-0"
                    >
                      <ShoppingCart className="w-3.5 h-3.5" />
                      <span>Add</span>
                    </button>
                  )}
                </div>
              </div>

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
