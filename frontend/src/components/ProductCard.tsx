import { Link } from "react-router-dom";
import { MARKET_INFO } from "@/lib/categories";
import type { GroupedProduct } from "@/lib/api";
import { ArrowRight, ExternalLink, ImageOff, Minus, Plus, ShoppingCart } from "lucide-react";
import { useCart } from "@/context/CartContext";

interface ProductCardProps {
  group: GroupedProduct;
  /** Show the main category (used by the manufacturer listing, which spans categories). */
  showCategory?: boolean;
}

const ProductCard = ({ group, showCategory = false }: ProductCardProps) => {
  const { items, addItem, updateQuantity } = useCart();
  const sorted = [...group.products].sort((a, b) => a.price - b.price);
  const bestPrice = sorted[0]?.price;
  const firstImage = sorted.find((p) => p.image)?.image;
  const detailHref = `/product?g_id=${group.group_id}`;

  return (
    <div className="product-card animate-fade-in flex flex-col">
      {/* Image -> product page */}
      <Link
        to={detailHref}
        aria-label={`Детали за ${group.group_name}`}
        className="block rounded-t-xl focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <div className="flex h-48 items-center justify-center overflow-hidden bg-muted/30 p-4">
          {firstImage ? (
            <img
              src={firstImage}
              alt={group.group_name}
              className="max-h-full max-w-full object-contain transition-transform duration-200 hover:scale-[1.03]"
              loading="lazy"
            />
          ) : (
            <ImageOff className="h-10 w-10 text-muted-foreground/30" aria-hidden="true" />
          )}
        </div>
      </Link>

      {/* Name -> product page */}
      <div className="px-4 pb-2 pt-3">
        {showCategory && group.main_category && (
          <span className="mb-1 inline-block rounded-full bg-muted px-2 py-0.5 text-[11px] font-medium text-muted-foreground">
            {group.main_category}
          </span>
        )}
        <Link
          to={detailHref}
          className="group/link inline-flex items-start gap-1 rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <h3 className="font-heading text-sm font-semibold leading-tight text-card-foreground line-clamp-2 group-hover/link:underline">
            {group.group_name}
          </h3>
        </Link>
        {sorted.length > 1 && (
          <p className="mt-1 text-xs text-muted-foreground">{sorted.length} продавници</p>
        )}
      </div>

      {/* Prices */}
      <div className="mt-auto">
        {sorted.map((product) => {
          const market = MARKET_INFO[product.market] || {
            name: product.market,
            color: "hsl(var(--muted-foreground))",
          };
          const isBest = product.price === bestPrice && sorted.length > 1;
          const cartItem = items.find((item) => item.product.product_id === product.product_id);
          const quantity = cartItem?.quantity || 0;

          return (
            <a
              key={product.product_id}
              href={product.link}
              target="_blank"
              rel="noopener noreferrer"
              className="price-row group/row relative transition-colors hover:bg-muted/50"
            >
              <div className="flex min-w-0 items-center gap-2">
                <span
                  aria-hidden="true"
                  className="h-2.5 w-2.5 flex-shrink-0 rounded-full"
                  style={{ backgroundColor: market.color }}
                />
                <span className="truncate text-sm text-card-foreground">{market.name}</span>
                <ExternalLink
                  className="h-3 w-3 flex-shrink-0 text-muted-foreground opacity-0 transition-opacity group-hover/row:opacity-100"
                  aria-hidden="true"
                />
              </div>
              <div className="flex flex-shrink-0 items-center gap-3">
                <span
                  className={`text-sm font-semibold tabular-nums ${isBest ? "price-best" : "text-card-foreground"}`}
                >
                  {product.price} ден.
                </span>
                <div
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                  }}
                  className="flex h-8 w-[84px] items-center justify-end"
                >
                  {quantity > 0 ? (
                    <div className="flex h-full w-full items-center overflow-hidden rounded-full border bg-background shadow-sm transition-all">
                      <button
                        onClick={() => updateQuantity(product.product_id, quantity - 1)}
                        className="flex h-full flex-1 items-center justify-center text-muted-foreground transition-colors hover:bg-muted/80 hover:text-foreground active:bg-muted"
                        aria-label="Намали количина"
                      >
                        <Minus className="h-3 w-3" aria-hidden="true" />
                      </button>
                      <span className="min-w-[1.5rem] text-center text-xs font-medium tabular-nums">
                        {quantity}
                      </span>
                      <button
                        onClick={() => updateQuantity(product.product_id, quantity + 1)}
                        className="flex h-full flex-1 items-center justify-center text-muted-foreground transition-colors hover:bg-muted/80 hover:text-foreground active:bg-muted"
                        aria-label="Зголеми количина"
                      >
                        <Plus className="h-3 w-3" aria-hidden="true" />
                      </button>
                    </div>
                  ) : (
                    <button
                      onClick={() => addItem(product)}
                      className="flex h-full w-full translate-x-2 items-center justify-center gap-1.5 rounded-full bg-primary px-3 text-xs font-medium text-primary-foreground opacity-0 shadow-sm transition-all hover:bg-primary/90 focus-visible:opacity-100 group-hover/row:translate-x-0 group-hover/row:opacity-100"
                      aria-label={`Додај ${market.name} во кошничка`}
                    >
                      <ShoppingCart className="h-3.5 w-3.5" aria-hidden="true" />
                      <span>Додај</span>
                    </button>
                  )}
                </div>
              </div>

              {/* Tooltip */}
              <div className="pointer-events-none absolute bottom-full left-0 right-0 z-50 mb-1 opacity-0 transition-opacity duration-100 group-hover/row:opacity-100">
                <div className="w-max max-w-[280px] rounded-md border bg-popover px-3 py-2 text-xs text-popover-foreground shadow-md">
                  <p className="font-medium leading-snug">{product.name}</p>
                  {product.singular_price && (
                    <p className="mt-1 text-muted-foreground">Ед. цена: {product.singular_price}</p>
                  )}
                  <p className={`mt-1 ${product.in_stock ? "text-emerald-600" : "text-destructive"}`}>
                    {product.in_stock ? "Има залиха" : "Нема залиха"}
                  </p>
                </div>
              </div>
            </a>
          );
        })}

        {/* Detail affordance */}
        <Link
          to={detailHref}
          className="flex items-center justify-center gap-1.5 rounded-b-xl border-t border-border px-3 py-2.5 text-xs font-medium text-primary transition-colors hover:bg-muted/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          Види детали
          <ArrowRight className="h-3.5 w-3.5" aria-hidden="true" />
        </Link>
      </div>
    </div>
  );
};

export default ProductCard;
