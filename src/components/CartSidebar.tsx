import { ShoppingCart, Plus, Minus, Trash2 } from "lucide-react";
import { useCart } from "@/context/CartContext";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";

import { MARKET_INFO } from "@/lib/categories";

const CartSidebar = () => {
  const { items, updateQuantity, removeItem, totalPrice, totalItems } = useCart();

  return (
    <Sheet>
      <SheetTrigger asChild>
        <Button variant="ghost" size="icon" className="relative">
          <ShoppingCart className="h-5 w-5" />
          {totalItems > 0 && (
            <span className="absolute -top-1 -right-1 flex h-4 w-4 items-center justify-center rounded-full bg-primary text-[10px] font-bold text-primary-foreground">
              {totalItems}
            </span>
          )}
        </Button>
      </SheetTrigger>
      <SheetContent className="flex w-full flex-col sm:max-w-md">
        <SheetHeader className="pb-4 border-b border-border">
          <SheetTitle className="flex items-center gap-2">
            <ShoppingCart className="h-5 w-5" />
            Кошничка
          </SheetTitle>
        </SheetHeader>

        <div className="flex-1 overflow-y-auto py-4">
          {items.length === 0 ? (
            <div className="flex h-full flex-col items-center justify-center space-y-2 text-muted-foreground">
              <ShoppingCart className="h-12 w-12 opacity-20" />
              <p>Вашата кошничка е празна</p>
            </div>
          ) : (
            <ul className="space-y-4">
              {items.map((item) => (
                <li key={item.product.product_id} className="flex gap-4 p-2 rounded-lg bg-card border border-border">
                  {item.product.image && (
                    <div className="h-20 w-20 flex-shrink-0 overflow-hidden rounded-md bg-muted flex items-center justify-center">
                      <img
                        src={item.product.image}
                        alt={item.product.name}
                        className="h-full w-full object-contain p-2"
                        onError={(e) => {
                          (e.target as HTMLImageElement).src = '/placeholder.svg';
                        }}
                      />
                    </div>
                  )}
                  <div className="flex flex-1 flex-col justify-between">
                    <div className="flex justify-between gap-2">
                      <div className="flex flex-col gap-1">
                        <h4 className="text-sm font-medium line-clamp-2 leading-tight">
                          {item.product.name}
                        </h4>
                        <span className="text-[10px] uppercase tracking-wider font-semibold text-muted-foreground">
                          {MARKET_INFO[item.product.market]?.name || item.product.market}
                        </span>
                      </div>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="h-6 w-6 text-muted-foreground hover:text-destructive shrink-0"
                        onClick={() => removeItem(item.product.product_id)}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                    <div className="flex items-center justify-between mt-2">
                      <div className="flex items-center space-x-2 bg-muted rounded-md p-1">
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6 rounded-sm"
                          onClick={() => updateQuantity(item.product.product_id, item.quantity - 1)}
                        >
                          <Minus className="h-3 w-3" />
                        </Button>
                        <span className="text-xs font-medium w-4 text-center">
                          {item.quantity}
                        </span>
                        <Button
                          variant="ghost"
                          size="icon"
                          className="h-6 w-6 rounded-sm"
                          onClick={() => updateQuantity(item.product.product_id, item.quantity + 1)}
                        >
                          <Plus className="h-3 w-3" />
                        </Button>
                      </div>
                      <p className="font-semibold text-sm">
                        {(item.product.price * item.quantity).toFixed(0)} ден.
                      </p>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </div>

        {items.length > 0 && (
          <div className="border-t border-border pt-4 mt-auto">
            <div className="flex items-center justify-between font-semibold text-lg">
              <span>Вкупно:</span>
              <span>{totalPrice.toFixed(0)} ден.</span>
            </div>
          </div>
        )}
      </SheetContent>
    </Sheet>
  );
};

export default CartSidebar;
