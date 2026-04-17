import { Link, useLocation, useNavigate } from "react-router-dom";
import { ChevronRight, Search } from "lucide-react";
import ThemeToggle from "@/components/ThemeToggle";
import CartSidebar from "@/components/CartSidebar";
import { useState } from "react";

const Header = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const params = new URLSearchParams(location.search);
  const mainCat = params.get("category");
  const subCat = params.get("sub");
  const [searchValue, setSearchValue] = useState("");

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    const q = searchValue.trim();
    if (q) navigate(`/search?q=${encodeURIComponent(q)}`);
  };

  return (
    <header className="sticky top-0 z-50 bg-card/80 backdrop-blur-md border-b border-border">
      <div className="container max-w-6xl mx-auto px-4 py-4 flex items-center gap-3">
        <Link to="/" className="flex items-center gap-2 flex-shrink-0">
          <span className="text-2xl">🛒</span>
          <span className="font-heading font-extrabold text-xl text-foreground tracking-tight">
            CenaPlus
          </span>
        </Link>

        {mainCat && (
          <>
            <ChevronRight className="w-4 h-4 text-muted-foreground flex-shrink-0" />
            <Link
              to={`/?category=${encodeURIComponent(mainCat)}`}
              className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors truncate"
            >
              {mainCat}
            </Link>
          </>
        )}

        {subCat && (
          <>
            <ChevronRight className="w-4 h-4 text-muted-foreground flex-shrink-0" />
            <span className="text-sm font-medium text-foreground truncate">
              {subCat}
            </span>
          </>
        )}

        <div className="ml-auto flex items-center gap-2">
          <form onSubmit={handleSearch}>
            <div className="relative">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" />
              <input
                type="text"
                value={searchValue}
                onChange={(e) => setSearchValue(e.target.value)}
                placeholder="Пребарај производи..."
                className="pl-8 pr-3 py-1.5 text-sm rounded-md bg-muted border border-border text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary w-48 sm:w-64 transition-all"
              />
            </div>
          </form>
          <ThemeToggle />
          <CartSidebar />
        </div>
      </div>
    </header>
  );
};

export default Header;
