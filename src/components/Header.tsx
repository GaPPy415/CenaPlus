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
    <header className="sticky top-0 z-50 border-b border-border bg-card/80 backdrop-blur-md">
      <div className="container mx-auto flex max-w-6xl items-center gap-3 px-4 py-4">
        <Link
          to="/"
          aria-label="CenaPlus — почетна"
          className="flex flex-shrink-0 items-center gap-2 rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <span className="text-2xl" aria-hidden="true">
            🛒
          </span>
          <span className="hidden font-heading text-xl font-extrabold tracking-tight text-foreground sm:inline">
            CenaPlus
          </span>
        </Link>

        {/* Breadcrumbs are secondary: hidden on phones, truncating on tablets. */}
        <div className="hidden min-w-0 items-center gap-3 md:flex">
          {mainCat && (
            <>
              <ChevronRight className="h-4 w-4 flex-shrink-0 text-muted-foreground" aria-hidden="true" />
              <Link
                to={`/?category=${encodeURIComponent(mainCat)}`}
                className="min-w-0 truncate text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
              >
                {mainCat}
              </Link>
            </>
          )}

          {subCat && (
            <>
              <ChevronRight className="h-4 w-4 flex-shrink-0 text-muted-foreground" aria-hidden="true" />
              <span className="min-w-0 truncate text-sm font-medium text-foreground">
                {subCat}
              </span>
            </>
          )}
        </div>

        <div className="ml-auto flex min-w-0 flex-1 items-center justify-end gap-2 sm:flex-none">
          <form onSubmit={handleSearch} className="min-w-0 flex-1 sm:flex-none">
            <div className="relative">
              <Search
                className="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground"
                aria-hidden="true"
              />
              <input
                type="text"
                value={searchValue}
                onChange={(e) => setSearchValue(e.target.value)}
                placeholder="Пребарај производи..."
                aria-label="Пребарај производи"
                className="w-full rounded-md border border-border bg-muted py-1.5 pl-8 pr-3 text-sm text-foreground transition-all placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary sm:w-48 lg:w-64"
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
