import { Link, useLocation } from "react-router-dom";
import { ChevronRight } from "lucide-react";
import ThemeToggle from "@/components/ThemeToggle";

const Header = () => {
  const location = useLocation();
  const params = new URLSearchParams(location.search);
  const mainCat = params.get("category");
  const subCat = params.get("sub");

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

        <div className="ml-auto">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
};

export default Header;
