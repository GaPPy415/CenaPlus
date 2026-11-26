import { Github } from "lucide-react";

const REPO_URL = "https://github.com/GaPPy415/CenaPlus";

const Footer = () => (
  <footer className="border-t border-border mt-12">
    <div className="container max-w-6xl mx-auto px-4 py-6 flex flex-col sm:flex-row items-center justify-between gap-3 text-sm text-muted-foreground">
      <p>© {new Date().getFullYear()} CenaPlus - Податоците се собираат од јавни ценовници на маркетите</p>
      <a
        href={REPO_URL}
        target="_blank"
        rel="noopener noreferrer"
        className="inline-flex items-center gap-2 hover:text-foreground transition-colors"
      >
        <Github className="h-5 w-5" aria-hidden="true" />
        Отворен код на GitHub
      </a>
    </div>
  </footer>
);

export default Footer;
