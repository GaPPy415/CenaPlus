import { ArrowLeft } from "lucide-react";
import { Link } from "react-router-dom";

const NotFound = () => (
  <main className="container mx-auto flex max-w-2xl flex-col items-center justify-center px-4 py-24 text-center">
    <h1 className="font-heading text-5xl font-extrabold text-foreground">404</h1>
    <p className="mt-3 text-lg text-muted-foreground">Страницата не е пронајдена</p>
    <Link
      to="/"
      className="mt-6 inline-flex min-h-[44px] items-center gap-2 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
    >
      <ArrowLeft className="h-4 w-4" aria-hidden="true" />
      Кон почетна
    </Link>
  </main>
);

export default NotFound;
