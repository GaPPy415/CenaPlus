import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ThemeProvider } from "next-themes";
import { CartProvider } from "@/context/CartContext";
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import Index from "./pages/Index";
import NotFound from "./pages/NotFound";
import SearchResults from "./pages/SearchResults";
import ProductDetail from "./pages/ProductDetail";
import ManufacturerView from "./pages/ManufacturerView";

const queryClient = new QueryClient();

const App = () => (
  <QueryClientProvider client={queryClient}>
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <CartProvider>
        <BrowserRouter>
          <div className="min-h-screen flex flex-col">
            <Header />
            <div className="flex-1">
              <Routes>
                <Route path="/" element={<Index />} />
                <Route path="/search" element={<SearchResults />} />
                <Route path="/product" element={<ProductDetail />} />
                <Route path="/manufacturer" element={<ManufacturerView />} />
                <Route path="*" element={<NotFound />} />
              </Routes>
            </div>
            <Footer />
          </div>
        </BrowserRouter>
      </CartProvider>
    </ThemeProvider>
  </QueryClientProvider>
);

export default App;
