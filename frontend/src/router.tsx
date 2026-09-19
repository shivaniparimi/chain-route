import { NavLink, Route, Routes } from "react-router-dom";

import { OverviewPage } from "./pages/OverviewPage";
import { PaymentDetailPage } from "./pages/PaymentDetailPage";
import { PaymentExplorerPage } from "./pages/PaymentExplorerPage";

const navLinkClass = ({ isActive }: { isActive: boolean }) =>
  `rounded-md px-3 py-2 text-sm font-medium ${
    isActive ? "bg-brand-50 text-brand-700" : "text-slate-600 hover:bg-slate-100 hover:text-slate-900"
  }`;

// Three routes, each backed by its real page as of Task 12 (the Overview
// Dashboard, the last of the three to get real content) -- no stub pages
// remain.
export function AppRoutes() {
  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto flex max-w-6xl items-center gap-6 px-6 py-4">
          <span className="text-lg font-semibold text-slate-900">ChainRoute</span>
          <nav className="flex gap-2">
            <NavLink to="/" className={navLinkClass} end>
              Overview
            </NavLink>
            <NavLink to="/payments" className={navLinkClass}>
              Payments
            </NavLink>
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-6xl">
        <Routes>
          <Route path="/" element={<OverviewPage />} />
          <Route path="/payments" element={<PaymentExplorerPage />} />
          <Route path="/payments/:id" element={<PaymentDetailPage />} />
        </Routes>
      </main>
    </div>
  );
}
