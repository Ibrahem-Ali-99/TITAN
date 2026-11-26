"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Bell, ChevronLeft, Menu, Moon, RefreshCw, Search, Sun, X } from "lucide-react";

import { NAV_ITEMS, type NavItem } from "@/lib/navigation";
import { cn } from "@/lib/utils";

const groups = [
  { label: "Workspace", items: ["/", "/streaming", "/data-sources"] },
  { label: "Data platform", items: ["/pipelines", "/warehouse", "/data-quality"] },
  { label: "Intelligence", items: ["/monitoring", "/ml"] },
] as const;

function Mark() {
  return (
    <span className="titan-mark" aria-hidden>
      <svg viewBox="0 0 32 32" fill="none">
        <path d="M16 2 30 16 16 30 2 16 16 2Z" stroke="currentColor" strokeWidth="1.7" />
        <path d="m16 7 9 9-9 9-9-9 9-9Z" stroke="currentColor" strokeWidth="1.1" opacity=".55" />
        <path d="m16 7 9 9h-9V7Zm0 18-9-9h9v9Z" fill="currentColor" opacity=".32" />
      </svg>
    </span>
  );
}

function NavLink({ item, onNavigate }: { item: NavItem; onNavigate: () => void }) {
  const pathname = usePathname();
  const active = pathname === item.href;
  const Icon = item.icon;
  return (
    <Link
      href={item.href}
      prefetch={false}
      onClick={onNavigate}
      aria-current={active ? "page" : undefined}
      className={cn("titan-nav-link", active && "is-active")}
    >
      <Icon className="size-4" aria-hidden />
      <span>{item.label === "ML & Predictions" ? "Machine Learning" : item.label}</span>
    </Link>
  );
}

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [light, setLight] = useState(false);
  const [query, setQuery] = useState("");

  useEffect(() => {
    document.documentElement.dataset.theme = light ? "light" : "dark";
  }, [light]);

  const current = NAV_ITEMS.find((item) => item.href === pathname) ?? NAV_ITEMS[0];
  const matches = useMemo(
    () => NAV_ITEMS.filter((item) => `${item.label} ${item.summary}`.toLowerCase().includes(query.trim().toLowerCase())),
    [query],
  );

  return (
    <div className={cn("titan-app", collapsed && "is-collapsed")}>
      {mobileOpen ? <button className="titan-mobile-scrim" aria-label="Close navigation" onClick={() => setMobileOpen(false)} /> : null}
      <aside className={cn("titan-sidebar", mobileOpen && "mobile-open")}>
        <div className="titan-brand">
          <Link href="/" aria-label="TITAN overview" onClick={() => setMobileOpen(false)}>
            <Mark />
            <span>
              <b>TITAN</b>
              <small>DATA INTELLIGENCE</small>
            </span>
          </Link>
          <button className="titan-mobile-close" aria-label="Close navigation" onClick={() => setMobileOpen(false)}>
            <X className="size-4" />
          </button>
          <button className="titan-collapse" aria-label={collapsed ? "Expand navigation" : "Collapse navigation"} onClick={() => setCollapsed((value) => !value)}>
            <ChevronLeft className="size-4" />
          </button>
        </div>
          <nav className="titan-nav" aria-label="Primary">
          {groups.map((group) => (
            <div className="titan-nav-group" key={group.label}>
              <p>{group.label}</p>
              {group.items.map((href) => {
                const item = NAV_ITEMS.find((candidate) => candidate.href === href);
                return item ? <NavLink item={item} onNavigate={() => setMobileOpen(false)} key={item.href} /> : null;
              })}
            </div>
          ))}
        </nav>
        <div className="titan-environment">
          <span className="titan-live-dot" />
          <div>
            <b>Local environment</b>
            <small>Synthetic commerce dataset</small>
          </div>
        </div>
      </aside>
      <div className="titan-main">
        <header className="titan-header">
          <div className="titan-header-title">
            <button className="titan-mobile-menu" aria-label="Open navigation" onClick={() => setMobileOpen(true)}>
              <Menu className="size-5" />
            </button>
            <span>TITAN /</span>
            <strong>{current.label === "ML & Predictions" ? "Machine Learning" : current.label}</strong>
          </div>
          <div className="titan-header-actions">
            <div className="titan-search-wrap">
              <Search className="size-4" aria-hidden />
              <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search screens…" aria-label="Search screens" />
              <kbd>⌘ K</kbd>
              {query ? (
                <div className="titan-search-results">
                  {matches.length ? matches.map((item) => <NavLink item={item} onNavigate={() => setQuery("")} key={item.href} />) : <span>No matching screens</span>}
                </div>
              ) : null}
            </div>
            <span className="titan-health-pill"><i /> All systems nominal</span>
            <button className="titan-icon-button" title="Refresh current screen" aria-label="Refresh current screen" onClick={() => window.location.reload()}>
              <RefreshCw className="size-4" />
            </button>
            <button className="titan-icon-button" title={light ? "Use dark theme" : "Use light theme"} aria-label={light ? "Use dark theme" : "Use light theme"} aria-pressed={light} onClick={() => setLight((value) => !value)}>
              {light ? <Moon className="size-4" /> : <Sun className="size-4" />}
            </button>
            <button className="titan-icon-button" title="Notifications are read-only in this console" aria-label="Notifications">
              <Bell className="size-4" />
            </button>
            <span className="titan-version">v0.2.0</span>
          </div>
        </header>
        <main className="titan-content">{children}</main>
      </div>
    </div>
  );
}
