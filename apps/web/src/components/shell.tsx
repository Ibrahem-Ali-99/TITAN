"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Bell, ChevronLeft, ChevronRight, Menu, Moon, RefreshCw, Search, Sun, X } from "lucide-react";

import { NAV_ITEMS, type NavItem } from "@/lib/navigation";
import { cn } from "@/lib/utils";

const groups = [
  { label: "WORKSPACE", items: ["/", "/streaming", "/data-sources"] },
  { label: "DATA PLATFORM", items: ["/pipelines", "/warehouse", "/data-quality"] },
  { label: "INTELLIGENCE", items: ["/monitoring", "/ml"] },
] as const;

function Mark() {
  return <span className="logo" aria-hidden><i /><i /><i /></span>;
}

function NavLink({ item, onNavigate }: { item: NavItem; onNavigate: () => void }) {
  const pathname = usePathname();
  const active = pathname === item.href;
  const Icon = item.icon;
  return (
    <Link
      href={item.href}
      prefetch={false}
      title={item.label === "ML & Predictions" ? "Machine Learning" : item.label}
      onClick={onNavigate}
      aria-current={active ? "page" : undefined}
      className={cn(active && "active")}
    >
      <Icon className="icon" aria-hidden />
      <span>{item.label === "ML & Predictions" ? "Machine Learning" : item.label}</span>
      {active ? <i aria-hidden /> : null}
    </Link>
  );
}

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [light, setLight] = useState(false);
  const [query, setQuery] = useState("");

  useEffect(() => { document.documentElement.dataset.theme = light ? "light" : "dark"; }, [light]);

  const current = NAV_ITEMS.find((item) => item.href === pathname) ?? NAV_ITEMS[0];
  const title = current.label === "ML & Predictions" ? "Machine Learning" : current.label;
  const matches = useMemo(
    () => NAV_ITEMS.filter((item) => `${item.label} ${item.summary}`.toLowerCase().includes(query.trim().toLowerCase())),
    [query],
  );

  return (
    <div className={cn("app", light && "light", collapsed && "is-collapsed")}>
      {mobileOpen ? <button className="mobile-scrim" aria-label="Close navigation" onClick={() => setMobileOpen(false)} /> : null}
      <aside className={cn("sidebar", mobileOpen && "mobile-open")}>
        <div className="brand">
          <Link href="/" aria-label="TITAN overview" onClick={() => setMobileOpen(false)}><Mark /><span>TITAN</span></Link>
          <button className="collapse" aria-label={collapsed ? "Expand navigation" : "Collapse navigation"} onClick={() => setCollapsed((value) => !value)}>{collapsed ? <ChevronRight className="icon" /> : <ChevronLeft className="icon" />}</button>
          <button className="mobile-close" aria-label="Close navigation" onClick={() => setMobileOpen(false)}><X className="icon" /></button>
        </div>
        <nav aria-label="Primary">
          {groups.map((group) => (
            <div className="nav-group" key={group.label}>
              <label>{group.label}</label>
              {group.items.map((href) => {
                const item = NAV_ITEMS.find((candidate) => candidate.href === href);
                return item ? <NavLink item={item} onNavigate={() => setMobileOpen(false)} key={item.href} /> : null;
              })}
            </div>
          ))}
        </nav>
        <div className="environment"><span><i /> Local Environment</span><small>TITAN v0.2.0 · synthetic commerce dataset</small></div>
      </aside>
      <main>
        <header>
          <div className="header-title"><button className="mobile-menu" aria-label="Open navigation" onClick={() => setMobileOpen(true)}><Menu className="icon" /></button><span>TITAN /</span><b>{title}</b></div>
          <div className="header-actions">
            <div className="search-shell">
              <label className="global-search"><Search className="icon" aria-hidden /><input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search TITAN" aria-label="Search screens" /><kbd>⌘ K</kbd></label>
              {query ? <div className="search-results">{matches.length ? matches.map((item) => <NavLink item={item} onNavigate={() => setQuery("")} key={item.href} />) : <span>No matching screens</span>}</div> : null}
            </div>
            <label className="time-range-control" title="Applies to observed activity charts only; snapshot metrics remain current"><span>Charts</span><select aria-label="Chart time range (observed activity charts only)" defaultValue="12h"><option>1h</option><option>12h</option><option>24h</option><option>7d</option><option>30d</option></select></label>
            <span className="health-pill"><i /> Live data connected</span>
            <button className="icon-btn" title="Refresh current screen" aria-label="Refresh current screen" onClick={() => window.location.reload()}><RefreshCw className="icon" /></button>
            <button className="icon-btn" title={light ? "Use dark theme" : "Use light theme"} aria-label={light ? "Use dark theme" : "Use light theme"} aria-pressed={light} onClick={() => setLight((value) => !value)}>{light ? <Moon className="icon" /> : <Sun className="icon" />}</button>
            <button className="icon-btn notification" title="Notifications are read-only in this console" aria-label="Notifications"><Bell className="icon" /><i /></button>
            <span className="avatar" aria-label="Local operator">TA</span>
          </div>
        </header>
        <div className="content">{children}</div>
      </main>
    </div>
  );
}
