"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { NAV_ITEMS } from "@/lib/navigation";
import { cn } from "@/lib/utils";

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="min-h-screen bg-[var(--background)] text-[var(--foreground)]">
      <div className="mx-auto grid w-full min-w-0 max-w-[1440px] md:grid-cols-[232px_1fr]">
        <aside className="flex min-w-0 flex-col border-b border-[var(--line)] bg-[var(--sidebar)] md:min-h-screen md:border-r md:border-b-0">
          <div className="flex items-center gap-2 px-4 pt-5 pb-4">
            <span className="grid size-7 place-items-center rounded-md border border-[var(--line)] text-sky-300" aria-hidden>
              <svg viewBox="0 0 16 16" className="size-3.5" fill="none" stroke="currentColor" strokeWidth="1.6">
                <path d="M8 1.5 14 8 8 14.5 2 8 8 1.5Z" />
              </svg>
            </span>
            <div>
              <p className="text-sm font-semibold tracking-[0.14em]">TITAN</p>
              <p className="text-[10px] tracking-[0.16em] text-sky-400">DATA INTELLIGENCE</p>
            </div>
          </div>
          <p className="px-4 pb-2 text-[10px] tracking-[0.18em] text-[var(--muted)]">WORKSPACE</p>
          <nav className="flex w-full gap-1 overflow-x-auto px-2 pb-3 md:flex-1 md:flex-col" aria-label="Primary">
            {NAV_ITEMS.map((item) => {
              const active = pathname === item.href;
              const Icon = item.icon;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  aria-current={active ? "page" : undefined}
                  className={cn(
                    "flex shrink-0 items-center gap-2 rounded-lg px-3 py-2 text-sm text-[var(--muted)] hover:bg-white/5 hover:text-[var(--foreground)]",
                    active && "bg-[var(--surface-2)] text-[var(--foreground)]",
                  )}
                >
                  <Icon className="size-4" aria-hidden />
                  <span>{item.label}</span>
                </Link>
              );
            })}
          </nav>
          <div className="border-t border-[var(--line)] px-4 py-4">
            <p className="text-[10px] tracking-[0.16em] text-[var(--muted)]">LOCAL ENVIRONMENT</p>
            <p className="mt-2 inline-flex items-center gap-2 rounded-full border border-[var(--line)] bg-[var(--surface)] px-2.5 py-1 text-xs text-[var(--foreground)]">
              <span className="size-1.5 rounded-full bg-sky-400" aria-hidden />
              Synthetic dataset
            </p>
          </div>
        </aside>
        <div className="min-w-0">
          <main className="px-4 py-5 md:px-6 md:py-6">{children}</main>
        </div>
      </div>
    </div>
  );
}
