"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { NAV_ITEMS } from "@/lib/navigation";
import { cn } from "@/lib/utils";

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <div className="min-h-screen bg-[#0e1116] text-zinc-100">
      <div className="mx-auto grid min-h-screen max-w-[1440px] md:grid-cols-[232px_1fr]">
        <aside className="border-b border-white/8 bg-[#12151c] md:border-r md:border-b-0">
          <div className="px-4 py-5">
            <p className="text-xs tracking-[0.2em] text-sky-300/80 uppercase">TITAN</p>
            <p className="mt-1 text-sm text-zinc-300">Operations</p>
          </div>
          <nav className="flex gap-1 overflow-x-auto px-2 pb-3 md:flex-col md:px-2" aria-label="Primary">
            {NAV_ITEMS.map((item) => {
              const active = pathname === item.href;
              const Icon = item.icon;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  aria-current={active ? "page" : undefined}
                  className={cn(
                    "flex shrink-0 items-center gap-2 rounded-md px-3 py-2 text-sm text-zinc-400 hover:bg-white/5 hover:text-zinc-100",
                    active && "bg-sky-950/70 text-sky-50",
                  )}
                >
                  <Icon className="size-4" aria-hidden />
                  <span>{item.label}</span>
                </Link>
              );
            })}
          </nav>
        </aside>
        <div className="min-w-0">
          <header className="flex flex-wrap items-center justify-between gap-3 border-b border-zinc-800 px-4 py-3 md:px-6">
            <div>
              <p className="text-sm text-zinc-400">Local environment</p>
              <p className="text-xs text-amber-200/90">
                Business events are synthetic. They are not live store traffic.
              </p>
            </div>
            <span className="rounded-md border border-amber-900/80 bg-amber-950/40 px-2 py-1 text-xs text-amber-200">
              synthetic data
            </span>
          </header>
          <main className="px-4 py-6 md:px-6">{children}</main>
        </div>
      </div>
    </div>
  );
}
