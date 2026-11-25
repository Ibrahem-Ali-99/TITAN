import type { Metadata } from "next";

import { Shell } from "@/components/shell";

import "./globals.css";

export const metadata: Metadata = {
  title: "TITAN",
  description: "Local data operations console for synthetic commerce events",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full">
        <Shell>{children}</Shell>
      </body>
    </html>
  );
}
