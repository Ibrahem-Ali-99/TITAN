import type { Metadata } from "next";

import { Shell } from "@/components/shell";

import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "TITAN · Data Intelligence",
    template: "%s · TITAN",
  },
  description: "TITAN enterprise data intelligence and operations console",
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
