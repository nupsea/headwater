import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import Link from "next/link";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Headwater",
  description: "Advisory data platform for data professionals",
};

const navItems = [
  { href: "/", label: "Dashboard" },
  { href: "/discovery", label: "Discovery" },
  { href: "/dictionary", label: "Dictionary" },
  { href: "/models", label: "Models" },
  { href: "/quality", label: "Quality" },
  { href: "/data", label: "Data" },
  { href: "/explore", label: "Explore" },
];

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="min-h-full flex flex-col">
        <nav className="border-b border-border bg-card px-6 py-3 flex items-center gap-8">
          <Link href="/" className="font-bold text-lg tracking-tight">
            Headwater
          </Link>
          <div className="flex gap-6 text-sm">
            {navItems.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className="text-muted hover:text-foreground transition-colors"
              >
                {item.label}
              </Link>
            ))}
          </div>
        </nav>
        <main className="flex-1 px-6 py-6 max-w-7xl mx-auto w-full">
          {children}
        </main>
      </body>
    </html>
  );
}
