import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Pair Analytics Dashboard",
  description: "Polymarket pair measurement analytics and strategy optimization",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className="font-sans antialiased">
        {children}
      </body>
    </html>
  );
}
