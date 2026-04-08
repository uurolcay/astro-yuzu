import "./globals.css";

export const metadata = {
  title: "Jyotish Veda | Celestial Insight",
  description: "Ancient Wisdom, Modern Vision",
};

export default function RootLayout({ children }) {
  return (
    <html lang="tr">
      <body>{children}</body>
    </html>
  );
}
