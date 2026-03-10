import "../styles/globals.css";

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ko">
      <body style={{ backgroundColor: '#080808', color: '#ededed', margin: 0 }}>
        {children}
      </body>
    </html>
  );
}
