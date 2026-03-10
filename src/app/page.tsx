export default function Page() {
  return (
    <div className="flex h-screen items-center justify-center bg-[#080808]">
      <div className="text-center">
        <h1 className="text-4xl font-bold text-white mb-4 tracking-tighter">MES SAAS REBUILT</h1>
        <p className="text-gray-400">Next.js 서버가 정상적으로 연결되었습니다.</p>
        <div className="mt-8 p-4 border border-[#1f1f22] rounded-lg bg-[#0c0c0c]">
            <span className="text-sm text-[#00ff88]">✓ 시스템 가동 중</span>
        </div>
      </div>
    </div>
  );
}
