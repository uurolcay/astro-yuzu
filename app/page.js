"use client";

import React, { useEffect, useState } from "react";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_ASTRO_API_BASE_URL || "http://127.0.0.1:8000";

function normalizeApiData(payload) {
  const data = payload?.data;
  const natal = data?.natal;

  if (!data || !natal) {
    return null;
  }

  return {
    interpretation: data.ai_insight || "Interpretation engine unavailable.",
    personalPoints: natal.karakas || {},
    natalProfile: natal.planets || [],
    transitHighlights: data.transit_highlights || [],
    source: payload.source || "unknown",
  };
}

export default function Home() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [formData, setFormData] = useState({
    birthDate: "1991-02-12T14:30",
    birthCity: "Istanbul",
  });

  const runEngine = async () => {
    setLoading(true);
    setError("");

    try {
      const query = new URLSearchParams({
        date: formData.birthDate,
        city: formData.birthCity,
      });

      const response = await fetch(
        `${API_BASE_URL}/api/v1/natal?${query.toString()}`
      );

      if (!response.ok) {
        throw new Error(`Engine request failed with status ${response.status}`);
      }

      const result = await response.json();
      const normalizedData = normalizeApiData(result);

      if (!normalizedData) {
        throw new Error("Engine response is missing natal data.");
      }

      setData(normalizedData);
    } catch (err) {
      console.error(err);
      setData(null);
      setError(err instanceof Error ? err.message : "Unknown engine error");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    runEngine();
  }, []);

  return (
    <div className="min-h-screen bg-[#020617] p-8 font-serif text-[#C5A059] selection:bg-[#C5A059]/20 md:p-16">
      <div className="fixed inset-0 pointer-events-none overflow-hidden opacity-20">
        <div className="absolute left-1/4 top-0 h-96 w-96 rounded-full bg-[#C5A059] blur-[150px]" />
      </div>

      <main className="relative z-10 mx-auto max-w-6xl">
        <header className="mb-16 text-center">
          <h1 className="bg-gradient-to-b from-[#F1D302] to-[#8A6240] bg-clip-text text-4xl font-bold uppercase tracking-[0.3em] text-transparent md:text-6xl">
            Jyotish Engine
          </h1>
          <p className="mt-4 text-[10px] tracking-[0.5em] opacity-60">
            Astrology to Life Language | Phase 3+5+6
          </p>
        </header>

        <section className="mb-12 rounded-2xl border border-[#C5A059]/20 bg-[#001F54]/10 p-8 shadow-2xl backdrop-blur-xl">
          <div className="grid grid-cols-1 items-end gap-8 md:grid-cols-3">
            <div className="flex flex-col gap-2">
              <label className="text-[9px] font-bold uppercase tracking-widest opacity-50">
                Birth Time
              </label>
              <input
                type="datetime-local"
                value={formData.birthDate}
                onChange={(e) =>
                  setFormData({ ...formData, birthDate: e.target.value })
                }
                className="rounded-lg border border-[#C5A059]/30 bg-[#020617] p-3 text-sm outline-none focus:border-[#C5A059]"
              />
            </div>
            <div className="flex flex-col gap-2">
              <label className="text-[9px] font-bold uppercase tracking-widest opacity-50">
                Location
              </label>
              <input
                type="text"
                value={formData.birthCity}
                onChange={(e) =>
                  setFormData({ ...formData, birthCity: e.target.value })
                }
                className="rounded-lg border border-[#C5A059]/30 bg-[#020617] p-3 text-sm outline-none focus:border-[#C5A059]"
              />
            </div>
            <button
              onClick={runEngine}
              disabled={loading}
              className="h-[46px] rounded-lg bg-[#C5A059] text-[10px] font-bold uppercase tracking-widest text-[#020617] shadow-lg shadow-[#C5A059]/10 transition-all hover:bg-[#F1D302] disabled:opacity-30"
            >
              {loading ? "Synthesizing Data..." : "Run Engine"}
            </button>
          </div>
        </section>

        {data && (
          <div className="grid grid-cols-1 gap-12 lg:grid-cols-12">
            <div className="flex flex-col justify-center lg:col-span-7">
              <div className="relative rounded-r-2xl border-l-2 border-[#C5A059] bg-white/[0.02] p-10 shadow-2xl">
                <span className="absolute right-6 top-4 font-serif text-5xl italic leading-none opacity-10">
                  "
                </span>
                <h2 className="mb-8 text-[10px] font-bold uppercase tracking-[0.4em] opacity-40">
                  User Readable Insight
                </h2>
                <p className="text-2xl font-light italic leading-[1.6] tracking-wide text-slate-100 md:text-3xl">
                  {data.interpretation}
                </p>
                <div className="mt-10 flex flex-wrap gap-4">
                  <span className="rounded-full border border-[#C5A059]/30 px-4 py-1 text-[10px] uppercase tracking-tighter opacity-60">
                    Source: {data.source}
                  </span>
                  <span className="rounded-full border border-[#C5A059]/30 px-4 py-1 text-[10px] uppercase tracking-tighter opacity-60">
                    Highlights: {data.transitHighlights.length}
                  </span>
                </div>
              </div>
            </div>

            <div className="space-y-6 lg:col-span-5">
              <div className="grid grid-cols-2 gap-4">
                <div className="rounded-xl border border-[#C5A059]/10 bg-[#C5A059]/5 p-6">
                  <p className="mb-2 text-[9px] uppercase tracking-widest opacity-40">
                    Atmakaraka (Soul)
                  </p>
                  <p className="text-xl font-bold">
                    {data.personalPoints.atmakaraka || "Unknown"}
                  </p>
                </div>
                <div className="rounded-xl border border-[#C5A059]/10 bg-[#C5A059]/5 p-6">
                  <p className="mb-2 text-[9px] uppercase tracking-widest opacity-40">
                    Amatyakaraka (Career)
                  </p>
                  <p className="text-xl font-bold">
                    {data.personalPoints.amatyakaraka || "Unknown"}
                  </p>
                </div>
              </div>

              <div className="rounded-2xl border border-white/5 bg-black/20 p-8">
                <h3 className="mb-6 text-[10px] font-bold uppercase tracking-[0.3em] opacity-40">
                  Technical Blueprint
                </h3>
                <div className="space-y-4">
                  {data.natalProfile.slice(0, 5).map((planet) => (
                    <div
                      key={planet.name}
                      className="flex items-center justify-between border-b border-white/5 pb-2 text-sm"
                    >
                      <span className="opacity-80">{planet.name}</span>
                      <span
                        className="font-mono text-[#8A6240]"
                        dangerouslySetInnerHTML={{
                          __html: `${planet.degree}&deg;`,
                        }}
                      />
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        {error && (
          <div className="mt-8 rounded-2xl border border-rose-500/30 bg-rose-500/10 p-5 text-sm text-rose-100">
            {error}
          </div>
        )}
      </main>
    </div>
  );
}
