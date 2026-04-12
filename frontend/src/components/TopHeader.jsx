function TopHeader() {
  return (
    <header className="rounded-2xl border border-line bg-panel px-6 py-5 shadow-shell">
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-indigoSoft text-lg font-semibold text-primary">
              H
            </div>
            <div>
              <p className="text-xs font-medium uppercase tracking-[0.22em] text-primary">
                HireGraph
              </p>
              <p className="mt-1 text-sm font-light text-muted">Analytics workspace</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <button
              type="button"
              className="rounded-xl border border-line bg-appBg px-4 py-2.5 text-sm font-light text-muted"
            >
              Live API
            </button>
            <button
              type="button"
              className="rounded-xl bg-primary px-4 py-2.5 text-sm font-medium text-white shadow-sm"
            >
              Refresh Insights
            </button>
          </div>
        </div>

        <div>
          <h1 className="text-3xl font-medium tracking-tight text-slateInk">
            Job Market Intelligence Dashboard
          </h1>
          <p className="mt-2 max-w-2xl text-sm font-light leading-6 text-muted">
            Query canonical jobs, track skill demand, and compare your profile against the
            market with reusable analytics APIs.
          </p>
        </div>
        <div className="grid gap-3 md:grid-cols-3">
          <div className="rounded-2xl border border-line bg-appBg px-4 py-4">
            <p className="text-xs uppercase tracking-[0.18em] text-muted">Insights</p>
            <p className="mt-2 text-base font-medium text-slateInk">Top Skills</p>
            <p className="mt-1 text-sm font-light text-muted">Demand-ranked by role search</p>
          </div>
          <div className="rounded-2xl border border-line bg-appBg px-4 py-4">
            <p className="text-xs uppercase tracking-[0.18em] text-muted">Analysis</p>
            <p className="mt-2 text-base font-medium text-slateInk">Skill Gap</p>
            <p className="mt-1 text-sm font-light text-muted">Market fit for your current stack</p>
          </div>
          <div className="rounded-2xl border border-line bg-appBg px-4 py-4">
            <p className="text-xs uppercase tracking-[0.18em] text-muted">Source</p>
            <p className="mt-2 text-base font-medium text-slateInk">API Driven</p>
            <p className="mt-1 text-sm font-light text-muted">Backed by async ingestion and analytics</p>
          </div>
        </div>
      </div>
    </header>
  );
}

export default TopHeader;
