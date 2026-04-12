function Sidebar({ sections, activeSection, onNavigate }) {
  return (
    <aside className="border-b border-line/80 bg-white/80 p-5 backdrop-blur-sm lg:min-h-screen lg:w-72 lg:border-b-0 lg:border-r">
      <div className="rounded-2xl border border-line bg-panel p-5 shadow-panel">
        <p className="text-xs font-medium uppercase tracking-[0.22em] text-accent">Navigation</p>
        <h1 className="mt-2 text-2xl font-medium tracking-tight text-slateInk">
          Market Intelligence
        </h1>
        <p className="mt-2 text-sm font-light leading-6 text-muted">
          Explore skill demand, uncover gaps, and browse canonical job insights.
        </p>
      </div>
      <nav className="mt-6 grid gap-2">
        {sections.map((section) => {
          const isActive = activeSection === section.id;
          return (
            <button
              key={section.id}
              type="button"
              onClick={() => {
                onNavigate(section.id);
                document.getElementById(section.id)?.scrollIntoView({ behavior: "smooth" });
              }}
              className={`rounded-xl border px-4 py-3 text-left text-sm transition ${
                isActive
                  ? "border-primary/20 bg-indigoSoft font-medium text-primary shadow-sm"
                  : "border-transparent bg-transparent font-light text-muted hover:border-line hover:bg-appBg hover:text-slateInk"
              }`}
            >
              {section.label}
            </button>
          );
        })}
      </nav>
      <div className="mt-8 rounded-2xl border border-line bg-appBgSoft px-4 py-4">
        <p className="text-xs uppercase tracking-[0.18em] text-muted">Workspace</p>
        <p className="mt-2 text-sm font-medium text-slateInk">Professional, minimal, API-first</p>
        <p className="mt-2 text-sm font-light leading-6 text-muted">
          Clean cards, fast analytics, and a focused dashboard for decision-making.
        </p>
      </div>
    </aside>
  );
}

export default Sidebar;
