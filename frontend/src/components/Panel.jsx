function Panel({ title, subtitle, children, actions }) {
  return (
    <div className="rounded-2xl border border-line bg-panel p-6 shadow-panel">
      <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h2 className="text-lg font-medium tracking-tight text-slateInk">{title}</h2>
          {subtitle ? (
            <p className="mt-1 max-w-2xl text-sm font-light leading-6 text-muted">{subtitle}</p>
          ) : null}
        </div>
        {actions ? <div className="shrink-0">{actions}</div> : null}
      </div>
      {children}
    </div>
  );
}

export default Panel;
