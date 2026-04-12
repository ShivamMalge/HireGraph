import { useEffect, useState } from "react";
import { fetchJobs } from "../services/api";
import Panel from "./Panel";

function JobsList() {
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let ignore = false;

    async function loadJobs() {
      setLoading(true);
      setError("");
      try {
        const response = await fetchJobs();
        if (!ignore) {
          setJobs(response.data);
        }
      } catch (err) {
        if (!ignore) {
          setError("Unable to load job insights right now.");
          setJobs([]);
        }
      } finally {
        if (!ignore) {
          setLoading(false);
        }
      }
    }

    loadJobs();
    return () => {
      ignore = true;
    };
  }, []);

  return (
    <Panel
      title="Canonical Job Insights"
      subtitle="Browse the latest canonical jobs produced by the processing pipeline."
    >
      {loading ? (
        <div className="py-12 text-center text-sm font-light text-muted">Loading jobs...</div>
      ) : error ? (
        <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-600">
          {error}
        </div>
      ) : jobs.length === 0 ? (
        <div className="rounded-2xl border border-line bg-appBg px-4 py-4 text-sm font-light text-muted">
          No canonical jobs available yet.
        </div>
      ) : (
        <div className="grid gap-3">
          {jobs.map((job) => (
            <div
              key={job.job_id}
              className="rounded-2xl border border-line bg-appBg px-4 py-4"
            >
              <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                <div>
                  <h3 className="text-lg font-medium text-slateInk">{job.canonical_title}</h3>
                  <p className="mt-1 text-sm font-light text-muted">
                    {job.canonical_location || "Location unavailable"}
                  </p>
                  <p className="mt-1 text-xs uppercase tracking-[0.2em] text-muted">
                    Pending company resolution
                  </p>
                </div>
                <div className="rounded-full bg-indigoSoft px-3 py-1 text-xs font-medium uppercase tracking-[0.2em] text-primary">
                  {job.job_status}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

export default JobsList;
