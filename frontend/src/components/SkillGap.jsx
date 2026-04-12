import { useState } from "react";
import { fetchSkillGap } from "../services/api";
import Panel from "./Panel";

function SkillGap({ compact = false }) {
  const [role, setRole] = useState("backend engineer");
  const [skills, setSkills] = useState("python, sql");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (event) => {
    event.preventDefault();
    setLoading(true);
    setError("");

    try {
      const response = await fetchSkillGap({
        role,
        skills: skills
          .split(",")
          .map((skill) => skill.trim())
          .filter(Boolean),
      });
      setResults(response.data);
    } catch (err) {
      setError("Unable to calculate skill gap right now.");
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Panel
      title="Skill Gap Analysis"
      subtitle="Compare your current skill set against market demand for a target role."
    >
      <form className="grid gap-3" onSubmit={handleSubmit}>
        <input
          value={role}
          onChange={(event) => setRole(event.target.value)}
          className="rounded-xl border border-line bg-appBg px-4 py-3 text-sm text-slateInk outline-none placeholder:text-muted focus:border-primary/30"
          placeholder="Role"
        />
        <textarea
          value={skills}
          onChange={(event) => setSkills(event.target.value)}
          rows={compact ? 2 : 3}
          className="rounded-xl border border-line bg-appBg px-4 py-3 text-sm text-slateInk outline-none placeholder:text-muted focus:border-primary/30"
          placeholder="python, sql, aws"
        />
        <button
          type="submit"
          className="rounded-xl bg-accent px-4 py-3 text-sm font-medium text-white transition hover:bg-violet-700"
        >
          {loading ? "Analyzing..." : "Find Skill Gaps"}
        </button>
      </form>

      <div className="mt-5">
        {error ? (
          <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-600">
            {error}
          </div>
        ) : results.length === 0 ? (
          <div className="rounded-2xl border border-line bg-appBg px-4 py-4 text-sm font-light text-muted">
            Submit a role and your current skills to see the highest-demand gaps.
          </div>
        ) : (
          <div className="space-y-3">
            {results.map((item) => (
              <div
                key={item.skill}
                className="flex items-center justify-between rounded-2xl border border-line bg-appBg px-4 py-3"
              >
                <div>
                  <p className="font-medium text-slateInk">{item.skill}</p>
                  <p className="text-xs uppercase tracking-[0.2em] text-muted">Missing skill</p>
                </div>
                <div className="rounded-full bg-purpleSoft px-3 py-1 text-sm font-medium text-accent">
                  demand {item.demand}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </Panel>
  );
}

export default SkillGap;
