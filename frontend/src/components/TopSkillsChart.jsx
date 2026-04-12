import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fetchTopSkills } from "../services/api";
import Panel from "./Panel";

function TopSkillsChart({ expanded = false }) {
  const [role, setRole] = useState("backend engineer");
  const [inputValue, setInputValue] = useState("backend engineer");
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let ignore = false;

    async function loadSkills() {
      setLoading(true);
      setError("");
      try {
        const response = await fetchTopSkills(role, expanded ? 12 : 8);
        if (!ignore) {
          setData(response.data);
        }
      } catch (err) {
        if (!ignore) {
          setError("Unable to load top skills right now.");
          setData([]);
        }
      } finally {
        if (!ignore) {
          setLoading(false);
        }
      }
    }

    loadSkills();
    return () => {
      ignore = true;
    };
  }, [role, expanded]);

  const handleSubmit = (event) => {
    event.preventDefault();
    setRole(inputValue.trim() || "backend engineer");
  };

  return (
    <Panel
      title="Top Skills by Role"
      subtitle="Search a role and visualize the highest-demand skills from canonical jobs."
      actions={
        <form className="flex gap-2" onSubmit={handleSubmit}>
          <input
            value={inputValue}
            onChange={(event) => setInputValue(event.target.value)}
            className="w-full min-w-0 rounded-xl border border-line bg-appBg px-4 py-2.5 text-sm text-slateInk outline-none ring-0 placeholder:text-muted focus:border-primary/30"
            placeholder="backend engineer"
          />
          <button
            type="submit"
            className="rounded-xl bg-primary px-4 py-2.5 text-sm font-medium text-white transition hover:bg-indigo-600"
          >
            Search
          </button>
        </form>
      }
    >
      {loading ? (
        <div className="flex h-80 items-center justify-center text-sm font-light text-muted">
          Loading chart...
        </div>
      ) : error ? (
        <div className="flex h-80 items-center justify-center rounded-2xl border border-rose-200 bg-rose-50 text-sm text-rose-600">
          {error}
        </div>
      ) : data.length === 0 ? (
        <div className="flex h-80 items-center justify-center rounded-2xl border border-line bg-appBg text-sm font-light text-muted">
          No skill trend data found for this role.
        </div>
      ) : (
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={data} margin={{ top: 10, right: 12, left: -20, bottom: 0 }}>
              <CartesianGrid stroke="#E2E8F0" vertical={false} strokeDasharray="4 4" />
              <XAxis
                dataKey="skill"
                stroke="#64748B"
                tickLine={false}
                axisLine={false}
                tickMargin={10}
              />
              <YAxis stroke="#64748B" tickLine={false} axisLine={false} />
              <Tooltip
                cursor={{ fill: "rgba(79, 70, 229, 0.08)" }}
                contentStyle={{
                  backgroundColor: "#FFFFFF",
                  border: "1px solid #E2E8F0",
                  borderRadius: "16px",
                  color: "#0F172A",
                  boxShadow: "0 10px 30px rgba(15, 23, 42, 0.08)",
                }}
              />
              <Bar dataKey="count" fill="#4F46E5" radius={[12, 12, 0, 0]} maxBarSize={42} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}
    </Panel>
  );
}

export default TopSkillsChart;
