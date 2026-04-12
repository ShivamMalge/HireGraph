import { useEffect, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fetchAnalyticsTrends } from "../services/api";
import Panel from "./Panel";

function JobTrendsCard() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [timeRange, setTimeRange] = useState("30d");

  useEffect(() => {
    let ignore = false;

    async function loadTrends() {
      setLoading(true);
      setError("");
      try {
        const response = await fetchAnalyticsTrends({ time_range: timeRange });
        if (!ignore) {
          setData((response.data.data || []).slice().reverse());
        }
      } catch (err) {
        if (!ignore) {
          setError("Unable to load job trends right now.");
        }
      } finally {
        if (!ignore) {
          setLoading(false);
        }
      }
    }

    loadTrends();
    return () => {
      ignore = true;
    };
  }, [timeRange]);

  return (
    <Panel
      title="Job Trends"
      subtitle="Recent job creation trend across the last 30 days."
      actions={
        <select
          value={timeRange}
          onChange={(event) => setTimeRange(event.target.value)}
          className="rounded-xl border border-line bg-appBg px-4 py-2.5 text-sm text-slateInk outline-none"
        >
          <option value="7d">Last 7 days</option>
          <option value="30d">Last 30 days</option>
        </select>
      }
    >
      {loading ? (
        <div className="flex h-72 items-center justify-center text-sm font-light text-muted">
          Loading trends...
        </div>
      ) : error ? (
        <div className="flex h-72 items-center justify-center rounded-2xl border border-rose-200 bg-rose-50 text-sm text-rose-600">
          {error}
        </div>
      ) : data.length === 0 ? (
        <div className="flex h-72 items-center justify-center rounded-2xl border border-line bg-appBg text-sm font-light text-muted">
          No trend data available.
        </div>
      ) : (
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={data} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid stroke="#E2E8F0" vertical={false} strokeDasharray="4 4" />
              <XAxis dataKey="date" stroke="#64748B" tickLine={false} axisLine={false} />
              <YAxis stroke="#64748B" tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{
                  backgroundColor: "#FFFFFF",
                  border: "1px solid #E2E8F0",
                  borderRadius: "16px",
                  color: "#0F172A",
                  boxShadow: "0 10px 30px rgba(15, 23, 42, 0.08)",
                }}
              />
              <Area
                type="monotone"
                dataKey="job_count"
                stroke="#7C3AED"
                fill="rgba(124, 58, 237, 0.12)"
                strokeWidth={2}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}
    </Panel>
  );
}

export default JobTrendsCard;
