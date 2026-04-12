import { useEffect, useState } from "react";
import { fetchAnalyticsLocations } from "../services/api";
import Panel from "./Panel";

function LocationsCard() {
  const [locations, setLocations] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    let ignore = false;

    async function loadLocations() {
      setLoading(true);
      setError("");
      try {
        const response = await fetchAnalyticsLocations({ limit: 6 });
        if (!ignore) {
          setLocations(response.data || []);
        }
      } catch (err) {
        if (!ignore) {
          setError("Unable to load locations right now.");
        }
      } finally {
        if (!ignore) {
          setLoading(false);
        }
      }
    }

    loadLocations();
    return () => {
      ignore = true;
    };
  }, []);

  return (
    <Panel
      title="Locations"
      subtitle="Top hiring locations across the current job dataset."
    >
      {loading ? (
        <div className="py-12 text-center text-sm font-light text-muted">Loading locations...</div>
      ) : error ? (
        <div className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-600">
          {error}
        </div>
      ) : locations.length === 0 ? (
        <div className="rounded-2xl border border-line bg-appBg px-4 py-4 text-sm font-light text-muted">
          No location data available.
        </div>
      ) : (
        <div className="space-y-3">
          {locations.map((item) => (
            <div
              key={item.canonical_location}
              className="flex items-center justify-between rounded-2xl border border-line bg-appBg px-4 py-3"
            >
              <div>
                <p className="font-medium text-slateInk">{item.canonical_location}</p>
                <p className="text-xs uppercase tracking-[0.18em] text-muted">Location demand</p>
              </div>
              <div className="rounded-full bg-purpleSoft px-3 py-1 text-sm font-medium text-accent">
                {item.job_count}
              </div>
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

export default LocationsCard;
