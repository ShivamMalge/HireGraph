import { useState } from "react";
import JobTrendsCard from "./components/JobTrendsCard";
import JobsList from "./components/JobsList";
import LocationsCard from "./components/LocationsCard";
import Sidebar from "./components/Sidebar";
import SkillGap from "./components/SkillGap";
import TopHeader from "./components/TopHeader";
import TopSkillsChart from "./components/TopSkillsChart";

const sections = [
  { id: "dashboard", label: "Dashboard" },
  { id: "skill-trends", label: "Skill Trends" },
  { id: "skill-gap", label: "Skill Gap" },
];

function App() {
  const [activeSection, setActiveSection] = useState("dashboard");

  return (
    <div className="min-h-screen bg-appBg text-slateInk">
      <div className="mx-auto flex min-h-screen max-w-[1480px] flex-col lg:flex-row">
        <Sidebar
          sections={sections}
          activeSection={activeSection}
          onNavigate={setActiveSection}
        />
        <main className="flex-1 px-4 py-4 sm:px-6 lg:px-8 xl:px-10">
          <TopHeader />
          <div className="mt-8 space-y-8 pb-8">
            <section id="dashboard" className="space-y-6">
              <div className="grid gap-6 xl:grid-cols-[1.35fr_1fr]">
                <TopSkillsChart />
                <SkillGap compact />
              </div>
              <div className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
                <JobTrendsCard />
                <LocationsCard />
              </div>
              <JobsList />
            </section>
            <section id="skill-trends" className="space-y-6">
              <TopSkillsChart expanded />
              <JobTrendsCard />
            </section>
            <section id="skill-gap" className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
              <SkillGap />
              <LocationsCard />
            </section>
          </div>
        </main>
      </div>
    </div>
  );
}

export default App;
