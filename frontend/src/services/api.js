import axios from "axios";

const api = axios.create({
  baseURL: "http://localhost:8000",
  timeout: 10000,
});

export const fetchTopSkills = (role, limit = 10) =>
  api.get("/analytics/top-skills", { params: { role, limit } });

export const fetchSkillGap = (payload) => api.post("/analytics/skill-gap", payload);

export const fetchJobs = () => api.get("/jobs");

export const fetchAnalyticsJobs = (params) => api.get("/analytics/jobs", { params });

export const fetchAnalyticsSkills = (params) => api.get("/analytics/skills", { params });

export const fetchAnalyticsLocations = (params) => api.get("/analytics/locations", { params });

export const fetchAnalyticsTrends = (params) => api.get("/analytics/trends", { params });

export const fetchJobPostings = () => api.get("/job-postings");

export default api;
