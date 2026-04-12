/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx,ts,tsx}"],
  theme: {
    extend: {
      colors: {
        appBg: "#F8FAFC",
        appBgSoft: "#F1F5F9",
        primary: "#4F46E5",
        accent: "#7C3AED",
        slateInk: "#0F172A",
        muted: "#64748B",
        panel: "#FFFFFF",
        line: "#E2E8F0",
        indigoSoft: "#EEF2FF",
        purpleSoft: "#F5F3FF",
      },
      boxShadow: {
        panel: "0 16px 40px rgba(15, 23, 42, 0.06)",
        shell: "0 1px 2px rgba(15, 23, 42, 0.04), 0 16px 32px rgba(15, 23, 42, 0.04)",
      },
    },
  },
  plugins: [],
};
