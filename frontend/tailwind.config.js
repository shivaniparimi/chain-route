/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // A restrained slate/neutral base (Tailwind's default slate scale
        // covers that) plus two custom palettes layered on top:
        //   - `brand`: the single accent used sparingly, for the
        //     selected-route / selected-provider highlight and primary
        //     interactive affordances. Teal rather than the ubiquitous
        //     indigo/purple SaaS default, since ChainRoute's own domain
        //     (bridges/corridors) reads better in a "signal" cyan-teal
        //     than in generic dashboard purple.
        //   - `status`: semantic colors reused everywhere a payment
        //     status renders, mapped consistently across the whole app:
        //     completed (green), the ROUTED/PROCESSING/SUBMITTED
        //     in-flight family (amber), failed (red), and a neutral
        //     informational blue for non-status chrome (e.g. counts,
        //     links).
        brand: {
          50: "#effefb",
          100: "#c8fdf1",
          200: "#93fae3",
          300: "#57eed2",
          400: "#22d3bd",
          500: "#0bb6a3",
          600: "#059186",
          700: "#08746d",
          800: "#0b5c58",
          900: "#0d4c49",
        },
        status: {
          completed: "#16a34a",
          "completed-bg": "#dcfce7",
          processing: "#d97706",
          "processing-bg": "#fef3c7",
          failed: "#dc2626",
          "failed-bg": "#fee2e2",
          info: "#2563eb",
          "info-bg": "#dbeafe",
        },
      },
    },
  },
  plugins: [],
}

