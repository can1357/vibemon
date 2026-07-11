import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

// Build straight into the Rust server crate so `vmon serve` embeds the SPA.
// The dev server proxies API routes to a local `vmon serve` instance on :8000.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: resolve(__dirname, "..", "vmond", "web"),
    emptyOutDir: true,
    sourcemap: false,
  },
  server: {
    port: 5173,
    proxy: {
      "/v1": { target: "http://127.0.0.1:8000", ws: true },
      "/healthz": { target: "http://127.0.0.1:8000" },
      "/metrics": { target: "http://127.0.0.1:8000" },
    },
  },
});
