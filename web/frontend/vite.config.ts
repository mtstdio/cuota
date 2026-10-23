import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The bundle is embedded in the Go plugin (internal/mgmt/dist) and served from
// /v0/resource/plugins/kpatdev-quota-router/, so asset URLs must be relative.
export default defineConfig({
  base: "./",
  plugins: [react()],
  build: {
    outDir: "../dist",
    emptyOutDir: true,
    assetsDir: "assets",
    sourcemap: false,
    modulePreload: { polyfill: false },
    target: "es2022",
  },
  server: {
    // `pnpm dev` talks to a CPA instance running the plugin (see scripts/demo.py).
    proxy: { "/v0": "http://127.0.0.1:18318" },
  },
  test: { environment: "jsdom" },
} as never);
