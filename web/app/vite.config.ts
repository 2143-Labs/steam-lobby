import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), viteSingleFile()],
  build: { target: "es2022" },
  server: {
    proxy: {
      "/ws": { target: "http://localhost:8080", ws: true },
      "/api": "http://localhost:8080",
      "/auth": "http://localhost:8080",
      "/modes": "http://localhost:8080",
      "/health": "http://localhost:8080",
    },
  },
});
