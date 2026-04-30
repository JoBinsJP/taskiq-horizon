import { fileURLToPath, URL } from "node:url";

import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [vue()],
  publicDir: false,
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src/js", import.meta.url)),
    },
  },
  build: {
    assetsDir: "",
    emptyOutDir: true,
    outDir: "../taskiq_horizon/dashboard/dist",
    sourcemap: false,
    target: "es2020",
    rollupOptions: {
      input: [
        fileURLToPath(new URL("./src/js/app.js", import.meta.url)),
      ],
      output: {
        entryFileNames: "[name].js",
        chunkFileNames: "[name].js",
        assetFileNames: "[name].[ext]",
      },
    },
  },
});
