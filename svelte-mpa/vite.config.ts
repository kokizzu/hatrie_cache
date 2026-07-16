import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

const root = fileURLToPath(new URL('.', import.meta.url));

export default defineConfig({
  plugins: [svelte()],
  build: {
    rollupOptions: {
      input: {
        dashboard: resolve(root, 'index.html'),
        keys: resolve(root, 'keys.html'),
        commands: resolve(root, 'commands.html'),
        admin: resolve(root, 'admin.html')
      }
    }
  },
  server: {
    port: 5173,
    strictPort: false
  }
});
