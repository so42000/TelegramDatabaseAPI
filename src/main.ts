import { createApp, type App } from "./app";
import { loadConfig } from "./config";

let app: App;
try {
  app = await createApp(loadConfig());
} catch (err) {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
}

console.log(`listening on :${app.port}`);

let stopping = false;
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  await app.stop();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
