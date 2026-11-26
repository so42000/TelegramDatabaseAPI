import { errorText, log } from "./log";
import { createApp, type App } from "./app";
import { loadConfig } from "./config";

let app: App;
try {
  app = await createApp(loadConfig());
} catch (err) {
  log.error(errorText(err));
  process.exit(1);
}

log.info(`listening on :${app.port}`, { port: app.port });

let stopping = false;
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  await app.stop();
  process.exit(0);
}

process.on("SIGINT", () => void shutdown());
process.on("SIGTERM", () => void shutdown());
