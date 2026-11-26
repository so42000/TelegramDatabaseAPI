// One JSON object per line: info goes to stdout, warn and error to stderr.
type Level = "info" | "warn" | "error";
type Fields = Record<string, unknown>;

function write(level: Level, msg: string, fields: Fields = {}): void {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, msg, ...fields });
  if (level === "info") console.log(line);
  else console.error(line);
}

export const log = {
  info: (msg: string, fields?: Fields): void => write("info", msg, fields),
  warn: (msg: string, fields?: Fields): void => write("warn", msg, fields),
  error: (msg: string, fields?: Fields): void => write("error", msg, fields),
};

export function errorText(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}
