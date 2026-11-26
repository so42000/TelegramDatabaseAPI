// Process-wide counters and histograms for /metrics. Per-app values (pool, batcher, db, snapshot) are
// passed to renderMetrics as gauges, because tests run several apps in one process.

export type Labels = Record<string, string>;

export interface Gauge {
  name: string;
  help: string;
  value: number;
}

function escapeLabelValue(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\n/g, "\\n");
}

function formatLabels(names: readonly string[], labels: Labels, extra?: [string, string]): string {
  const parts = names.map((name) => `${name}="${escapeLabelValue(labels[name] ?? "")}"`);
  if (extra) parts.push(`${extra[0]}="${escapeLabelValue(extra[1])}"`);
  return parts.length === 0 ? "" : `{${parts.join(",")}}`;
}

function seriesKey(names: readonly string[], labels: Labels): string {
  return names.map((name) => labels[name] ?? "").join("\u0000");
}

export class Counter {
  private readonly series = new Map<string, { labels: Labels; value: number }>();

  constructor(
    private readonly name: string,
    private readonly help: string,
    private readonly labelNames: readonly string[],
  ) {}

  inc(labels: Labels, by = 1): void {
    const key = seriesKey(this.labelNames, labels);
    const existing = this.series.get(key);
    if (existing) existing.value += by;
    else this.series.set(key, { labels, value: by });
  }

  render(): string {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} counter`];
    for (const { labels, value } of this.series.values()) {
      lines.push(`${this.name}${formatLabels(this.labelNames, labels)} ${value}`);
    }
    return lines.join("\n");
  }
}

interface HistogramSeries {
  labels: Labels;
  // Per-bucket (not cumulative) counts; values above the last bound are only in `count`.
  counts: number[];
  sum: number;
  count: number;
}

export class Histogram {
  private readonly series = new Map<string, HistogramSeries>();

  constructor(
    private readonly name: string,
    private readonly help: string,
    private readonly labelNames: readonly string[],
    private readonly buckets: readonly number[],
  ) {}

  observe(labels: Labels, value: number): void {
    const key = seriesKey(this.labelNames, labels);
    let entry = this.series.get(key);
    if (!entry) {
      entry = { labels, counts: this.buckets.map(() => 0), sum: 0, count: 0 };
      this.series.set(key, entry);
    }
    const index = this.buckets.findIndex((bound) => value <= bound);
    if (index >= 0) entry.counts[index] = (entry.counts[index] ?? 0) + 1;
    entry.sum += value;
    entry.count++;
  }

  render(): string {
    const lines = [`# HELP ${this.name} ${this.help}`, `# TYPE ${this.name} histogram`];
    for (const { labels, counts, sum, count } of this.series.values()) {
      let cumulative = 0;
      this.buckets.forEach((bound, i) => {
        cumulative += counts[i] ?? 0;
        const le = formatLabels(this.labelNames, labels, ["le", String(bound)]);
        lines.push(`${this.name}_bucket${le} ${cumulative}`);
      });
      lines.push(`${this.name}_bucket${formatLabels(this.labelNames, labels, ["le", "+Inf"])} ${count}`);
      lines.push(`${this.name}_sum${formatLabels(this.labelNames, labels)} ${sum}`);
      lines.push(`${this.name}_count${formatLabels(this.labelNames, labels)} ${count}`);
    }
    return lines.join("\n");
  }
}

export const httpRequests = new Counter(
  "tgstore_http_requests_total",
  "HTTP responses by route, method and status.",
  ["route", "method", "status"],
);
// Measured until the response headers are ready, so streamed bodies are not included.
export const httpDuration = new Histogram(
  "tgstore_http_request_duration_seconds",
  "Time to response headers by route.",
  ["route"],
  [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120],
);
// One increment per attempt, including retried attempts. Downloads use method "download".
export const telegramRequests = new Counter(
  "tgstore_telegram_requests_total",
  "Telegram Bot API and file download attempts by method and outcome.",
  ["method", "outcome"],
);
export const snapshotFlushes = new Counter(
  "tgstore_snapshot_flushes_total",
  "Snapshot flushes that wrote a new manifest or failed.",
  ["outcome"],
);
export const botsDisabled = new Counter("tgstore_bot_disabled_total", "Bots disabled after losing access.", []);

export function renderMetrics(gauges: Gauge[]): string {
  const blocks = [
    httpRequests.render(),
    httpDuration.render(),
    telegramRequests.render(),
    snapshotFlushes.render(),
    botsDisabled.render(),
    ...gauges.map((g) => `# HELP ${g.name} ${g.help}\n# TYPE ${g.name} gauge\n${g.name} ${g.value}`),
  ];
  return `${blocks.join("\n")}\n`;
}
