FROM oven/bun:1.4.5

WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

COPY src ./src

ENV DB_PATH=/data/index.sqlite
RUN mkdir -p /data && chown bun:bun /data
USER bun
VOLUME /data
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=3 \
  CMD ["bun", "-e", "const r = await fetch('http://127.0.0.1:' + (process.env.PORT || 8080) + '/readyz').catch(() => null); process.exit(r?.ok ? 0 : 1)"]

CMD ["bun", "run", "src/main.ts"]
