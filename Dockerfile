FROM oven/bun:1.4.5

WORKDIR /app

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

COPY src ./src

ENV DB_PATH=/data/index.sqlite
VOLUME /data
EXPOSE 8080

CMD ["bun", "run", "src/main.ts"]
