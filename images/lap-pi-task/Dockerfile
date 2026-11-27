FROM node:22-bookworm-slim AS node
FROM ghcr.io/stigenai/ax/ax-task-runner:v0.3.1-stigenai.1@sha256:fdc4c5d665e3668d8d08e0ccfab620864654f2c1a9c21570f46ca97462cdac98
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm
WORKDIR /opt/lap-pi-task
COPY images/lap-pi-task/package*.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY images/lap-pi-task/*.mjs ./
RUN npm test && npm run check
WORKDIR /workspace
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/ax-task-runner"]
