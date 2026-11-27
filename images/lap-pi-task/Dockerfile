FROM node:22-bookworm-slim AS node
FROM ghcr.io/google/ax/ax-runner:v0.4.0-rc.2@sha256:0937ebe5ad2d8db6bb25e88ded44d2d65de1b1bd8f0a9a97c6bb3787407d9947
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm
WORKDIR /opt/lap-pi-task
COPY images/lap-pi-task/package*.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY images/lap-pi-task/*.mjs ./
RUN npm test && npm run check
# ax-runner sends SIGTERM to the task command, then waits this long before
# SIGKILL. ax-runner runs in the actor, so the image sets it.
ENV AX_STOP_GRACE_PERIOD=90s
WORKDIR /workspace
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/ax-runner"]
