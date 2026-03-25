FROM ghcr.io/puppeteer/puppeteer:23.6.0

USER root

WORKDIR /app

COPY package.json ./
RUN npm install

COPY server.js ./

RUN mkdir -p /data/wwebjs_auth && chown -R pptruser:pptruser /data /app

USER pptruser

# Do NOT set PUPPETEER_EXECUTABLE_PATH — let Puppeteer find its own bundled Chrome
EXPOSE 8080

CMD ["node", "server.js"]
