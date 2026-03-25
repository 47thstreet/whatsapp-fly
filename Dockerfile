FROM ghcr.io/puppeteer/puppeteer:23.6.0

# Run as root to access /data volume
USER root

WORKDIR /app

# Copy and install
COPY package.json ./
RUN npm install

COPY server.js ./

# Create data directory for WhatsApp session persistence
RUN mkdir -p /data/wwebjs_auth && chown -R pptruser:pptruser /data

# Switch back to puppeteer user
USER pptruser

EXPOSE 8080

CMD ["node", "server.js"]
