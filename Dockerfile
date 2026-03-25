FROM node:20-slim

# Install system deps needed by Puppeteer's bundled Chromium
RUN apt-get update && apt-get install -y \
    ca-certificates \
    fonts-liberation \
    fonts-noto-color-emoji \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libxss1 \
    xdg-utils \
    wget \
    --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Let Puppeteer download its own Chromium (most reliable)
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=false

WORKDIR /app

COPY package.json ./
RUN npm install

COPY server.js ./

# Create data directory for WhatsApp session persistence
RUN mkdir -p /data/wwebjs_auth

EXPOSE 8080

CMD ["node", "server.js"]
