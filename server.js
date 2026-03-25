const express = require('express');
const jwt = require('jsonwebtoken');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');

const app = express();
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET;

if (!JWT_SECRET) {
  console.error('FATAL: JWT_SECRET environment variable is required');
  process.exit(1);
}

// --- Middleware ---

app.use(express.json());

// CORS for Vercel frontend
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// JWT auth (skip for /health)
function authMiddleware(req, res, next) {
  if (req.path === '/health') return next();

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Missing or invalid Authorization header' });
  }

  try {
    const decoded = jwt.verify(authHeader.slice(7), JWT_SECRET);
    req.user = decoded;
    next();
  } catch (err) {
    return res.status(401).json({ error: 'Invalid or expired token' });
  }
}

app.use(authMiddleware);

// --- WhatsApp Client ---

let currentQr = null;
let clientReady = false;
let clientStatus = 'initializing';

const client = new Client({
  authStrategy: new LocalAuth({
    dataPath: '/data/wwebjs_auth',
  }),
  puppeteer: {
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--single-process',
      '--disable-gpu',
    ],
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || undefined,
  },
});

client.on('qr', (qr) => {
  currentQr = qr;
  clientStatus = 'waiting_for_qr_scan';
  console.log('QR code received, waiting for scan...');
});

client.on('ready', () => {
  currentQr = null;
  clientReady = true;
  clientStatus = 'connected';
  console.log('WhatsApp client is ready!');
});

client.on('authenticated', () => {
  console.log('WhatsApp client authenticated');
  clientStatus = 'authenticated';
});

client.on('auth_failure', (msg) => {
  console.error('WhatsApp auth failure:', msg);
  clientReady = false;
  clientStatus = 'auth_failure';
});

client.on('disconnected', (reason) => {
  console.log('WhatsApp client disconnected:', reason);
  clientReady = false;
  clientStatus = 'disconnected';
  currentQr = null;
  // Attempt to reconnect
  setTimeout(() => {
    console.log('Attempting to reconnect...');
    client.initialize().catch(err => {
      console.error('Reconnection failed:', err.message);
    });
  }, 5000);
});

// Initialize the client
console.log('Initializing WhatsApp client...');
client.initialize().catch(err => {
  console.error('Failed to initialize WhatsApp client:', err.message);
  clientStatus = 'error';
});

// --- Routes ---

// Health check (no auth)
app.get('/health', (req, res) => {
  res.json({ status: 'ok', whatsapp: clientStatus });
});

// GET /api/whatsapp/status
app.get('/api/whatsapp/status', (req, res) => {
  res.json({
    status: clientStatus,
    ready: clientReady,
    hasQr: !!currentQr,
  });
});

// GET /api/whatsapp/qr
app.get('/api/whatsapp/qr', async (req, res) => {
  if (clientReady) {
    return res.json({ qr: null, message: 'Already connected' });
  }
  if (!currentQr) {
    return res.json({ qr: null, message: 'No QR code available yet, client is ' + clientStatus });
  }
  try {
    const qrDataUrl = await qrcode.toDataURL(currentQr);
    res.json({ qr: qrDataUrl });
  } catch (err) {
    res.status(500).json({ error: 'Failed to generate QR code image' });
  }
});

// GET /api/whatsapp/groups
app.get('/api/whatsapp/groups', async (req, res) => {
  if (!clientReady) {
    return res.status(503).json({ error: 'WhatsApp not connected', groups: [] });
  }
  try {
    const chats = await client.getChats();
    const groups = chats
      .filter(chat => chat.isGroup)
      .map(chat => ({
        id: chat.id._serialized,
        name: chat.name,
        participantCount: chat.participants ? chat.participants.length : undefined,
      }));
    res.json({ groups });
  } catch (err) {
    console.error('Error fetching groups:', err.message);
    res.status(500).json({ error: 'Failed to fetch groups', groups: [] });
  }
});

// POST /api/whatsapp/broadcast
app.post('/api/whatsapp/broadcast', async (req, res) => {
  if (!clientReady) {
    return res.status(503).json({ error: 'WhatsApp not connected' });
  }

  const { groupIds, message } = req.body;

  if (!groupIds || !Array.isArray(groupIds) || groupIds.length === 0) {
    return res.status(400).json({ error: 'groupIds must be a non-empty array' });
  }
  if (!message || typeof message !== 'string' || message.trim().length === 0) {
    return res.status(400).json({ error: 'message must be a non-empty string' });
  }

  const results = [];
  for (const groupId of groupIds) {
    try {
      await client.sendMessage(groupId, message);
      results.push({ groupId, success: true });
    } catch (err) {
      console.error(`Failed to send to ${groupId}:`, err.message);
      results.push({ groupId, success: false, error: err.message });
    }
  }

  const successCount = results.filter(r => r.success).length;
  res.json({
    sent: successCount,
    failed: results.length - successCount,
    total: results.length,
    results,
  });
});

// --- Start Server ---

app.listen(PORT, '0.0.0.0', () => {
  console.log(`WhatsApp Broadcast server listening on 0.0.0.0:${PORT}`);
});
