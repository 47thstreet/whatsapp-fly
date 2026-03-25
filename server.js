const express = require('express');
const crypto = require('crypto');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');

const app = express();
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET;
const KARTIS_EVENTS_URL = process.env.KARTIS_EVENTS_URL || 'https://kartis-astro.vercel.app/api/cms/public-events';
const TBP_URL = process.env.TBP_URL || 'https://tbp-website-astro.vercel.app';

if (!JWT_SECRET) {
  console.error('FATAL: JWT_SECRET environment variable is required');
  process.exit(1);
}

// ─── Event Recommender (standalone, no ClawdAgent) ───────────────────────

let cachedEvents = [];
let cacheTimestamp = 0;
const CACHE_TTL = 5 * 60 * 1000; // 5 min

async function fetchEvents() {
  if (Date.now() - cacheTimestamp < CACHE_TTL && cachedEvents.length > 0) {
    return cachedEvents;
  }
  try {
    const res = await fetch(KARTIS_EVENTS_URL, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    cachedEvents = Array.isArray(data) ? data : [];
    cacheTimestamp = Date.now();
    console.log(`Fetched ${cachedEvents.length} events from Kartis`);
    return cachedEvents;
  } catch (err) {
    console.error('Failed to fetch events:', err.message);
    return cachedEvents; // stale cache
  }
}

function getUpcoming(events) {
  const now = new Date();
  return events
    .filter(e => { try { return new Date(e.date) >= now; } catch { return false; } })
    .sort((a, b) => new Date(a.date) - new Date(b.date));
}

function isHebrew(text) { return /[\u0590-\u05FF]/.test(text); }

function formatEvent(e, heb) {
  const lines = [];
  try {
    const d = new Date(e.date);
    const dateStr = d.toLocaleDateString(heb ? 'he-IL' : 'en-US', { weekday: 'short', day: 'numeric', month: 'short' });
    lines.push(`*${e.name}*`);
    lines.push(`📅 ${dateStr}${e.time ? ' | ' + e.time : ''}`);
  } catch {
    lines.push(`*${e.name}*`);
  }
  if (e.venue) lines.push(`📍 ${e.venue}${e.location ? ', ' + e.location : ''}`);
  if (e.price) lines.push(`💰 ${e.price}`);
  if (e.ticketUrl) lines.push(`🎟️ ${e.ticketUrl}`);
  return lines.join('\n');
}

const DAY_MAP = {
  sunday: 0, monday: 1, tuesday: 2, wednesday: 3, thursday: 4, friday: 5, saturday: 6,
  'ראשון': 0, 'שני': 1, 'שלישי': 2, 'רביעי': 3, 'חמישי': 4, 'שישי': 5, 'שבת': 6,
};

async function getRecommendation(userMessage) {
  const events = await fetchEvents();
  const upcoming = getUpcoming(events);
  const heb = isHebrew(userMessage);
  const lower = userMessage.toLowerCase();

  let matched = [];

  // Today/tonight
  if (lower.includes('tonight') || lower.includes('today') || lower.includes('הערב') || lower.includes('היום')) {
    const today = new Date().toISOString().slice(0, 10);
    matched = upcoming.filter(e => e.date?.startsWith(today));
  }
  // Weekend
  else if (lower.includes('weekend') || lower.includes('סוף שבוע') || lower.includes('סופש')) {
    const now = new Date();
    const day = now.getDay();
    const thu = new Date(now); thu.setDate(now.getDate() + ((4 - day + 7) % 7));
    const sun = new Date(now); sun.setDate(now.getDate() + ((0 - day + 7) % 7) + 7);
    matched = upcoming.filter(e => {
      try { const d = new Date(e.date); return d >= thu && d <= sun; } catch { return false; }
    });
  }
  // Specific day
  else {
    for (const [kw, dayNum] of Object.entries(DAY_MAP)) {
      if (lower.includes(kw)) {
        const now = new Date();
        const ahead = (dayNum - now.getDay() + 7) % 7 || 7;
        const target = new Date(now); target.setDate(now.getDate() + ahead);
        const targetStr = target.toISOString().slice(0, 10);
        matched = upcoming.filter(e => e.date?.startsWith(targetStr));
        break;
      }
    }
  }

  // Keyword search fallback
  if (matched.length === 0) {
    const terms = lower.replace(/[^\w\u0590-\u05FF\s]/g, '').split(/\s+/).filter(w => w.length > 2);
    matched = upcoming.filter(e => {
      const hay = `${e.name} ${e.description || ''} ${e.venue || ''}`.toLowerCase();
      return terms.some(t => hay.includes(t));
    });
  }

  const header = '🎉 *The Best Parties*\n\n';

  if (matched.length > 0) {
    const list = matched.slice(0, 3).map(e => formatEvent(e, heb)).join('\n\n');
    const footer = heb
      ? `\n\n_כל האירועים_ ➡️ ${TBP_URL}/events`
      : `\n\nAll events ➡️ ${TBP_URL}/events`;
    return header + list + footer;
  }

  if (upcoming.length > 0) {
    const intro = heb ? '🔥 הנה מה שבקרוב:\n\n' : "🔥 Here's what's coming up:\n\n";
    const list = upcoming.slice(0, 3).map(e => formatEvent(e, heb)).join('\n\n');
    const footer = heb
      ? `\n\n_כל האירועים_ ➡️ ${TBP_URL}/events`
      : `\n\nAll events ➡️ ${TBP_URL}/events`;
    return header + intro + list + footer;
  }

  return heb
    ? '🎉 *The Best Parties*\n\nאין אירועים קרובים כרגע.\nעקבו ➡️ ' + TBP_URL
    : '🎉 *The Best Parties*\n\nNo upcoming events right now.\nStay tuned ➡️ ' + TBP_URL;
}

async function formatEventsForBroadcast(max = 5) {
  const events = await fetchEvents();
  const upcoming = getUpcoming(events);
  if (upcoming.length === 0) return '🎉 *The Best Parties*\n\nNo upcoming events.';
  const list = upcoming.slice(0, max).map(e => formatEvent(e, true)).join('\n\n');
  return `🎉 *The Best Parties — אירועים קרובים*\n\n${list}\n\n_כל האירועים_ ➡️ ${TBP_URL}/events`;
}

// ─── Party keywords for auto-response ────────────────────────────────────

const PARTY_KEYWORDS = [
  // English
  'ticket', 'tickets', 'how much', 'buy ticket', 'where to buy',
  'party', 'event', 'tonight', 'this weekend', 'thursday', 'friday', 'saturday',
  'club', 'nightlife', 'table', 'vip', 'bottle', 'guestlist', 'guest list', 'rsvp',
  'thebestparties', 'kartis', 'tbp',
  // Hebrew
  'כרטיס', 'כרטיסים', 'טיקט', 'טיקטים', 'כמה עולה', 'כמה זה עולה',
  'מסיבה', 'מסיבות', 'אירוע', 'אירועים', 'הערב', 'סוף שבוע', 'סופש',
  'מועדון', 'שולחן', 'בקבוק', 'רשימת אורחים',
  'איפה קונים', 'איפה אפשר', 'קנות כרטיס', 'לקנות כרטיס',
];

// ─── Express Middleware ──────────────────────────────────────────────────

app.use(express.json());

app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function verifyJWT(token, secret) {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('Bad token format');
  const [header, payload, sig] = parts;
  // Verify signature using raw HMAC (matches the Vercel proxy's makeToken)
  const expected = crypto.createHmac('sha256', secret)
    .update(header + '.' + payload)
    .digest('base64')
    .replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  if (sig !== expected) throw new Error('Signature mismatch');
  // Decode payload
  const decoded = JSON.parse(Buffer.from(payload, 'base64url').toString());
  if (decoded.exp && decoded.exp < Math.floor(Date.now() / 1000)) throw new Error('Expired');
  return decoded;
}

function authMiddleware(req, res, next) {
  if (req.path === '/health') return next();
  const h = req.headers.authorization;
  if (!h || !h.startsWith('Bearer ')) return res.status(401).json({ error: 'Missing auth' });
  try { req.user = verifyJWT(h.slice(7), JWT_SECRET); next(); }
  catch (e) { return res.status(401).json({ error: 'Invalid token', detail: e.message }); }
}

app.use(authMiddleware);

// ─── WhatsApp Client ─────────────────────────────────────────────────────

let currentQr = null;
let clientReady = false;
let clientStatus = 'initializing';

const client = new Client({
  authStrategy: new LocalAuth({ dataPath: '/data/wwebjs_auth' }),
  puppeteer: {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
           '--disable-accelerated-2d-canvas', '--no-first-run', '--no-zygote',
           '--single-process', '--disable-gpu'],
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || '/usr/bin/google-chrome-stable',
  },
});

client.on('qr', (qr) => { currentQr = qr; clientStatus = 'waiting_for_qr_scan'; console.log('QR ready'); });
client.on('ready', () => { currentQr = null; clientReady = true; clientStatus = 'connected'; console.log('WhatsApp connected!'); });
client.on('authenticated', () => { clientStatus = 'authenticated'; console.log('WhatsApp authenticated'); });
client.on('auth_failure', (msg) => { clientReady = false; clientStatus = 'auth_failure'; console.error('Auth fail:', msg); });
client.on('disconnected', (reason) => {
  clientReady = false; clientStatus = 'disconnected'; currentQr = null;
  console.log('Disconnected:', reason);
  setTimeout(() => client.initialize().catch(e => console.error('Reconnect fail:', e.message)), 5000);
});

// ─── Standalone Group Chat Bot (no ClawdAgent) ──────────────────────────

client.on('message', async (msg) => {
  if (msg.fromMe) return;

  const chat = await msg.getChat().catch(() => null);
  if (!chat) return;

  // In group chats: respond to party keywords
  if (chat.isGroup) {
    const lower = msg.body.toLowerCase();
    const isPartyQuery = PARTY_KEYWORDS.some(kw => lower.includes(kw.toLowerCase()));

    if (isPartyQuery) {
      console.log(`[BOT] Party keyword in group "${chat.name}" from ${msg.author || msg.from}`);
      try {
        const recommendation = await getRecommendation(msg.body);
        await msg.reply(recommendation);
        console.log(`[BOT] Replied with event recommendation`);
      } catch (err) {
        console.error('[BOT] Failed to reply:', err.message);
        // Fallback static reply
        await msg.reply(
          `🎉 *The Best Parties*\n\nCheck out our events ➡️ ${TBP_URL}/events`
        ).catch(() => {});
      }
    }
    return; // don't process other group messages
  }

  // In DMs: always respond with event info
  console.log(`[BOT] DM from ${msg.from}: ${msg.body.slice(0, 50)}...`);
  try {
    const recommendation = await getRecommendation(msg.body);
    await msg.reply(recommendation);
  } catch (err) {
    console.error('[BOT] DM reply failed:', err.message);
    await msg.reply(
      `🎉 *The Best Parties*\n\nCheck out our events ➡️ ${TBP_URL}/events`
    ).catch(() => {});
  }
});

async function initWhatsApp(attempt = 1) {
  console.log(`Initializing WhatsApp client (attempt ${attempt})...`);
  try {
    await client.initialize();
  } catch (err) {
    console.error(`Init failed (attempt ${attempt}):`, err.message, err.stack);
    clientStatus = 'error';
    if (attempt < 3) {
      console.log(`Retrying in 10s...`);
      setTimeout(() => initWhatsApp(attempt + 1), 10000);
    } else {
      console.error('All init attempts failed. Check Chromium installation.');
    }
  }
}
initWhatsApp();

// ─── API Routes ──────────────────────────────────────────────────────────

app.get('/health', (req, res) => res.json({ status: 'ok', whatsapp: clientStatus }));

app.get('/api/whatsapp/status', (req, res) => {
  res.json({ status: clientStatus, ready: clientReady, hasQr: !!currentQr });
});

app.get('/api/whatsapp/qr', async (req, res) => {
  if (clientReady) return res.json({ qr: null, message: 'Already connected' });
  if (!currentQr) return res.json({ qr: null, message: 'No QR yet, status: ' + clientStatus });
  try {
    const qrDataUrl = await qrcode.toDataURL(currentQr);
    res.json({ qr: qrDataUrl, qrDataUrl }); // both keys for compat
  } catch { res.status(500).json({ error: 'QR generation failed' }); }
});

app.get('/api/whatsapp/groups', async (req, res) => {
  if (!clientReady) return res.status(503).json({ error: 'Not connected', groups: [] });
  try {
    const chats = await client.getChats();
    const groups = chats.filter(c => c.isGroup).map(c => ({
      id: c.id._serialized, name: c.name,
      participantCount: c.participants ? c.participants.length : undefined,
    }));
    res.json({ groups });
  } catch (err) { res.status(500).json({ error: err.message, groups: [] }); }
});

// Manual broadcast (from UI)
app.post('/api/whatsapp/broadcast', async (req, res) => {
  if (!clientReady) return res.status(503).json({ error: 'Not connected' });

  const chatIds = req.body.chatIds || req.body.groupIds;
  const { message } = req.body;

  if (!chatIds || !Array.isArray(chatIds) || chatIds.length === 0)
    return res.status(400).json({ error: 'chatIds/groupIds required' });
  if (!message || typeof message !== 'string')
    return res.status(400).json({ error: 'message required' });
  if (chatIds.length > 50)
    return res.status(400).json({ error: 'Max 50 per broadcast' });

  let sent = 0, failed = 0;
  for (const id of chatIds) {
    try {
      await client.sendMessage(id, message);
      sent++;
      await new Promise(r => setTimeout(r, 1500)); // rate limit
    } catch (err) {
      failed++;
      console.error(`Broadcast fail ${id}:`, err.message);
    }
  }
  res.json({ sent, failed, total: chatIds.length });
});

// Auto-broadcast events (fetches from Kartis, sends to groups)
app.post('/api/whatsapp/broadcast-events', async (req, res) => {
  if (!clientReady) return res.status(503).json({ error: 'Not connected' });

  const chatIds = req.body.chatIds || req.body.groupIds;
  const maxEvents = req.body.maxEvents || 5;

  if (!chatIds || !Array.isArray(chatIds) || chatIds.length === 0)
    return res.status(400).json({ error: 'chatIds/groupIds required' });

  try {
    const message = await formatEventsForBroadcast(maxEvents);
    let sent = 0, failed = 0;
    for (const id of chatIds) {
      try {
        await client.sendMessage(id, message);
        sent++;
        await new Promise(r => setTimeout(r, 1500));
      } catch { failed++; }
    }
    res.json({ sent, failed, total: chatIds.length, message });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Start ───────────────────────────────────────────────────────────────

app.listen(PORT, '0.0.0.0', () => {
  console.log(`WhatsApp Standalone server on 0.0.0.0:${PORT}`);
  console.log(`Events API: ${KARTIS_EVENTS_URL}`);
  console.log(`TBP URL: ${TBP_URL}`);
});
