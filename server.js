const express = require('express');
const crypto = require('crypto');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;
const JWT_SECRET = process.env.JWT_SECRET;
const KARTIS_EVENTS_URL = process.env.KARTIS_EVENTS_URL || 'https://kartis-astro.vercel.app/api/cms/public-events';
const TBP_URL = process.env.TBP_URL || 'https://tbp-website-astro.vercel.app';

if (!JWT_SECRET) {
  console.error('FATAL: JWT_SECRET environment variable is required');
  process.exit(1);
}

// ─── Data Directory ─────────────────────────────────────────────────────
const DATA_DIR = fs.existsSync('/data') ? '/data' : '.';
const AUTH_DIR = path.join(DATA_DIR, 'wwebjs_auth');
console.log('Data dir:', DATA_DIR);
console.log('Auth dir:', AUTH_DIR);

function ensureDir(dir) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
ensureDir(AUTH_DIR);

// ─── JSON File Persistence Helpers ──────────────────────────────────────
function loadJSON(filePath, fallback = []) {
  try {
    if (fs.existsSync(filePath)) return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (e) { console.error(`Failed to load ${filePath}:`, e.message); }
  return fallback;
}

function saveJSON(filePath, data) {
  try { fs.writeFileSync(filePath, JSON.stringify(data, null, 2)); }
  catch (e) { console.error(`Failed to save ${filePath}:`, e.message); }
}

const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
const SCHEDULES_FILE = path.join(DATA_DIR, 'schedules.json');
const TEMPLATES_FILE = path.join(DATA_DIR, 'templates.json');
const HISTORY_FILE = path.join(DATA_DIR, 'history.json');
const GROUP_TAGS_FILE = path.join(DATA_DIR, 'group-tags.json');
const AUTO_RULES_FILE = path.join(DATA_DIR, 'auto-rules.json');

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

// ─── Party keywords for auto-response (default rule) ─────────────────────

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
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

function verifyJWT(token, secret) {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('Bad token format');
  const [header, payload, sig] = parts;
  const expected = crypto.createHmac('sha256', secret)
    .update(header + '.' + payload)
    .digest('base64')
    .replace(/=/g, '').replace(/\+/g, '-').replace(/\//g, '_');
  if (sig !== expected) throw new Error('Signature mismatch');
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

// ─── Chromium Args (shared across all clients) ──────────────────────────

const PUPPETEER_ARGS = [
  '--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage',
  '--disable-accelerated-2d-canvas', '--no-first-run', '--no-zygote',
  '--single-process', '--disable-gpu', '--disable-extensions',
  '--disable-background-networking', '--disable-default-apps',
  '--disable-sync', '--disable-translate', '--metrics-recording-only',
  '--no-default-browser-check', '--mute-audio',
  '--disable-component-update', '--disable-breakpad',
  '--disable-features=TranslateUI,BlinkGenPropertyTrees',
  '--js-flags=--max-old-space-size=128 --gc-interval=100',
];

// ─── Multi-Account WhatsApp System ──────────────────────────────────────

// accounts map: accountId -> { id, name, client, qr, ready, status }
const accounts = new Map();

function cleanStaleLocks(dir) {
  try {
    if (!fs.existsSync(dir)) return;
    const walk = (d) => {
      try {
        for (const entry of fs.readdirSync(d, { withFileTypes: true })) {
          const full = path.join(d, entry.name);
          if (entry.isDirectory()) walk(full);
          else if (entry.name.startsWith('Singleton')) {
            try { fs.unlinkSync(full); console.log('Removed stale lock:', full); } catch {}
          }
        }
      } catch {}
    };
    walk(dir);
  } catch (e) { console.log('Lock cleanup skipped:', e.message); }
}

function createWhatsAppClient(accountId) {
  const dataPath = path.join(AUTH_DIR, accountId);
  ensureDir(dataPath);
  cleanStaleLocks(dataPath);

  const client = new Client({
    authStrategy: new LocalAuth({ clientId: accountId, dataPath: AUTH_DIR }),
    puppeteer: {
      headless: true,
      args: PUPPETEER_ARGS,
    },
    webVersionCache: { type: 'none' },
  });

  return client;
}

function setupClientEvents(accountId, client) {
  const acc = accounts.get(accountId);
  if (!acc) return;

  client.on('qr', (qr) => {
    acc.qr = qr;
    acc.status = 'waiting_for_qr_scan';
    console.log(`[${accountId}] QR ready`);
  });

  client.on('ready', () => {
    acc.qr = null;
    acc.ready = true;
    acc.status = 'ready';
    console.log(`[${accountId}] WhatsApp connected!`);
  });

  client.on('authenticated', () => {
    acc.status = 'authenticated';
    console.log(`[${accountId}] WhatsApp authenticated`);
  });

  client.on('auth_failure', (msg) => {
    acc.ready = false;
    acc.status = 'auth_failure';
    console.error(`[${accountId}] Auth fail:`, msg);
  });

  client.on('disconnected', (reason) => {
    acc.ready = false;
    acc.status = 'disconnected';
    acc.qr = null;
    console.log(`[${accountId}] Disconnected:`, reason);
    setTimeout(() => {
      client.initialize().catch(e => console.error(`[${accountId}] Reconnect fail:`, e.message));
    }, 5000);
  });

  // ─── Message handler: auto-response with custom rules + default party bot ─
  client.on('message', async (msg) => {
    if (msg.fromMe) return;

    const chat = await msg.getChat().catch(() => null);
    if (!chat) return;

    const lower = msg.body.toLowerCase();
    const autoRules = loadJSON(AUTO_RULES_FILE, []);

    // Check custom auto-response rules first
    for (const rule of autoRules) {
      if (!rule.enabled) continue;
      // Rule can be scoped to account or global
      if (rule.account && rule.account !== accountId) continue;
      const matched = rule.keywords.some(kw => lower.includes(kw.toLowerCase()));
      if (matched) {
        console.log(`[${accountId}] Auto-rule "${rule.id}" triggered in "${chat.name || msg.from}"`);
        try {
          // Simple template variable replacement
          let response = rule.response;
          // No variables to replace in auto-rules (they're static responses)
          await msg.reply(response);
          console.log(`[${accountId}] Auto-rule replied`);
        } catch (err) {
          console.error(`[${accountId}] Auto-rule reply failed:`, err.message);
        }
        return; // first matching rule wins
      }
    }

    // Default party keyword handler (existing behavior)
    if (chat.isGroup) {
      const isPartyQuery = PARTY_KEYWORDS.some(kw => lower.includes(kw.toLowerCase()));
      if (isPartyQuery) {
        console.log(`[${accountId}] Party keyword in group "${chat.name}" from ${msg.author || msg.from}`);
        try {
          const recommendation = await getRecommendation(msg.body);
          await msg.reply(recommendation);
          console.log(`[${accountId}] Replied with event recommendation`);
        } catch (err) {
          console.error(`[${accountId}] Failed to reply:`, err.message);
          await msg.reply(
            `🎉 *The Best Parties*\n\nCheck out our events ➡️ ${TBP_URL}/events`
          ).catch(() => {});
        }
      }
      return;
    }

    // In DMs: always respond with event info
    console.log(`[${accountId}] DM from ${msg.from}: ${msg.body.slice(0, 50)}...`);
    try {
      const recommendation = await getRecommendation(msg.body);
      await msg.reply(recommendation);
    } catch (err) {
      console.error(`[${accountId}] DM reply failed:`, err.message);
      await msg.reply(
        `🎉 *The Best Parties*\n\nCheck out our events ➡️ ${TBP_URL}/events`
      ).catch(() => {});
    }
  });
}

async function initAccount(accountId, attempt = 1) {
  const acc = accounts.get(accountId);
  if (!acc) return;
  console.log(`[${accountId}] Initializing WhatsApp client (attempt ${attempt})...`);
  try {
    await acc.client.initialize();
  } catch (err) {
    console.error(`[${accountId}] Init failed (attempt ${attempt}):`, err.message);
    acc.status = 'error';
    if (attempt < 3) {
      console.log(`[${accountId}] Retrying in 10s...`);
      setTimeout(() => initAccount(accountId, attempt + 1), 10000);
    } else {
      console.error(`[${accountId}] All init attempts failed.`);
    }
  }
}

function registerAccount(id, name) {
  if (accounts.has(id)) return accounts.get(id);

  const client = createWhatsAppClient(id);
  const acc = { id, name, client, qr: null, ready: false, status: 'initializing' };
  accounts.set(id, acc);
  setupClientEvents(id, client);
  return acc;
}

function getAccount(req) {
  const id = req.query.account || req.body?.account || 'default';
  return { id, acc: accounts.get(id) };
}

// Load saved accounts and initialize them
function loadAndInitAccounts() {
  const saved = loadJSON(ACCOUNTS_FILE, []);
  // Always ensure "default" account exists
  if (!saved.find(a => a.id === 'default')) {
    saved.unshift({ id: 'default', name: 'Default' });
    saveJSON(ACCOUNTS_FILE, saved);
  }
  // Clean stale locks in the global auth dir
  cleanStaleLocks(AUTH_DIR);

  for (const { id, name } of saved) {
    registerAccount(id, name);
    initAccount(id);
  }
}

// ─── Broadcast History Helper ───────────────────────────────────────────

function logBroadcast(entry) {
  const history = loadJSON(HISTORY_FILE, []);
  history.unshift(entry); // newest first
  // Keep last 500
  if (history.length > 500) history.length = 500;
  saveJSON(HISTORY_FILE, history);
}

// ─── Template Variable Replacement ──────────────────────────────────────

function applyTemplate(templateMessage, variables = {}) {
  return templateMessage.replace(/\{\{(\w+)\}\}/g, (match, key) => {
    return variables[key] !== undefined ? variables[key] : match;
  });
}

// ─── Scheduled Broadcasts ───────────────────────────────────────────────

function checkScheduledBroadcasts() {
  const schedules = loadJSON(SCHEDULES_FILE, []);
  const now = Date.now();
  let changed = false;

  for (const sched of schedules) {
    if (sched.status !== 'pending') continue;
    const sendAt = new Date(sched.sendAt).getTime();
    if (sendAt <= now) {
      // Time to send
      sched.status = 'sending';
      changed = true;
      executeBroadcast(sched.account || 'default', sched.chatIds, sched.message, sched.id, sched.name)
        .then(result => {
          const scheds = loadJSON(SCHEDULES_FILE, []);
          const s = scheds.find(x => x.id === sched.id);
          if (s) {
            s.status = 'completed';
            s.result = result;
            saveJSON(SCHEDULES_FILE, scheds);
          }
        })
        .catch(err => {
          const scheds = loadJSON(SCHEDULES_FILE, []);
          const s = scheds.find(x => x.id === sched.id);
          if (s) {
            s.status = 'failed';
            s.error = err.message;
            saveJSON(SCHEDULES_FILE, scheds);
          }
        });
    }
  }

  if (changed) saveJSON(SCHEDULES_FILE, schedules);
}

async function executeBroadcast(accountId, chatIds, message, broadcastId, broadcastName) {
  const acc = accounts.get(accountId);
  if (!acc || !acc.ready) throw new Error(`Account ${accountId} not ready`);

  let sent = 0, failed = 0;
  const failures = [];
  for (const id of chatIds) {
    try {
      await acc.client.sendMessage(id, message);
      sent++;
      await new Promise(r => setTimeout(r, 1500)); // rate limit
    } catch (err) {
      failed++;
      failures.push({ chatId: id, error: err.message });
      console.error(`[${accountId}] Broadcast fail ${id}:`, err.message);
    }
  }

  const result = { sent, failed, total: chatIds.length, failures };

  // Log to history
  logBroadcast({
    id: broadcastId || crypto.randomUUID(),
    name: broadcastName || null,
    timestamp: new Date().toISOString(),
    account: accountId,
    chatIds,
    messagePreview: message.slice(0, 200),
    sent,
    failed,
    total: chatIds.length,
    failures,
  });

  return result;
}

// Check every 30 seconds
setInterval(checkScheduledBroadcasts, 30000);

// ─── API Routes ──────────────────────────────────────────────────────────

app.get('/health', (req, res) => {
  const defaultAcc = accounts.get('default');
  res.json({ status: 'ok', whatsapp: defaultAcc ? defaultAcc.status : 'no_accounts' });
});

// ─── Account Management ─────────────────────────────────────────────────

app.post('/api/accounts', (req, res) => {
  const { id, name } = req.body;
  if (!id || typeof id !== 'string') return res.status(400).json({ error: 'id required (slug)' });
  if (!/^[a-z0-9-]+$/.test(id)) return res.status(400).json({ error: 'id must be lowercase alphanumeric with hyphens' });
  if (accounts.has(id)) return res.status(409).json({ error: 'Account already exists' });

  registerAccount(id, name || id);
  initAccount(id);

  // Persist
  const saved = loadJSON(ACCOUNTS_FILE, []);
  saved.push({ id, name: name || id });
  saveJSON(ACCOUNTS_FILE, saved);

  res.json({ ok: true, id, name: name || id });
});

app.get('/api/accounts', (req, res) => {
  const list = [];
  for (const [id, acc] of accounts) {
    list.push({ id, name: acc.name, status: acc.status, ready: acc.ready, hasQr: !!acc.qr });
  }
  res.json({ accounts: list });
});

app.delete('/api/accounts/:id', async (req, res) => {
  const id = req.params.id;
  if (id === 'default') return res.status(400).json({ error: 'Cannot delete default account' });
  const acc = accounts.get(id);
  if (!acc) return res.status(404).json({ error: 'Account not found' });

  try {
    await acc.client.destroy().catch(() => {});
  } catch {}

  accounts.delete(id);

  // Remove session dir
  const sessionDir = path.join(AUTH_DIR, `session-${id}`);
  try { fs.rmSync(sessionDir, { recursive: true, force: true }); } catch {}

  // Remove from persistence
  const saved = loadJSON(ACCOUNTS_FILE, []).filter(a => a.id !== id);
  saveJSON(ACCOUNTS_FILE, saved);

  res.json({ ok: true, deleted: id });
});

// ─── WhatsApp Status / QR / Groups ──────────────────────────────────────

app.get('/api/whatsapp/status', (req, res) => {
  const { id, acc } = getAccount(req);
  if (!acc) return res.status(404).json({ error: `Account "${id}" not found`, status: 'not_found', ready: false });
  res.json({ status: acc.status, ready: acc.ready, hasQr: !!acc.qr, account: id });
});

app.get('/api/whatsapp/qr', async (req, res) => {
  const { id, acc } = getAccount(req);
  if (!acc) return res.status(404).json({ error: `Account "${id}" not found` });
  if (acc.ready) return res.json({ qr: null, status: 'ready', message: 'Already connected' });
  if (acc.status === 'authenticated') return res.json({ qr: null, status: 'authenticated', message: 'Authenticated, loading...' });
  if (!acc.qr) return res.json({ qr: null, status: acc.status, message: 'No QR yet, status: ' + acc.status });
  try {
    const qrDataUrl = await qrcode.toDataURL(acc.qr);
    res.json({ qr: qrDataUrl, qrDataUrl, status: acc.status });
  } catch { res.status(500).json({ error: 'QR generation failed' }); }
});

app.get('/api/whatsapp/groups', async (req, res) => {
  const { id, acc } = getAccount(req);
  if (!acc) return res.status(404).json({ error: `Account "${id}" not found`, groups: [] });
  if (!acc.ready) return res.status(503).json({ error: 'Not connected', groups: [] });

  try {
    const chats = await acc.client.getChats();
    let groups = chats.filter(c => c.isGroup).map(c => ({
      id: c.id._serialized, name: c.name,
      participantCount: c.participants ? c.participants.length : undefined,
    }));

    // Attach tags
    const allTags = loadJSON(GROUP_TAGS_FILE, {});
    groups = groups.map(g => ({ ...g, tags: allTags[g.id] || [] }));

    // Filter by tags if requested
    const filterTags = req.query.tags;
    if (filterTags) {
      const wanted = filterTags.split(',').map(t => t.trim().toLowerCase());
      groups = groups.filter(g => {
        const gTags = (g.tags || []).map(t => t.toLowerCase());
        return wanted.some(w => gTags.includes(w));
      });
    }

    res.json({ groups, account: id });
  } catch (err) { res.status(500).json({ error: err.message, groups: [] }); }
});

// ─── Broadcast ──────────────────────────────────────────────────────────

app.post('/api/whatsapp/broadcast', async (req, res) => {
  const accountId = req.body.account || 'default';
  const acc = accounts.get(accountId);
  if (!acc) return res.status(404).json({ error: `Account "${accountId}" not found` });
  if (!acc.ready) return res.status(503).json({ error: 'Not connected' });

  const chatIds = req.body.chatIds || req.body.groupIds;
  let { message, templateId, variables } = req.body;

  // If using a template, resolve it
  if (templateId) {
    const templates = loadJSON(TEMPLATES_FILE, []);
    const tpl = templates.find(t => t.id === templateId);
    if (!tpl) return res.status(404).json({ error: `Template "${templateId}" not found` });
    message = applyTemplate(tpl.message, variables || {});
  }

  if (!chatIds || !Array.isArray(chatIds) || chatIds.length === 0)
    return res.status(400).json({ error: 'chatIds/groupIds required' });
  if (!message || typeof message !== 'string')
    return res.status(400).json({ error: 'message required (or use templateId)' });
  if (chatIds.length > 50)
    return res.status(400).json({ error: 'Max 50 per broadcast' });

  const broadcastId = crypto.randomUUID();
  const result = await executeBroadcast(accountId, chatIds, message, broadcastId, req.body.name);
  res.json({ ...result, broadcastId });
});

// Auto-broadcast events (fetches from Kartis, sends to groups)
app.post('/api/whatsapp/broadcast-events', async (req, res) => {
  const accountId = req.body.account || 'default';
  const acc = accounts.get(accountId);
  if (!acc) return res.status(404).json({ error: `Account "${accountId}" not found` });
  if (!acc.ready) return res.status(503).json({ error: 'Not connected' });

  const chatIds = req.body.chatIds || req.body.groupIds;
  const maxEvents = req.body.maxEvents || 5;

  if (!chatIds || !Array.isArray(chatIds) || chatIds.length === 0)
    return res.status(400).json({ error: 'chatIds/groupIds required' });

  try {
    const message = await formatEventsForBroadcast(maxEvents);
    const broadcastId = crypto.randomUUID();
    const result = await executeBroadcast(accountId, chatIds, message, broadcastId, 'Event Broadcast');
    res.json({ ...result, message, broadcastId });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── Scheduled Broadcasts ───────────────────────────────────────────────

app.post('/api/whatsapp/schedule', (req, res) => {
  const { chatIds, message, sendAt, name, account, templateId, variables } = req.body;
  const accountId = account || 'default';

  if (!accounts.has(accountId)) return res.status(404).json({ error: `Account "${accountId}" not found` });

  let finalMessage = message;
  if (templateId) {
    const templates = loadJSON(TEMPLATES_FILE, []);
    const tpl = templates.find(t => t.id === templateId);
    if (!tpl) return res.status(404).json({ error: `Template "${templateId}" not found` });
    finalMessage = applyTemplate(tpl.message, variables || {});
  }

  if (!chatIds || !Array.isArray(chatIds) || chatIds.length === 0)
    return res.status(400).json({ error: 'chatIds required' });
  if (!finalMessage || typeof finalMessage !== 'string')
    return res.status(400).json({ error: 'message required (or use templateId)' });
  if (!sendAt) return res.status(400).json({ error: 'sendAt required (ISO 8601)' });

  const sendAtDate = new Date(sendAt);
  if (isNaN(sendAtDate.getTime())) return res.status(400).json({ error: 'Invalid sendAt date' });
  if (sendAtDate.getTime() <= Date.now()) return res.status(400).json({ error: 'sendAt must be in the future' });

  const id = crypto.randomUUID();
  const schedule = {
    id,
    name: name || null,
    account: accountId,
    chatIds,
    message: finalMessage,
    sendAt: sendAtDate.toISOString(),
    status: 'pending',
    createdAt: new Date().toISOString(),
  };

  const schedules = loadJSON(SCHEDULES_FILE, []);
  schedules.push(schedule);
  saveJSON(SCHEDULES_FILE, schedules);

  res.json({ ok: true, schedule });
});

app.get('/api/whatsapp/schedules', (req, res) => {
  const schedules = loadJSON(SCHEDULES_FILE, []);
  const pending = schedules.filter(s => s.status === 'pending');
  res.json({ schedules: pending });
});

app.delete('/api/whatsapp/schedules/:id', (req, res) => {
  const schedules = loadJSON(SCHEDULES_FILE, []);
  const idx = schedules.findIndex(s => s.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Schedule not found' });
  if (schedules[idx].status !== 'pending') return res.status(400).json({ error: 'Can only cancel pending schedules' });
  schedules[idx].status = 'cancelled';
  saveJSON(SCHEDULES_FILE, schedules);
  res.json({ ok: true, cancelled: req.params.id });
});

// ─── Message Templates ──────────────────────────────────────────────────

app.post('/api/whatsapp/templates', (req, res) => {
  const { id, name, message } = req.body;
  if (!id || typeof id !== 'string') return res.status(400).json({ error: 'id required' });
  if (!message || typeof message !== 'string') return res.status(400).json({ error: 'message required' });

  const templates = loadJSON(TEMPLATES_FILE, []);
  const existing = templates.findIndex(t => t.id === id);
  const tpl = { id, name: name || id, message, updatedAt: new Date().toISOString() };

  if (existing >= 0) {
    templates[existing] = tpl;
  } else {
    tpl.createdAt = new Date().toISOString();
    templates.push(tpl);
  }

  saveJSON(TEMPLATES_FILE, templates);
  res.json({ ok: true, template: tpl });
});

app.get('/api/whatsapp/templates', (req, res) => {
  const templates = loadJSON(TEMPLATES_FILE, []);
  res.json({ templates });
});

app.delete('/api/whatsapp/templates/:id', (req, res) => {
  const templates = loadJSON(TEMPLATES_FILE, []);
  const filtered = templates.filter(t => t.id !== req.params.id);
  if (filtered.length === templates.length) return res.status(404).json({ error: 'Template not found' });
  saveJSON(TEMPLATES_FILE, filtered);
  res.json({ ok: true, deleted: req.params.id });
});

// ─── Broadcast History ──────────────────────────────────────────────────

app.get('/api/whatsapp/history', (req, res) => {
  const history = loadJSON(HISTORY_FILE, []);
  const limit = Math.min(parseInt(req.query.limit) || 50, 100);
  res.json({ history: history.slice(0, limit) });
});

app.get('/api/whatsapp/history/:id', (req, res) => {
  const history = loadJSON(HISTORY_FILE, []);
  const entry = history.find(h => h.id === req.params.id);
  if (!entry) return res.status(404).json({ error: 'Broadcast not found' });
  res.json({ broadcast: entry });
});

// ─── Group Labels/Tags ──────────────────────────────────────────────────

app.post('/api/whatsapp/groups/:groupId/tags', (req, res) => {
  const { groupId } = req.params;
  const { tags } = req.body;
  if (!tags || !Array.isArray(tags)) return res.status(400).json({ error: 'tags array required' });

  const allTags = loadJSON(GROUP_TAGS_FILE, {});
  allTags[groupId] = [...new Set(tags.map(t => t.trim().toLowerCase()))];
  saveJSON(GROUP_TAGS_FILE, allTags);

  res.json({ ok: true, groupId, tags: allTags[groupId] });
});

app.get('/api/whatsapp/groups/:groupId/tags', (req, res) => {
  const allTags = loadJSON(GROUP_TAGS_FILE, {});
  res.json({ groupId: req.params.groupId, tags: allTags[req.params.groupId] || [] });
});

// ─── Auto-Response Rules ────────────────────────────────────────────────

app.post('/api/whatsapp/auto-rules', (req, res) => {
  const { keywords, response, enabled, account } = req.body;
  if (!keywords || !Array.isArray(keywords) || keywords.length === 0)
    return res.status(400).json({ error: 'keywords array required' });
  if (!response || typeof response !== 'string')
    return res.status(400).json({ error: 'response required' });

  const rules = loadJSON(AUTO_RULES_FILE, []);
  const id = crypto.randomUUID();
  const rule = {
    id,
    keywords,
    response,
    enabled: enabled !== false,
    account: account || null,
    createdAt: new Date().toISOString(),
  };
  rules.push(rule);
  saveJSON(AUTO_RULES_FILE, rules);
  res.json({ ok: true, rule });
});

app.get('/api/whatsapp/auto-rules', (req, res) => {
  const rules = loadJSON(AUTO_RULES_FILE, []);
  res.json({ rules });
});

app.put('/api/whatsapp/auto-rules/:id', (req, res) => {
  const rules = loadJSON(AUTO_RULES_FILE, []);
  const idx = rules.findIndex(r => r.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: 'Rule not found' });

  const { keywords, response, enabled, account } = req.body;
  if (keywords !== undefined) rules[idx].keywords = keywords;
  if (response !== undefined) rules[idx].response = response;
  if (enabled !== undefined) rules[idx].enabled = enabled;
  if (account !== undefined) rules[idx].account = account;
  rules[idx].updatedAt = new Date().toISOString();

  saveJSON(AUTO_RULES_FILE, rules);
  res.json({ ok: true, rule: rules[idx] });
});

app.delete('/api/whatsapp/auto-rules/:id', (req, res) => {
  const rules = loadJSON(AUTO_RULES_FILE, []);
  const filtered = rules.filter(r => r.id !== req.params.id);
  if (filtered.length === rules.length) return res.status(404).json({ error: 'Rule not found' });
  saveJSON(AUTO_RULES_FILE, filtered);
  res.json({ ok: true, deleted: req.params.id });
});

// ─── Start ───────────────────────────────────────────────────────────────

app.listen(PORT, '0.0.0.0', () => {
  console.log(`WhatsApp Multi-Account server on 0.0.0.0:${PORT}`);
  console.log(`Events API: ${KARTIS_EVENTS_URL}`);
  console.log(`TBP URL: ${TBP_URL}`);
  loadAndInitAccounts();
});
