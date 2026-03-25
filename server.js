const express = require('express');
const crypto = require('crypto');
const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode');
const fs = require('fs');
const path = require('path');
const leads = require('./leads');

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
const CONTACTS_FILE = path.join(DATA_DIR, 'contacts.json');
const SETTINGS_FILE = path.join(DATA_DIR, 'settings.json');
const GROUP_STATS_FILE = path.join(DATA_DIR, 'group-stats.json');
const CRM_FILE = path.join(DATA_DIR, 'crm.json');
const BLOCKLIST_FILE = path.join(DATA_DIR, 'blocklist.json');
const LISTS_FILE = path.join(DATA_DIR, 'lists.json');

// ─── Cooldown System ─────────────────────────────────────────────────────
const groupCooldowns = new Map(); // groupId -> timestamp

function getCooldownMinutes() {
  const settings = loadJSON(SETTINGS_FILE, {});
  return settings.cooldownMinutes || parseInt(process.env.COOLDOWN_MINUTES) || 30;
}

function isCooldownActive(groupId) {
  const last = groupCooldowns.get(groupId);
  if (!last) return false;
  const cooldownMs = getCooldownMinutes() * 60 * 1000;
  return (Date.now() - last) < cooldownMs;
}

function setCooldown(groupId) {
  groupCooldowns.set(groupId, Date.now());
}

// ─── Quiet Hours ─────────────────────────────────────────────────────────
function getQuietHours() {
  const settings = loadJSON(SETTINGS_FILE, {});
  return {
    start: settings.quietStart || process.env.QUIET_START || '02:00',
    end: settings.quietEnd || process.env.QUIET_END || '10:00',
  };
}

function isQuietHours() {
  const { start, end } = getQuietHours();
  const now = new Date();
  const currentMinutes = now.getHours() * 60 + now.getMinutes();
  const [sh, sm] = start.split(':').map(Number);
  const [eh, em] = end.split(':').map(Number);
  const startMin = sh * 60 + sm;
  const endMin = eh * 60 + em;
  if (startMin < endMin) {
    return currentMinutes >= startMin && currentMinutes < endMin;
  }
  // Wraps midnight (e.g. 02:00 -> 10:00 doesn't wrap, but 22:00 -> 06:00 does)
  return currentMinutes >= startMin || currentMinutes < endMin;
}

// ─── Group Stats ─────────────────────────────────────────────────────────
function updateGroupStats(groupId, groupName, field) {
  const stats = loadJSON(GROUP_STATS_FILE, {});
  if (!stats[groupId]) {
    stats[groupId] = { groupName, queriesDetected: 0, responseSent: 0, lastQueryAt: null, lastResponseAt: null };
  }
  stats[groupId].groupName = groupName || stats[groupId].groupName;
  if (field === 'query') {
    stats[groupId].queriesDetected++;
    stats[groupId].lastQueryAt = new Date().toISOString();
  } else if (field === 'response') {
    stats[groupId].responseSent++;
    stats[groupId].lastResponseAt = new Date().toISOString();
  }
  saveJSON(GROUP_STATS_FILE, stats);
}

// ─── Scanner Feed (ring buffer, last 100) ────────────────────────────────
const scannerFeed = [];
const MAX_FEED = 100;

function addToFeed(entry) {
  scannerFeed.unshift(entry);
  if (scannerFeed.length > MAX_FEED) scannerFeed.length = MAX_FEED;
}

// ─── CRM In-Memory Store ────────────────────────────────────────────────
const crmContacts = new Map(); // phone -> contact object
let crmDirty = false;

function loadCRM() {
  const data = loadJSON(CRM_FILE, []);
  crmContacts.clear();
  for (const c of data) {
    crmContacts.set(c.id, c);
  }
  console.log(`CRM loaded: ${crmContacts.size} contacts`);
}

function saveCRM() {
  if (!crmDirty) return;
  const arr = Array.from(crmContacts.values());
  saveJSON(CRM_FILE, arr);
  crmDirty = false;
}

// Flush CRM to disk every 30 seconds
setInterval(saveCRM, 30000);

function getBlocklist() {
  return loadJSON(BLOCKLIST_FILE, []);
}

function isBlocked(phone) {
  const bl = getBlocklist();
  return bl.some(b => b.phone === phone);
}

function phoneFromJid(jid) {
  // "972501234567@c.us" -> "972501234567"
  return jid.split('@')[0];
}

function formatPhone(id) {
  // Ensure phone starts with +
  const num = id.replace(/[^0-9]/g, '');
  return '+' + num;
}

function calculateScore(contact) {
  let score = contact.score || 0;
  // Apply decay: -5 per week of inactivity
  if (contact.profile && contact.profile.lastActive) {
    const lastActive = new Date(contact.profile.lastActive).getTime();
    const weeksSinceActive = Math.floor((Date.now() - lastActive) / (7 * 24 * 60 * 60 * 1000));
    if (weeksSinceActive > 0) {
      score = Math.max(0, score - (weeksSinceActive * 5));
    }
  }
  return Math.min(100, Math.max(0, score));
}

function statusFromScore(score) {
  if (score <= 20) return 'cold';
  if (score <= 40) return 'new';
  if (score <= 60) return 'warm';
  if (score <= 80) return 'hot';
  return 'vip';
}

function getCrmContact(phoneId) {
  const contact = crmContacts.get(phoneId);
  if (!contact) return null;
  // Recalculate score with decay
  contact.score = calculateScore(contact);
  contact.status = contact.blocked ? 'blocked' : statusFromScore(contact.score);
  return contact;
}

function upsertCrmContact(phoneId, updates) {
  const now = new Date().toISOString();
  let contact = crmContacts.get(phoneId);
  if (!contact) {
    contact = {
      id: phoneId,
      phone: formatPhone(phoneId),
      name: null,
      pushName: null,
      profilePic: null,
      source: { type: 'unknown', firstSeen: now },
      tags: [],
      lists: ['all-contacts'],
      profile: {
        language: null,
        interests: [],
        lastActive: now,
        messageCount: 0,
        firstMessage: null,
        lastMessage: null,
        triggeredKeywords: [],
        dmSent: false,
        dmSentAt: null,
        responded: false,
        respondedAt: null,
        eventsClicked: 0,
        ticketsPurchased: 0,
      },
      score: 0,
      status: 'new',
      blocked: false,
      createdAt: now,
      updatedAt: now,
    };
    crmContacts.set(phoneId, contact);
  }
  // Apply updates
  if (updates.name) contact.name = updates.name;
  if (updates.pushName) contact.pushName = updates.pushName;
  if (updates.source && !contact.source.groupId) contact.source = updates.source;
  if (updates.tags) {
    for (const tag of updates.tags) {
      if (!contact.tags.includes(tag)) contact.tags.push(tag);
    }
  }
  if (updates.lists) {
    for (const list of updates.lists) {
      if (!contact.lists.includes(list)) contact.lists.push(list);
    }
  }
  if (updates.profile) {
    Object.assign(contact.profile, updates.profile);
  }
  if (updates.score !== undefined) {
    contact.score = Math.min(100, Math.max(0, updates.score));
  }
  contact.updatedAt = now;
  contact.score = calculateScore(contact);
  contact.status = contact.blocked ? 'blocked' : statusFromScore(contact.score);
  crmDirty = true;
  return contact;
}

// ─── CRM Settings Helpers ───────────────────────────────────────────────

function getCrmSettings() {
  const settings = loadJSON(SETTINGS_FILE, {});
  return {
    autoDmEnabled: settings.autoDmEnabled || false,
    autoDmTemplate: settings.autoDmTemplate || "Hey {name}! 🎉 Saw you're looking for events. Here's what's coming up:\n\n{events}\n\n— The Best Parties",
    autoDmCooldownHours: settings.autoDmCooldownHours || 24,
    scrapeIntervalHours: settings.scrapeIntervalHours || 6,
  };
}

function canSendAutoDm(contact) {
  const settings = getCrmSettings();
  if (!settings.autoDmEnabled) return false;
  if (contact.blocked) return false;
  if (contact.profile.dmSent && contact.profile.dmSentAt) {
    const cooldownMs = settings.autoDmCooldownHours * 60 * 60 * 1000;
    if (Date.now() - new Date(contact.profile.dmSentAt).getTime() < cooldownMs) return false;
  }
  return true;
}

// ─── Group Scraper ──────────────────────────────────────────────────────

async function scrapeGroupParticipants(client, chat) {
  const results = { scraped: 0, new: 0, updated: 0 };
  try {
    const groupId = chat.id._serialized;
    const groupName = chat.name || groupId;
    const listSlug = groupName.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

    // Ensure list exists for this group
    const lists = loadJSON(LISTS_FILE, []);
    if (!lists.find(l => l.id === listSlug)) {
      lists.push({ id: listSlug, name: groupName, description: `Auto-created from group: ${groupName}`, createdAt: new Date().toISOString() });
      saveJSON(LISTS_FILE, lists);
    }

    // Get participants - chat.participants is populated for groups
    const participants = chat.participants || [];
    for (const p of participants) {
      const phoneId = p.id._serialized ? phoneFromJid(p.id._serialized) : phoneFromJid(p.id.user || p.id._serialized);
      if (!phoneId || phoneId.length < 5) continue;
      if (isBlocked(formatPhone(phoneId))) continue;

      results.scraped++;
      const existing = crmContacts.has(phoneId);

      upsertCrmContact(phoneId, {
        source: {
          type: 'group_scrape',
          groupId,
          groupName,
          firstSeen: new Date().toISOString(),
        },
        tags: [listSlug],
        lists: ['all-contacts', listSlug],
        profile: { lastActive: new Date().toISOString() },
      });

      if (existing) results.updated++;
      else results.new++;
    }
  } catch (err) {
    console.error(`Scrape error for group ${chat.name}:`, err.message);
  }
  return results;
}

async function scrapeAllGroups() {
  console.log('CRM: Starting group scrape across all accounts...');
  const totals = { scraped: 0, new: 0, updated: 0 };

  for (const [accountId, acc] of accounts) {
    if (!acc.ready) continue;
    try {
      const chats = await acc.client.getChats();
      const groups = chats.filter(c => c.isGroup);
      console.log(`[${accountId}] Scraping ${groups.length} groups...`);

      for (const group of groups) {
        const result = await scrapeGroupParticipants(acc.client, group);
        totals.scraped += result.scraped;
        totals.new += result.new;
        totals.updated += result.updated;
      }
    } catch (err) {
      console.error(`[${accountId}] Scrape failed:`, err.message);
    }
  }

  saveCRM(); // Force flush after scrape
  console.log(`CRM: Scrape complete — scraped: ${totals.scraped}, new: ${totals.new}, updated: ${totals.updated}`);
  return totals;
}

// Schedule periodic scraping
let scrapeInterval = null;
function startScrapeSchedule() {
  const hours = getCrmSettings().scrapeIntervalHours;
  if (scrapeInterval) clearInterval(scrapeInterval);
  scrapeInterval = setInterval(() => scrapeAllGroups(), hours * 60 * 60 * 1000);
  console.log(`CRM: Scrape scheduled every ${hours} hours`);
}

// ─── Contact Profiling ──────────────────────────────────────────────────

const INTEREST_KEYWORDS = {
  parties: ['party', 'parties', 'מסיבה', 'מסיבות'],
  tickets: ['ticket', 'tickets', 'כרטיס', 'כרטיסים', 'טיקט'],
  tables: ['table', 'tables', 'שולחן', 'שולחנות'],
  vip: ['vip', 'אוויאיפי'],
  bottles: ['bottle', 'bottles', 'בקבוק', 'בקבוקים'],
  nightlife: ['club', 'nightlife', 'מועדון'],
};

function detectLanguage(text) {
  return /[\u0590-\u05FF]/.test(text) ? 'he' : 'en';
}

function extractInterests(text) {
  const lower = text.toLowerCase();
  const found = [];
  for (const [interest, keywords] of Object.entries(INTEREST_KEYWORDS)) {
    if (keywords.some(kw => lower.includes(kw))) found.push(interest);
  }
  return found;
}

function profileContactFromMessage(phoneId, message, pushName) {
  const contact = getCrmContact(phoneId) || upsertCrmContact(phoneId, {});
  const lang = detectLanguage(message);
  const interests = extractInterests(message);
  const lower = message.toLowerCase();

  let scoreBoost = 0;
  const newKeywords = [];

  // Check for keyword triggers
  for (const kw of PARTY_KEYWORDS) {
    if (lower.includes(kw.toLowerCase()) && !contact.profile.triggeredKeywords.includes(kw)) {
      newKeywords.push(kw);
      scoreBoost += contact.profile.triggeredKeywords.length === 0 ? 10 : 5;
    }
  }

  // Ticket/price interest
  if (/ticket|price|כרטיס|מחיר|כמה עולה|how much/.test(lower)) scoreBoost += 15;
  // Table/VIP interest
  if (/table|vip|שולחן/.test(lower)) scoreBoost += 30;

  const updates = {
    pushName: pushName || contact.pushName,
    profile: {
      language: lang,
      lastActive: new Date().toISOString(),
      messageCount: (contact.profile.messageCount || 0) + 1,
      lastMessage: message.slice(0, 500),
      triggeredKeywords: [...contact.profile.triggeredKeywords, ...newKeywords],
    },
    score: (contact.score || 0) + scoreBoost,
  };

  if (!contact.profile.firstMessage) {
    updates.profile.firstMessage = message.slice(0, 500);
  }

  if (interests.length > 0) {
    updates.profile.interests = [...new Set([...(contact.profile.interests || []), ...interests])];
  }

  return upsertCrmContact(phoneId, updates);
}

// ─── Contact Capture (legacy, still used for DMs) ───────────────────────
function autoTagContact(message) {
  const lower = message.toLowerCase();
  if (/ticket|כרטיס|טיקט/.test(lower)) return 'tickets';
  if (/table|שולחן/.test(lower)) return 'tables';
  if (/vip/.test(lower)) return 'vip';
  return 'general';
}

function captureContact(phoneNumber, name, message) {
  const contacts = loadJSON(CONTACTS_FILE, []);
  const existing = contacts.find(c => c.phone === phoneNumber);
  const tag = autoTagContact(message);
  if (existing) {
    existing.name = name || existing.name;
    existing.lastMessage = message;
    existing.lastMessageAt = new Date().toISOString();
    if (!existing.tags.includes(tag)) existing.tags.push(tag);
  } else {
    contacts.push({
      id: crypto.randomUUID(),
      phone: phoneNumber,
      name: name || null,
      firstMessage: message,
      lastMessage: message,
      tags: [tag],
      capturedAt: new Date().toISOString(),
      lastMessageAt: new Date().toISOString(),
    });
  }
  saveJSON(CONTACTS_FILE, contacts);
}

// ─── Smart Chat Scanner — Intent Detection ───────────────────────────────

const INTENT_PATTERNS_EN = [
  /anyone\s+know\s+what.?s\s+happen/i,
  /where\s+should\s+we\s+go\s+out/i,
  /looking\s+for\s+something\s+fun/i,
  /any\s+(events?|parties?|clubs?)\s*(happening|tonight|this|around)?/i,
  /recommendations?\s+(for\s+)?(tonight|thursday|friday|saturday|this)/i,
  /who.?s\s+going\s+out/i,
  /what.?s\s+(going\s+on|happening)/i,
  /where\s+can\s+i\s+buy\s+ticket/i,
  /any\s+good\s+(clubs?|parties?|events?|places?)/i,
  /want\s+to\s+go\s+out/i,
  /let.?s\s+go\s+out/i,
  /plans?\s+for\s+(tonight|this|the)/i,
  /anything\s+happening/i,
  /what\s+are\s+we\s+doing/i,
  /where\s+to\s+go\s+(tonight|this|out)/i,
  /what\s+to\s+do\s+(tonight|this)/i,
];

const INTENT_PATTERNS_HE = [
  /מישה[וּ]?\s*יוד[עת]\s*מה\s*(יש|קורה)/,
  /איפה\s*יוצאים/,
  /מחפש[ת]?\s*משהו\s*(לעשות|כיף)/,
  /יש\s*(אירועים|מסיבות|משהו)/,
  /המלצות?\s*(ל|על)/,
  /מי\s*יוצא/,
  /מה\s*קורה\s*(ב|ה)?(סופש|סוף\s*שבוע|ערב)/,
  /רוצ[הה]\s*לצאת/,
  /בוא[וי]?\s*נצא/,
  /תוכניות?\s*(ל|ה)?/,
  /יש\s*משהו/,
  /מה\s*עושים/,
  /לאן\s*(הולכים|יוצאים|נלך)/,
  /מה\s*יש\s*(ה)?ערב/,
  /מה\s*יש\s*(ב)?(סופש|סוף\s*שבוע)/,
];

function isPartyIntent(message) {
  const text = message.trim();
  for (const re of INTENT_PATTERNS_EN) {
    if (re.test(text)) return true;
  }
  for (const re of INTENT_PATTERNS_HE) {
    if (re.test(text)) return true;
  }
  return false;
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

    // Default party keyword handler (existing behavior + smart intent detection)
    if (chat.isGroup) {
      const keywordMatch = PARTY_KEYWORDS.some(kw => lower.includes(kw.toLowerCase()));
      const intentMatch = isPartyIntent(msg.body);
      const isPartyQuery = keywordMatch || intentMatch;

      // Lead detection — runs on every group message
      const groupId = chat.id._serialized;
      const senderName = msg.author || msg.from;
      const lead = leads.detectLead(msg.body, senderName, msg.from, chat.name, groupId);
      if (lead.isLead) {
        leads.storeLead({ ...lead, groupId, groupName: chat.name, senderId: msg.from, senderName, account: accountId });
      }

      if (isPartyQuery) {
        console.log(`[${accountId}] Party ${intentMatch ? 'intent' : 'keyword'} in group "${chat.name}" from ${senderName}`);

        // CRM: Profile contact from group message
        const senderJid = msg.author || msg.from;
        const senderPhone = phoneFromJid(senderJid);
        if (senderPhone && !isBlocked(formatPhone(senderPhone))) {
          const contact = await msg.getContact().catch(() => null);
          const pushName = contact ? (contact.pushname || contact.name || null) : null;
          const groupSlug = (chat.name || '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

          profileContactFromMessage(senderPhone, msg.body, pushName);
          upsertCrmContact(senderPhone, {
            source: { type: 'group_scrape', groupId, groupName: chat.name, firstSeen: new Date().toISOString() },
            tags: [groupSlug],
            lists: ['all-contacts', groupSlug],
          });

          // Auto-DM if enabled and cooldown passed for this contact
          const crmContact = getCrmContact(senderPhone);
          if (crmContact && canSendAutoDm(crmContact)) {
            try {
              const settings = getCrmSettings();
              const recommendation = await getRecommendation(msg.body);
              const dmMessage = settings.autoDmTemplate
                .replace('{name}', pushName || 'there')
                .replace('{events}', recommendation);
              const dmChat = await acc.client.getChatById(senderJid).catch(() => null);
              if (dmChat) {
                await dmChat.sendMessage(dmMessage);
                upsertCrmContact(senderPhone, {
                  profile: { dmSent: true, dmSentAt: new Date().toISOString() },
                });
                console.log(`[${accountId}] Auto-DM sent to ${senderPhone}`);
              }
            } catch (dmErr) {
              console.error(`[${accountId}] Auto-DM failed for ${senderPhone}:`, dmErr.message);
            }
          }
        }

        // Track stats
        updateGroupStats(groupId, chat.name, 'query');

        // Check quiet hours
        if (isQuietHours()) {
          console.log(`[${accountId}] Skipping response — quiet hours active`);
          addToFeed({
            timestamp: new Date().toISOString(), groupName: chat.name, groupId,
            senderName, message: msg.body.slice(0, 200), responded: false,
            responsePreview: null, account: accountId,
          });
          return;
        }

        // Check cooldown
        if (isCooldownActive(groupId)) {
          console.log(`[${accountId}] Skipping response — cooldown active for "${chat.name}"`);
          addToFeed({
            timestamp: new Date().toISOString(), groupName: chat.name, groupId,
            senderName, message: msg.body.slice(0, 200), responded: false,
            responsePreview: null, account: accountId,
          });
          return;
        }

        try {
          const recommendation = await getRecommendation(msg.body);
          await msg.reply(recommendation);
          setCooldown(groupId);
          updateGroupStats(groupId, chat.name, 'response');
          console.log(`[${accountId}] Replied with event recommendation`);
          addToFeed({
            timestamp: new Date().toISOString(), groupName: chat.name, groupId,
            senderName, message: msg.body.slice(0, 200), responded: true,
            responsePreview: recommendation.slice(0, 150), account: accountId,
          });
        } catch (err) {
          console.error(`[${accountId}] Failed to reply:`, err.message);
          await msg.reply(
            `🎉 *The Best Parties*\n\nCheck out our events ➡️ ${TBP_URL}/events`
          ).catch(() => {});
        }
      }
      return;
    }

    // In DMs: capture contact + CRM profiling + always respond with event info
    console.log(`[${accountId}] DM from ${msg.from}: ${msg.body.slice(0, 50)}...`);
    try {
      const contact = await msg.getContact().catch(() => null);
      const contactName = contact ? (contact.pushname || contact.name || null) : null;
      captureContact(msg.from, contactName, msg.body);

      // CRM: Profile DM contact and mark as responded if they were DM'd before
      const dmPhone = phoneFromJid(msg.from);
      if (dmPhone && !isBlocked(formatPhone(dmPhone))) {
        profileContactFromMessage(dmPhone, msg.body, contactName);
        const crmContact = getCrmContact(dmPhone);
        if (crmContact && crmContact.profile.dmSent && !crmContact.profile.responded) {
          upsertCrmContact(dmPhone, {
            profile: { responded: true, respondedAt: new Date().toISOString() },
            score: (crmContact.score || 0) + 20,
          });
          console.log(`[${accountId}] CRM: DM response from ${dmPhone}, +20 score`);
        }
      }
    } catch (e) {
      console.error(`[${accountId}] Contact capture failed:`, e.message);
    }
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

// ─── Cooldown Endpoints ──────────────────────────────────────────────────

app.get('/api/whatsapp/cooldowns', (req, res) => {
  const cooldownMin = getCooldownMinutes();
  const active = [];
  for (const [groupId, ts] of groupCooldowns) {
    const elapsed = Date.now() - ts;
    const cooldownMs = cooldownMin * 60 * 1000;
    if (elapsed < cooldownMs) {
      active.push({
        groupId,
        lastResponseAt: new Date(ts).toISOString(),
        expiresAt: new Date(ts + cooldownMs).toISOString(),
        remainingSeconds: Math.round((cooldownMs - elapsed) / 1000),
      });
    }
  }
  res.json({ cooldowns: active, cooldownMinutes: cooldownMin });
});

app.post('/api/whatsapp/cooldowns/reset', (req, res) => {
  const count = groupCooldowns.size;
  groupCooldowns.clear();
  console.log(`Cooldowns reset (cleared ${count} entries)`);
  res.json({ ok: true, cleared: count });
});

// ─── Contact Endpoints ──────────────────────────────────────────────────

app.get('/api/whatsapp/contacts', (req, res) => {
  const contacts = loadJSON(CONTACTS_FILE, []);
  res.json({ contacts });
});

app.get('/api/whatsapp/contacts/export', (req, res) => {
  const contacts = loadJSON(CONTACTS_FILE, []);
  const header = 'phone,name,tags,firstMessage,capturedAt,lastMessageAt';
  const rows = contacts.map(c => {
    const escape = (s) => `"${(s || '').replace(/"/g, '""')}"`;
    return [escape(c.phone), escape(c.name), escape((c.tags || []).join(';')),
            escape(c.firstMessage), escape(c.capturedAt), escape(c.lastMessageAt)].join(',');
  });
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=contacts.csv');
  res.send([header, ...rows].join('\n'));
});

app.delete('/api/whatsapp/contacts/:id', (req, res) => {
  const contacts = loadJSON(CONTACTS_FILE, []);
  const filtered = contacts.filter(c => c.id !== req.params.id);
  if (filtered.length === contacts.length) return res.status(404).json({ error: 'Contact not found' });
  saveJSON(CONTACTS_FILE, filtered);
  res.json({ ok: true, deleted: req.params.id });
});

// ─── Settings Endpoints (Quiet Hours, Cooldown, CRM) ────────────────────

app.get('/api/whatsapp/settings', (req, res) => {
  const settings = loadJSON(SETTINGS_FILE, {});
  const { start, end } = getQuietHours();
  const crmSettings = getCrmSettings();
  res.json({
    quietStart: start,
    quietEnd: end,
    cooldownMinutes: getCooldownMinutes(),
    isQuietNow: isQuietHours(),
    ...crmSettings,
    ...settings,
  });
});

app.put('/api/whatsapp/settings', (req, res) => {
  const settings = loadJSON(SETTINGS_FILE, {});
  const { quietStart, quietEnd, cooldownMinutes, autoDmEnabled, autoDmTemplate, autoDmCooldownHours, scrapeIntervalHours } = req.body;
  if (quietStart !== undefined) settings.quietStart = quietStart;
  if (quietEnd !== undefined) settings.quietEnd = quietEnd;
  if (cooldownMinutes !== undefined) settings.cooldownMinutes = Number(cooldownMinutes);
  if (autoDmEnabled !== undefined) settings.autoDmEnabled = Boolean(autoDmEnabled);
  if (autoDmTemplate !== undefined) settings.autoDmTemplate = String(autoDmTemplate);
  if (autoDmCooldownHours !== undefined) settings.autoDmCooldownHours = Number(autoDmCooldownHours);
  if (scrapeIntervalHours !== undefined) {
    settings.scrapeIntervalHours = Number(scrapeIntervalHours);
    startScrapeSchedule(); // Restart schedule with new interval
  }
  saveJSON(SETTINGS_FILE, settings);
  res.json({ ok: true, settings });
});

// ─── Group Stats Endpoint ───────────────────────────────────────────────

app.get('/api/whatsapp/groups/stats', (req, res) => {
  const stats = loadJSON(GROUP_STATS_FILE, {});
  const sorted = Object.entries(stats)
    .map(([groupId, s]) => ({ groupId, ...s }))
    .sort((a, b) => {
      const aTime = a.lastQueryAt || a.lastResponseAt || '';
      const bTime = b.lastQueryAt || b.lastResponseAt || '';
      return bTime.localeCompare(aTime);
    });
  res.json({ stats: sorted });
});

// ─── Scanner Feed & Stats Endpoints ─────────────────────────────────────

app.get('/api/whatsapp/scanner/feed', (req, res) => {
  res.json({ feed: scannerFeed });
});

app.get('/api/whatsapp/scanner/stats', (req, res) => {
  const todayStr = new Date().toISOString().slice(0, 10);
  const todayEntries = scannerFeed.filter(e => e.timestamp.startsWith(todayStr));
  const totalQueries = todayEntries.length;
  const totalResponses = todayEntries.filter(e => e.responded).length;
  const activeGroups = new Set(todayEntries.map(e => e.groupId)).size;
  res.json({ totalQueriesToday: totalQueries, totalResponsesToday: totalResponses, activeGroupsToday: activeGroups });
});

// ─── Leads Monitor Endpoints ─────────────────────────────────────────────

app.get('/api/whatsapp/leads', (req, res) => {
  const filters = {
    status: req.query.status,
    group: req.query.group,
    account: req.query.account,
    minConfidence: req.query.minConfidence,
    category: req.query.category,
    language: req.query.language,
    limit: req.query.limit,
    offset: req.query.offset,
  };
  res.json(leads.getLeads(filters));
});

app.get('/api/whatsapp/leads/stats', (req, res) => {
  res.json(leads.getLeadStats());
});

app.get('/api/whatsapp/leads/export', (req, res) => {
  const csv = leads.exportLeadsCsv({
    status: req.query.status,
    group: req.query.group,
    account: req.query.account,
  });
  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=leads.csv');
  res.send(csv);
});

app.put('/api/whatsapp/leads/:id', (req, res) => {
  const { status, note } = req.body;
  if (!status) return res.status(400).json({ error: 'status required' });
  try {
    const lead = leads.updateLeadStatus(req.params.id, status, note);
    if (!lead) return res.status(404).json({ error: 'Lead not found' });
    res.json({ ok: true, lead });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.get('/api/whatsapp/keywords', (req, res) => {
  res.json(leads.getCustomKeywords());
});

app.post('/api/whatsapp/keywords', (req, res) => {
  const { keywords, category } = req.body;
  if (!keywords || !Array.isArray(keywords) || !category) {
    return res.status(400).json({ error: 'keywords[] and category required' });
  }
  try {
    const updated = leads.setCustomKeywords({ keywords, category });
    res.json({ ok: true, custom: updated });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.delete('/api/whatsapp/keywords/:keyword', (req, res) => {
  const keyword = decodeURIComponent(req.params.keyword);
  const removed = leads.removeCustomKeyword(keyword);
  if (!removed) return res.status(404).json({ error: 'Keyword not found in custom keywords' });
  res.json({ ok: true, deleted: keyword });
});

// ─── CRM Scrape Endpoints ───────────────────────────────────────────────

app.post('/api/whatsapp/scrape', async (req, res) => {
  try {
    const result = await scrapeAllGroups();
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/whatsapp/scrape/:groupId', async (req, res) => {
  const { groupId } = req.params;
  const accountId = req.body.account || 'default';
  const acc = accounts.get(accountId);
  if (!acc || !acc.ready) return res.status(503).json({ error: 'Account not ready' });

  try {
    const chats = await acc.client.getChats();
    const group = chats.find(c => c.isGroup && c.id._serialized === groupId);
    if (!group) return res.status(404).json({ error: 'Group not found' });
    const result = await scrapeGroupParticipants(acc.client, group);
    saveCRM();
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── CRM Contact Endpoints ─────────────────────────────────────────────

app.get('/api/whatsapp/crm/contacts', (req, res) => {
  let contactList = Array.from(crmContacts.values()).map(c => {
    c.score = calculateScore(c);
    c.status = c.blocked ? 'blocked' : statusFromScore(c.score);
    return c;
  });

  // Filter by status
  if (req.query.status) {
    const statuses = req.query.status.split(',').map(s => s.trim());
    contactList = contactList.filter(c => statuses.includes(c.status));
  }

  // Filter by tags
  if (req.query.tags) {
    const tags = req.query.tags.split(',').map(t => t.trim().toLowerCase());
    contactList = contactList.filter(c => c.tags.some(t => tags.includes(t.toLowerCase())));
  }

  // Filter by list
  if (req.query.list) {
    contactList = contactList.filter(c => c.lists.includes(req.query.list));
  }

  // Search by name/phone
  if (req.query.q) {
    const q = req.query.q.toLowerCase();
    contactList = contactList.filter(c =>
      (c.name && c.name.toLowerCase().includes(q)) ||
      (c.pushName && c.pushName.toLowerCase().includes(q)) ||
      (c.phone && c.phone.includes(q)) ||
      (c.id && c.id.includes(q))
    );
  }

  // Sort
  const sort = req.query.sort || 'updatedAt';
  if (sort === 'score') {
    contactList.sort((a, b) => (b.score || 0) - (a.score || 0));
  } else if (sort === 'lastActive') {
    contactList.sort((a, b) => {
      const aTime = a.profile?.lastActive || a.updatedAt || '';
      const bTime = b.profile?.lastActive || b.updatedAt || '';
      return bTime.localeCompare(aTime);
    });
  } else {
    contactList.sort((a, b) => (b.updatedAt || '').localeCompare(a.updatedAt || ''));
  }

  // Pagination
  const limit = Math.min(parseInt(req.query.limit) || 50, 500);
  const offset = parseInt(req.query.offset) || 0;
  const total = contactList.length;
  contactList = contactList.slice(offset, offset + limit);

  res.json({ contacts: contactList, total, limit, offset });
});

app.get('/api/whatsapp/crm/contacts/stats', (req, res) => {
  const all = Array.from(crmContacts.values());
  const byStatus = {};
  const bySource = {};
  for (const c of all) {
    const score = calculateScore(c);
    const status = c.blocked ? 'blocked' : statusFromScore(score);
    byStatus[status] = (byStatus[status] || 0) + 1;
    const groupName = c.source?.groupName || 'unknown';
    bySource[groupName] = (bySource[groupName] || 0) + 1;
  }
  res.json({ total: all.length, byStatus, bySource });
});

app.get('/api/whatsapp/crm/contacts/export', (req, res) => {
  const all = Array.from(crmContacts.values());
  const escape = (s) => `"${(s || '').toString().replace(/"/g, '""')}"`;
  const header = 'id,phone,name,pushName,status,score,tags,lists,language,messageCount,firstMessage,lastMessage,dmSent,responded,createdAt,updatedAt';
  const rows = all.map(c => [
    escape(c.id), escape(c.phone), escape(c.name), escape(c.pushName),
    escape(c.blocked ? 'blocked' : statusFromScore(calculateScore(c))),
    calculateScore(c),
    escape((c.tags || []).join(';')), escape((c.lists || []).join(';')),
    escape(c.profile?.language), c.profile?.messageCount || 0,
    escape(c.profile?.firstMessage), escape(c.profile?.lastMessage),
    c.profile?.dmSent || false, c.profile?.responded || false,
    escape(c.createdAt), escape(c.updatedAt),
  ].join(','));

  res.setHeader('Content-Type', 'text/csv');
  res.setHeader('Content-Disposition', 'attachment; filename=crm-contacts.csv');
  res.send([header, ...rows].join('\n'));
});

app.get('/api/whatsapp/crm/contacts/:id', (req, res) => {
  const contact = getCrmContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Contact not found' });
  res.json({ contact });
});

app.put('/api/whatsapp/crm/contacts/:id', (req, res) => {
  const contact = getCrmContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Contact not found' });

  const { tags, status, name, notes, score } = req.body;
  const updates = {};
  if (tags) updates.tags = tags;
  if (name) updates.name = name;
  if (score !== undefined) updates.score = Number(score);
  if (status === 'customer' || status === 'vip') {
    if (status === 'customer') updates.score = 75;
    if (status === 'vip') updates.score = 90;
  }
  if (notes !== undefined) updates.profile = { ...updates.profile, notes };

  const updated = upsertCrmContact(req.params.id, updates);
  saveCRM();
  res.json({ ok: true, contact: updated });
});

app.delete('/api/whatsapp/crm/contacts/:id', (req, res) => {
  if (!crmContacts.has(req.params.id)) return res.status(404).json({ error: 'Contact not found' });
  crmContacts.delete(req.params.id);
  crmDirty = true;
  saveCRM();
  res.json({ ok: true, deleted: req.params.id });
});

app.post('/api/whatsapp/crm/contacts/:id/dm', async (req, res) => {
  const contact = getCrmContact(req.params.id);
  if (!contact) return res.status(404).json({ error: 'Contact not found' });
  if (contact.blocked) return res.status(400).json({ error: 'Contact is blocked' });

  const { message, account } = req.body;
  if (!message) return res.status(400).json({ error: 'message required' });

  const accountId = account || 'default';
  const acc = accounts.get(accountId);
  if (!acc || !acc.ready) return res.status(503).json({ error: 'Account not ready' });

  try {
    const jid = contact.id + '@c.us';
    await acc.client.sendMessage(jid, message);
    upsertCrmContact(contact.id, {
      profile: { dmSent: true, dmSentAt: new Date().toISOString() },
    });
    saveCRM();
    res.json({ ok: true, sent: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ─── Contact Lists ──────────────────────────────────────────────────────

app.post('/api/whatsapp/lists', (req, res) => {
  const { name, description } = req.body;
  if (!name) return res.status(400).json({ error: 'name required' });
  const id = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

  const lists = loadJSON(LISTS_FILE, []);
  if (lists.find(l => l.id === id)) return res.status(409).json({ error: 'List already exists' });

  const list = { id, name, description: description || '', createdAt: new Date().toISOString() };
  lists.push(list);
  saveJSON(LISTS_FILE, lists);
  res.json({ ok: true, list });
});

app.get('/api/whatsapp/lists', (req, res) => {
  const lists = loadJSON(LISTS_FILE, []);
  const all = Array.from(crmContacts.values());
  const result = lists.map(l => ({
    ...l,
    contactCount: all.filter(c => c.lists.includes(l.id)).length,
  }));
  res.json({ lists: result });
});

app.get('/api/whatsapp/lists/:id', (req, res) => {
  const lists = loadJSON(LISTS_FILE, []);
  const list = lists.find(l => l.id === req.params.id);
  if (!list) return res.status(404).json({ error: 'List not found' });

  const contacts = Array.from(crmContacts.values())
    .filter(c => c.lists.includes(req.params.id))
    .map(c => { c.score = calculateScore(c); c.status = c.blocked ? 'blocked' : statusFromScore(c.score); return c; });

  res.json({ list, contacts });
});

app.post('/api/whatsapp/lists/:id/contacts', (req, res) => {
  const { contactIds } = req.body;
  if (!contactIds || !Array.isArray(contactIds)) return res.status(400).json({ error: 'contactIds array required' });

  const lists = loadJSON(LISTS_FILE, []);
  if (!lists.find(l => l.id === req.params.id)) return res.status(404).json({ error: 'List not found' });

  let added = 0;
  for (const cid of contactIds) {
    const contact = crmContacts.get(cid);
    if (contact && !contact.lists.includes(req.params.id)) {
      contact.lists.push(req.params.id);
      added++;
      crmDirty = true;
    }
  }
  saveCRM();
  res.json({ ok: true, added });
});

app.delete('/api/whatsapp/lists/:id/contacts', (req, res) => {
  const { contactIds } = req.body;
  if (!contactIds || !Array.isArray(contactIds)) return res.status(400).json({ error: 'contactIds array required' });

  let removed = 0;
  for (const cid of contactIds) {
    const contact = crmContacts.get(cid);
    if (contact) {
      const idx = contact.lists.indexOf(req.params.id);
      if (idx >= 0) {
        contact.lists.splice(idx, 1);
        removed++;
        crmDirty = true;
      }
    }
  }
  saveCRM();
  res.json({ ok: true, removed });
});

// ─── Bulk DM from List ──────────────────────────────────────────────────

app.post('/api/whatsapp/lists/:id/broadcast', async (req, res) => {
  const lists = loadJSON(LISTS_FILE, []);
  if (!lists.find(l => l.id === req.params.id)) return res.status(404).json({ error: 'List not found' });

  const { message, account } = req.body;
  if (!message) return res.status(400).json({ error: 'message required' });

  const accountId = account || 'default';
  const acc = accounts.get(accountId);
  if (!acc || !acc.ready) return res.status(503).json({ error: 'Account not ready' });

  const contacts = Array.from(crmContacts.values()).filter(c => c.lists.includes(req.params.id));
  const settings = getCrmSettings();
  const cooldownMs = settings.autoDmCooldownHours * 60 * 60 * 1000;

  let sent = 0, skipped = 0, failed = 0;

  for (const contact of contacts) {
    if (contact.blocked || isBlocked(contact.phone)) { skipped++; continue; }
    if (contact.profile.dmSentAt && (Date.now() - new Date(contact.profile.dmSentAt).getTime()) < cooldownMs) {
      skipped++; continue;
    }

    try {
      const jid = contact.id + '@c.us';
      await acc.client.sendMessage(jid, message);
      upsertCrmContact(contact.id, {
        profile: { dmSent: true, dmSentAt: new Date().toISOString() },
      });
      sent++;
      await new Promise(r => setTimeout(r, 3000));
    } catch (err) {
      console.error(`Bulk DM failed for ${contact.id}:`, err.message);
      failed++;
    }
  }

  saveCRM();

  logBroadcast({
    id: crypto.randomUUID(),
    name: `List broadcast: ${req.params.id}`,
    timestamp: new Date().toISOString(),
    account: accountId,
    chatIds: contacts.map(c => c.id + '@c.us'),
    messagePreview: message.slice(0, 200),
    sent, failed, skipped,
    total: contacts.length,
  });

  res.json({ sent, skipped, failed, total: contacts.length });
});

// ─── Blocklist ──────────────────────────────────────────────────────────

app.get('/api/whatsapp/blocklist', (req, res) => {
  const blocklist = getBlocklist();
  res.json({ blocklist });
});

app.post('/api/whatsapp/blocklist', (req, res) => {
  const { phone, reason } = req.body;
  if (!phone) return res.status(400).json({ error: 'phone required' });

  const blocklist = getBlocklist();
  if (blocklist.find(b => b.phone === phone)) return res.status(409).json({ error: 'Already blocked' });

  blocklist.push({ phone, reason: reason || null, blockedAt: new Date().toISOString() });
  saveJSON(BLOCKLIST_FILE, blocklist);

  const phoneId = phone.replace(/[^0-9]/g, '');
  const contact = crmContacts.get(phoneId);
  if (contact) {
    contact.blocked = true;
    contact.status = 'blocked';
    crmDirty = true;
    saveCRM();
  }

  res.json({ ok: true, blocked: phone });
});

app.delete('/api/whatsapp/blocklist/:phone', (req, res) => {
  const blocklist = getBlocklist();
  const filtered = blocklist.filter(b => b.phone !== req.params.phone);
  if (filtered.length === blocklist.length) return res.status(404).json({ error: 'Not found in blocklist' });
  saveJSON(BLOCKLIST_FILE, filtered);

  const phoneId = req.params.phone.replace(/[^0-9]/g, '');
  const contact = crmContacts.get(phoneId);
  if (contact) {
    contact.blocked = false;
    crmDirty = true;
    saveCRM();
  }

  res.json({ ok: true, unblocked: req.params.phone });
});

// ─── Start ───────────────────────────────────────────────────────────────

app.listen(PORT, '0.0.0.0', () => {
  console.log(`WhatsApp Multi-Account server on 0.0.0.0:${PORT}`);
  console.log(`Events API: ${KARTIS_EVENTS_URL}`);
  console.log(`TBP URL: ${TBP_URL}`);
  leads.initLeads(DATA_DIR);
  loadCRM();
  loadAndInitAccounts();
  startScrapeSchedule();
  // Initial scrape 60s after startup (give clients time to connect)
  setTimeout(() => scrapeAllGroups(), 60000);
});
