// ─── Leads Monitor Module ────────────────────────────────────────────────────
// Real-time keyword detection, lead storage, stats, custom keywords, CSV export
// CommonJS module — imported by server.js

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ─── Keyword Dictionaries ────────────────────────────────────────────────────

const KEYWORDS = {
  en: {
    party: ['party', 'parties', 'partying'],
    event: ['event', 'events'],
    club: ['club', 'clubs', 'nightclub'],
    dj: ['dj', 'deejay'],
    vip: ['bottle service', 'vip', 'bottles', 'bottle'],
    guestlist: ['guestlist', 'guest list', 'list'],
    tickets: ['tickets', 'ticket', 'tix'],
    nightlife: ['nightlife', 'night life', 'going out', 'go out', 'tonight', 'this weekend', 'where to go'],
    celebration: ['celebration', 'birthday party', 'birthday', 'bday', 'new years', 'nye', 'new year'],
    venue: ['rooftop', 'venue', 'afterparty', 'after party', 'rave', 'festival'],
  },
  he: {
    party: ['מסיבה', 'מסיבות', 'פארטי'],
    event: ['אירוע', 'אירועים'],
    club: ['מועדון', 'מועדונים'],
    dj: ['דיג׳יי', 'דיגיי', 'dj'],
    vip: ['שולחן VIP', 'שולחן', 'בקבוק', 'בקבוקים', 'vip'],
    guestlist: ['רשימת אורחים', 'רשימה', 'ליסט'],
    tickets: ['כרטיסים', 'כרטיס', 'טיקט', 'טיקטים', 'כמה עולה'],
    nightlife: ['היום בלילה', 'הלילה', 'הערב', 'סוף שבוע', 'סופש', 'לצאת', 'איפה יוצאים', 'מה עושים'],
    celebration: ['חגיגה', 'יום הולדת', 'חגיגת', 'יומולדת'],
    venue: ['גג', 'מקום', 'אפטר', 'רייב', 'פסטיבל'],
  }
};

// Hebrew party-specific terms that get a confidence bonus
const HEBREW_PARTY_TERMS = new Set([
  'מסיבה', 'מסיבות', 'פארטי', 'מועדון', 'מועדונים',
  'איפה יוצאים', 'מה עושים', 'הלילה', 'הערב',
]);

// ─── State ───────────────────────────────────────────────────────────────────

let DATA_DIR = '.';
let leadsFilePath = '';
let customKeywordsFilePath = '';

// Ring buffer — last 500 leads in memory
const MAX_LEADS = 500;
let leads = [];

// Custom keywords loaded from disk
let customKeywords = {}; // { category: [keyword, ...] }

// Persistence debounce
let persistTimer = null;
const PERSIST_INTERVAL = 30000; // 30s

// Stats — reset daily at midnight
let stats = {
  leadsToday: 0,
  leadsThisWeek: 0,
  topGroups: new Map(),
  topKeywords: new Map(),
  hourlyDistribution: new Array(24).fill(0),
  peakHour: 0,
  lastResetDate: null,
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

function loadJSON(filePath, fallback) {
  try {
    if (fs.existsSync(filePath)) return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch (e) { console.error(`[leads] Failed to load ${filePath}:`, e.message); }
  return fallback;
}

function saveJSON(filePath, data) {
  try { fs.writeFileSync(filePath, JSON.stringify(data, null, 2)); }
  catch (e) { console.error(`[leads] Failed to save ${filePath}:`, e.message); }
}

function schedulePersist() {
  if (persistTimer) return; // already scheduled
  persistTimer = setTimeout(() => {
    persistTimer = null;
    saveJSON(leadsFilePath, leads);
  }, PERSIST_INTERVAL);
}

function resetStatsIfNeeded() {
  const todayStr = new Date().toISOString().slice(0, 10);
  if (stats.lastResetDate !== todayStr) {
    // Check if it's a new week (Monday)
    const today = new Date();
    const dayOfWeek = today.getDay(); // 0=Sun
    const isNewWeek = dayOfWeek === 1 && stats.lastResetDate !== todayStr;

    stats.leadsToday = 0;
    if (isNewWeek) stats.leadsThisWeek = 0;
    stats.topGroups = new Map();
    stats.topKeywords = new Map();
    stats.hourlyDistribution = new Array(24).fill(0);
    stats.peakHour = 0;
    stats.lastResetDate = todayStr;
  }
}

// ─── Build lookup for fast matching ──────────────────────────────────────────

function buildKeywordLookup() {
  // Returns array of { keyword, category, language }
  // Sorted longest-first so multi-word phrases match before single words
  const entries = [];

  for (const [lang, categories] of Object.entries(KEYWORDS)) {
    for (const [category, words] of Object.entries(categories)) {
      for (const word of words) {
        entries.push({ keyword: word.toLowerCase(), category, language: lang });
      }
    }
  }

  // Add custom keywords (treated as language-agnostic)
  for (const [category, words] of Object.entries(customKeywords)) {
    if (!Array.isArray(words)) continue;
    for (const word of words) {
      entries.push({ keyword: word.toLowerCase(), category, language: 'custom' });
    }
  }

  // Sort longest first for greedy matching
  entries.sort((a, b) => b.keyword.length - a.keyword.length);
  return entries;
}

// ─── Lead Detection ──────────────────────────────────────────────────────────

function detectLead(message, senderName, senderId, groupName, groupId) {
  if (!message || typeof message !== 'string') {
    return { isLead: false };
  }

  const lower = message.toLowerCase();
  const lookup = buildKeywordLookup();

  const matchedKeywords = [];
  const categoriesSet = new Set();
  let detectedLang = null;
  let hebrewPartyBonus = false;

  for (const entry of lookup) {
    if (lower.includes(entry.keyword)) {
      // Avoid duplicate keyword matches
      if (!matchedKeywords.includes(entry.keyword)) {
        matchedKeywords.push(entry.keyword);
      }
      categoriesSet.add(entry.category);

      // Track language (prefer first detected)
      if (!detectedLang && entry.language !== 'custom') {
        detectedLang = entry.language;
      }

      // Check for Hebrew party bonus
      if (HEBREW_PARTY_TERMS.has(entry.keyword)) {
        hebrewPartyBonus = true;
      }
    }
  }

  if (matchedKeywords.length === 0) {
    return { isLead: false };
  }

  // Confidence scoring
  let confidence;
  if (matchedKeywords.length >= 3) {
    confidence = 0.9;
  } else if (matchedKeywords.length === 2) {
    confidence = 0.7;
  } else {
    confidence = 0.4;
  }

  if (hebrewPartyBonus) confidence += 0.1;
  if (message.includes('?')) confidence += 0.05;

  // Cap at 1.0
  confidence = Math.min(confidence, 1.0);
  // Round to 2 decimals
  confidence = Math.round(confidence * 100) / 100;

  // Build highlighted message
  let highlightedMessage = message;
  // Sort matched keywords longest-first to avoid partial replacement issues
  const sortedMatched = [...matchedKeywords].sort((a, b) => b.length - a.length);
  for (const kw of sortedMatched) {
    // Case-insensitive replace all occurrences
    const escaped = kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    highlightedMessage = highlightedMessage.replace(
      new RegExp(escaped, 'gi'),
      '<mark>$&</mark>'
    );
  }

  const categories = [...categoriesSet];

  return {
    isLead: true,
    matchedKeywords,
    categories,
    language: detectedLang || 'en',
    confidence,
    message,
    highlightedMessage,
  };
}

// ─── Lead Storage ────────────────────────────────────────────────────────────

function extractPhone(senderId) {
  if (!senderId) return null;
  // senderId format: "972501234567@c.us" or "972501234567:34@c.us"
  const match = senderId.match(/^(\d+)/);
  if (match) return '+' + match[1];
  return null;
}

function storeLead(leadData) {
  resetStatsIfNeeded();

  const lead = {
    id: 'lead_' + Date.now() + '_' + Math.random().toString(36).slice(2, 7),
    timestamp: new Date().toISOString(),
    groupId: leadData.groupId || null,
    groupName: leadData.groupName || null,
    senderId: leadData.senderId || null,
    senderName: leadData.senderName || null,
    senderPhone: extractPhone(leadData.senderId),
    message: leadData.message || '',
    highlightedMessage: leadData.highlightedMessage || '',
    matchedKeywords: leadData.matchedKeywords || [],
    categories: leadData.categories || [],
    language: leadData.language || 'en',
    confidence: leadData.confidence || 0,
    status: 'new',
    actions: [],
    account: leadData.account || 'default',
  };

  leads.unshift(lead);
  if (leads.length > MAX_LEADS) leads.length = MAX_LEADS;

  // Update stats
  stats.leadsToday++;
  stats.leadsThisWeek++;

  if (lead.groupId) {
    stats.topGroups.set(lead.groupId, (stats.topGroups.get(lead.groupId) || 0) + 1);
  }
  for (const kw of lead.matchedKeywords) {
    stats.topKeywords.set(kw, (stats.topKeywords.get(kw) || 0) + 1);
  }

  const hour = new Date().getHours();
  stats.hourlyDistribution[hour]++;

  // Recalculate peak hour
  let maxCount = 0;
  for (let h = 0; h < 24; h++) {
    if (stats.hourlyDistribution[h] > maxCount) {
      maxCount = stats.hourlyDistribution[h];
      stats.peakHour = h;
    }
  }

  schedulePersist();
  return lead;
}

// ─── Query Leads ─────────────────────────────────────────────────────────────

function getLeads(filters = {}) {
  let result = leads;

  if (filters.status) {
    result = result.filter(l => l.status === filters.status);
  }
  if (filters.group) {
    result = result.filter(l => l.groupId === filters.group);
  }
  if (filters.account) {
    result = result.filter(l => l.account === filters.account);
  }
  if (filters.minConfidence) {
    const min = parseFloat(filters.minConfidence);
    result = result.filter(l => l.confidence >= min);
  }
  if (filters.category) {
    result = result.filter(l => l.categories.includes(filters.category));
  }
  if (filters.language) {
    result = result.filter(l => l.language === filters.language);
  }

  const limit = Math.min(parseInt(filters.limit) || 50, MAX_LEADS);
  const offset = parseInt(filters.offset) || 0;

  return {
    leads: result.slice(offset, offset + limit),
    total: result.length,
    limit,
    offset,
  };
}

// ─── Stats ───────────────────────────────────────────────────────────────────

function getLeadStats() {
  resetStatsIfNeeded();

  // Convert Maps to sorted arrays
  const topGroups = [...stats.topGroups.entries()]
    .map(([groupId, count]) => ({ groupId, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10);

  const topKeywords = [...stats.topKeywords.entries()]
    .map(([keyword, count]) => ({ keyword, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 20);

  return {
    leadsToday: stats.leadsToday,
    leadsThisWeek: stats.leadsThisWeek,
    totalInMemory: leads.length,
    topGroups,
    topKeywords,
    hourlyDistribution: stats.hourlyDistribution,
    peakHour: stats.peakHour,
  };
}

// ─── Custom Keywords ─────────────────────────────────────────────────────────

function getCustomKeywords() {
  return {
    builtin: KEYWORDS,
    custom: customKeywords,
  };
}

function setCustomKeywords(newKeywords) {
  // newKeywords: { keywords: ["word1", "word2"], category: "party" }
  if (!newKeywords.category || !Array.isArray(newKeywords.keywords)) {
    throw new Error('category and keywords[] required');
  }

  const category = newKeywords.category.toLowerCase();
  if (!customKeywords[category]) {
    customKeywords[category] = [];
  }

  for (const kw of newKeywords.keywords) {
    const lower = kw.toLowerCase().trim();
    if (lower && !customKeywords[category].includes(lower)) {
      customKeywords[category].push(lower);
    }
  }

  saveJSON(customKeywordsFilePath, customKeywords);
  return customKeywords;
}

function removeCustomKeyword(keyword) {
  const lower = keyword.toLowerCase().trim();
  let removed = false;

  for (const category of Object.keys(customKeywords)) {
    const idx = customKeywords[category].indexOf(lower);
    if (idx !== -1) {
      customKeywords[category].splice(idx, 1);
      removed = true;
      // Clean up empty categories
      if (customKeywords[category].length === 0) {
        delete customKeywords[category];
      }
    }
  }

  if (removed) {
    saveJSON(customKeywordsFilePath, customKeywords);
  }
  return removed;
}

// ─── Update Lead Status ──────────────────────────────────────────────────────

function updateLeadStatus(leadId, status, actionNote) {
  const lead = leads.find(l => l.id === leadId);
  if (!lead) return null;

  const validStatuses = ['new', 'replied', 'added-to-list', 'dismissed'];
  if (!validStatuses.includes(status)) {
    throw new Error(`Invalid status. Must be one of: ${validStatuses.join(', ')}`);
  }

  lead.status = status;
  lead.actions.push({
    action: status,
    note: actionNote || null,
    at: new Date().toISOString(),
  });

  schedulePersist();
  return lead;
}

// ─── CSV Export ──────────────────────────────────────────────────────────────

function exportLeadsCsv(filters = {}) {
  const { leads: filtered } = getLeads({ ...filters, limit: MAX_LEADS });

  const escape = (s) => `"${(s || '').toString().replace(/"/g, '""')}"`;

  const header = 'id,timestamp,groupName,groupId,senderName,senderPhone,message,matchedKeywords,categories,language,confidence,status,account';
  const rows = filtered.map(l => [
    escape(l.id),
    escape(l.timestamp),
    escape(l.groupName),
    escape(l.groupId),
    escape(l.senderName),
    escape(l.senderPhone),
    escape(l.message),
    escape((l.matchedKeywords || []).join(';')),
    escape((l.categories || []).join(';')),
    escape(l.language),
    escape(l.confidence),
    escape(l.status),
    escape(l.account),
  ].join(','));

  return [header, ...rows].join('\n');
}

// ─── Init ────────────────────────────────────────────────────────────────────

function initLeads(dataDir) {
  DATA_DIR = dataDir || '.';
  leadsFilePath = path.join(DATA_DIR, 'leads.json');
  customKeywordsFilePath = path.join(DATA_DIR, 'custom-keywords.json');

  // Load persisted leads
  leads = loadJSON(leadsFilePath, []);
  if (leads.length > MAX_LEADS) leads.length = MAX_LEADS;
  console.log(`[leads] Loaded ${leads.length} persisted leads`);

  // Load custom keywords
  customKeywords = loadJSON(customKeywordsFilePath, {});
  console.log(`[leads] Loaded custom keywords:`, Object.keys(customKeywords).length, 'categories');

  // Initialize stats from today's persisted leads
  resetStatsIfNeeded();
  const todayStr = new Date().toISOString().slice(0, 10);
  const now = new Date();
  const weekAgo = new Date(now);
  weekAgo.setDate(weekAgo.getDate() - 7);

  for (const lead of leads) {
    const ts = lead.timestamp || '';
    if (ts.startsWith(todayStr)) {
      stats.leadsToday++;
      const hour = new Date(ts).getHours();
      stats.hourlyDistribution[hour]++;
    }
    if (new Date(ts) >= weekAgo) {
      stats.leadsThisWeek++;
    }
    if (lead.groupId) {
      stats.topGroups.set(lead.groupId, (stats.topGroups.get(lead.groupId) || 0) + 1);
    }
    for (const kw of (lead.matchedKeywords || [])) {
      stats.topKeywords.set(kw, (stats.topKeywords.get(kw) || 0) + 1);
    }
  }

  // Recalculate peak hour
  let maxCount = 0;
  for (let h = 0; h < 24; h++) {
    if (stats.hourlyDistribution[h] > maxCount) {
      maxCount = stats.hourlyDistribution[h];
      stats.peakHour = h;
    }
  }

  // Schedule midnight reset
  scheduleMidnightReset();

  console.log(`[leads] Initialized — ${stats.leadsToday} today, ${stats.leadsThisWeek} this week`);
}

function scheduleMidnightReset() {
  const now = new Date();
  const tomorrow = new Date(now);
  tomorrow.setDate(tomorrow.getDate() + 1);
  tomorrow.setHours(0, 0, 0, 0);
  const msUntilMidnight = tomorrow.getTime() - now.getTime();

  setTimeout(() => {
    resetStatsIfNeeded();
    scheduleMidnightReset(); // reschedule for next midnight
  }, msUntilMidnight);
}

// ─── Exports ─────────────────────────────────────────────────────────────────

module.exports = {
  detectLead,
  storeLead,
  getLeads,
  getLeadStats,
  getCustomKeywords,
  setCustomKeywords,
  removeCustomKeyword,
  updateLeadStatus,
  exportLeadsCsv,
  initLeads,
};
