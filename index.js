/**
 * Darwin Train Data Backend — nationwide
 * ---------------------------------------------------------------------------
 * Consumes the National Rail Darwin Push Port (Kafka) firehose and serves live
 * departures + platform numbers for ANY Great Britain station via a REST API.
 *
 * Retention is LAZY: departures stored only for "hot" stations (requested in
 * the last HOT_TTL_MS) plus always-hot SEED_CRS (SE20 home area). A Hetzner
 * cron pings /health every 10 min so Render never sleeps.
 */

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { Kafka } = require('kafkajs');
const webpush = require('web-push');

const app = express();
app.use(cors());
app.use(express.json());

// ============================================
// WEB PUSH (platform-change / cancellation alerts for a tracked train)
// VAPID keys are set in the Render dashboard, never committed. Without them,
// push is disabled and the endpoints degrade gracefully.
// ============================================
const VAPID_PUBLIC = (process.env.VAPID_PUBLIC_KEY || '').trim();
const VAPID_PRIVATE = (process.env.VAPID_PRIVATE_KEY || '').trim();
const VAPID_SUBJECT = (process.env.VAPID_SUBJECT || 'mailto:alerts@tremaine-159cf.web.app').trim();
const pushEnabled = !!(VAPID_PUBLIC && VAPID_PRIVATE);
if (pushEnabled) {
    try { webpush.setVapidDetails(VAPID_SUBJECT, VAPID_PUBLIC, VAPID_PRIVATE); }
    catch (e) { console.error('Invalid VAPID keys:', e.message); }
}
// endpoint -> { subscription, rid, crs, dest, scheduledTime, lastPlat, lastCancelled, createdAt }
const pushSubs = new Map();
const PUSH_TTL_MS = 6 * 60 * 60 * 1000;   // same-day services; drop stale subs after 6h

async function sendPush(sub, payload) {
    if (!pushEnabled) return;
    try {
        await webpush.sendNotification(sub.subscription, JSON.stringify(payload));
    } catch (e) {
        // 404/410 = subscription gone (unsubscribed / expired) — forget it.
        if (e.statusCode === 404 || e.statusCode === 410) pushSubs.delete(sub.subscription.endpoint);
    }
}

// Called from updateDeparture when a train updates: fire a push to anyone tracking
// this rid at this station if its platform was newly assigned/changed or it's cancelled.
function maybeNotifyPush(crs, merged) {
    if (!pushEnabled || pushSubs.size === 0 || !merged.rid) return;
    const plat = extractPlatform(merged.plat);
    const cancelled = !!merged.cancelled;
    for (const sub of pushSubs.values()) {
        if (sub.rid !== merged.rid || sub.crs !== crs) continue;
        if (plat && plat !== '-' && plat !== sub.lastPlat) {
            const changed = sub.lastPlat && sub.lastPlat !== '-';
            sendPush(sub, {
                title: changed ? `⚠️ Platform changed — ${sub.dest}` : `🚉 Platform ${plat} — ${sub.dest}`,
                body: changed
                    ? `Your ${sub.scheduledTime} to ${sub.dest} moved to Platform ${plat} (was ${sub.lastPlat}).`
                    : `Your ${sub.scheduledTime} to ${sub.dest} will depart from Platform ${plat}.`,
                tag: `plat-${sub.rid}`
            });
            sub.lastPlat = plat;
        }
        if (cancelled && !sub.lastCancelled) {
            sendPush(sub, {
                title: `❌ Cancelled — ${sub.dest}`,
                body: `Your ${sub.scheduledTime} to ${sub.dest} has been cancelled.`,
                tag: `cancel-${sub.rid}`
            });
            sub.lastCancelled = true;
        }
    }
}

setInterval(() => {
    const now = Date.now();
    for (const [ep, sub] of pushSubs) if (now - sub.createdAt > PUSH_TTL_MS) pushSubs.delete(ep);
}, 10 * 60 * 1000);

// ============================================
// CONFIGURATION
// ============================================
const CONFIG = {
    kafka: {
        brokers: (process.env.KAFKA_BROKERS || 'pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092').split(','),
        sasl: {
            mechanism: 'plain',
            username: process.env.KAFKA_USERNAME,
            password: process.env.KAFKA_PASSWORD
        },
        ssl: true,
        groupId: process.env.KAFKA_GROUP_ID || 'SC-989188c0-5e4a-49fd-98e6-767a0ba6a66c'
    },
    topic: 'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON'
};

const SEED_CRS = ['PNW', 'PNE', 'ANZ', 'BIK'];
const SEED_CRS_SET = new Set(SEED_CRS);

const HOT_TTL_MS = 30 * 60 * 1000;
const MAX_PER_STATION = 12;
// Entries with no timed departure (IP-only schedule points) are kept briefly
// in case a TS or schedule update arrives to fill in the time. After this TTL
// they are evicted, preventing them from crowding out real departures.
const GHOST_TTL_MS = 5 * 60 * 1000;

const SE20_LINES = {
    'PNW': 'Southern', 'PNE': 'Southeastern', 'ANZ': 'Overground', 'BIK': 'Tram'
};
const SE20_WALK = { 'PNW': 4, 'PNE': 5, 'ANZ': 11, 'BIK': 14 };
const SE20_TRAVEL = {
    'PNW': { walk: 4, brisk: 3, run: 2 },
    'PNE': { walk: 5, brisk: 4, run: 3 },
    'ANZ': { walk: 11, brisk: 9, run: 7 },
    'BIK': { walk: 14, brisk: 11, run: 8 }
};

const DESTINATIONS = {
    'victoria': ['VICTRIC', 'VICTRIA', 'VICTRIE', 'VICTRI'],
    'london bridge': ['LNDNBDE', 'LNDNBDG', 'LONBDGE', 'LONDONB'],
    'crystal palace': ['CRYSTLP', 'CRYSTPL', 'CRSTLPL'],
    'beckenham junction': ['BCKHMJN', 'BCKNHMJ', 'BCKJN'],
    'denmark hill': ['DNMKHL', 'DENMRKH'],
    'peckham rye': ['PCKHMRY', 'PCKMRYE']
};

const EXIT_POSITIONING = {
    'victoria': {
        'PNE': { carriage: 'front', exit: 'barriers', note: 'Front 2 carriages for main exit' },
        'PNW': { carriage: 'middle', exit: 'barriers', note: 'Middle carriages for main concourse' }
    },
    'london bridge': {
        'PNE': { carriage: 'rear', exit: 'jubilee', note: 'Rear for Jubilee line, front for Northern' },
        'PNW': { carriage: 'front', exit: 'northern', note: 'Front carriages for Northern line' },
        'ANZ': { carriage: 'middle', exit: 'barriers', note: 'Middle for main exit' }
    },
    'crystal palace': {
        'PNW': { carriage: 'any', exit: 'lift', note: 'Exit via lifts — position less important' },
        'ANZ': { carriage: 'any', exit: 'lift', note: 'Exit via lifts — position less important' }
    },
    'east croydon': {
        'PNW': { carriage: 'middle', exit: 'barriers', note: 'Middle for ticket barriers' },
        'ANZ': { carriage: 'front', exit: 'barriers', note: 'Front for main exit' }
    },
    'beckenham junction': {
        'PNE': { carriage: 'front', exit: 'tram', note: 'Front carriages for tram platforms' }
    },
    'denmark hill': {
        'ANZ': { carriage: 'rear', exit: 'thameslink', note: 'Rear for Thameslink platforms' }
    },
    'peckham rye': {
        'ANZ': { carriage: 'middle', exit: 'interchange', note: 'Middle for Southern/Southeastern interchange' }
    }
};

const crowdingPatterns = {
    'PNE': {
        peak: { weekday: [7, 8, 9, 17, 18, 19], weekend: [11, 12, 17, 18], level: 4 },
        moderate: { weekday: [6, 10, 16, 20], level: 2 },
        quiet: { level: 1 }
    },
    'PNW': {
        peak: { weekday: [7, 8, 9, 17, 18, 19], weekend: [11, 12, 13, 14, 15], level: 3 },
        moderate: { weekday: [6, 10, 16, 20], level: 2 },
        quiet: { level: 1 }
    },
    'ANZ': {
        peak: { weekday: [7, 8, 9, 17, 18], weekend: [12, 13, 17, 18], level: 3 },
        moderate: { weekday: [6, 10, 16, 19], level: 2 },
        quiet: { level: 1 }
    },
    'BIK': {
        peak: { weekday: [8, 9, 15, 16, 17, 18], level: 2 },
        quiet: { level: 1 }
    }
};

const CONNECTION_STATS = {
    'london bridge': {
        'jubilee': { walkMins: 6, successRate: { tight: 65, normal: 92 } },
        'northern': { walkMins: 4, successRate: { tight: 75, normal: 95 } },
        'elizabeth': { walkMins: 5, successRate: { tight: 70, normal: 93 } }
    },
    'victoria': {
        'district': { walkMins: 6, successRate: { tight: 60, normal: 88 } },
        'circle': { walkMins: 6, successRate: { tight: 60, normal: 88 } },
        'victoria_line': { walkMins: 7, successRate: { tight: 55, normal: 82 } }
    },
    'east croydon': {
        'platform_change': { walkMins: 4, successRate: { tight: 80, normal: 95 } },
        'thameslink': { walkMins: 3, successRate: { tight: 85, normal: 97 } }
    },
    'clapham junction': {
        'platform_change': { walkMins: 5, successRate: { tight: 70, normal: 90 } }
    },
    'crystal palace': {
        'bus': { walkMins: 2, successRate: { tight: 90, normal: 98 } }
    },
    'beckenham junction': {
        'tram': { walkMins: 3, successRate: { tight: 80, normal: 95 } }
    },
    'denmark hill': {
        'thameslink': { walkMins: 2, successRate: { tight: 85, normal: 97 } }
    },
    'peckham rye': {
        'southern': { walkMins: 2, successRate: { tight: 88, normal: 97 } },
        'southeastern': { walkMins: 2, successRate: { tight: 88, normal: 97 } }
    }
};

// ============================================
// STATION REFERENCE (TIPLOC <-> CRS <-> name <-> lat/lon)
// ============================================
const refByTiploc = new Map();
const refByCrs = new Map();
const refCoords = [];

function loadReference() {
    try {
        const raw = fs.readFileSync(path.join(__dirname, 'stations-reference.json'), 'utf8');
        const arr = JSON.parse(raw);
        arr.forEach(e => {
            if (!e.tiploc || !e.crs) return;
            const lat = +e.lat, lon = +e.lon;
            const rec = { tiploc: e.tiploc, crs: e.crs, name: e.name || e.crs, lat, lon };
            refByTiploc.set(e.tiploc, rec);
            if (!refByCrs.has(e.crs)) refByCrs.set(e.crs, rec);
            if (Number.isFinite(lat) && lat !== 0 && Number.isFinite(lon) && !/^[ZX]/.test(e.crs)) {
                refCoords.push(rec);
            }
        });
        console.log(`Loaded station reference: ${refByTiploc.size} TIPLOCs, ${refByCrs.size} CRS, ${refCoords.length} with coords`);
    } catch (e) {
        console.error('Failed to load stations-reference.json:', e.message);
    }
}
loadReference();

function tiplocToCrs(tiploc) { const r = refByTiploc.get(tiploc); return r ? r.crs : null; }
function nameForTiploc(tiploc) { const r = refByTiploc.get(tiploc); return r ? r.name : null; }

// Strip punctuation so "King's Cross" matches query "kings cross" (and vice versa).
function normaliseName(s) { return s.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim(); }

// Darwin passenger-facing reason codes (source: Open Rail wiki Darwin:Late_Running_reason_codes)
const LATE_REASONS = {
    '1':'a passenger accident at a station','2':'the late finish of a previous working',
    '3':'a delay in a previous station working','4':'a train equipment problem',
    '5':'an operational problem','6':'a train late from a maintenance depot',
    '7':'a train late from a stabling location','8':'a train late into a terminus',
    '9':'a joining delay','10':'an overcrowding delay','11':'a door-fault delay',
    '18':'a late-running freight train','19':'congestion caused by a late-running train',
    '21':'an operational problem at another station',
    '22':'a delay caused by a freight train','23':'an engineering works overrun',
    '24':'a track defect','25':'a points failure','26':'a signal failure',
    '27':'a level crossing problem','28':'a track circuit failure',
    '29':'a broken rail','30':'a bridge defect','31':'a lineside fire',
    '32':'an animal on the line','33':'a person on the line',
    '34':'a fatality on the line','35':'a vandalism or theft incident',
    '36':'a trespass incident','37':'a security alert',
    '38':'a police incident','39':'a medical emergency on a train',
    '40':'a medical emergency at a station','41':'a passenger taken ill',
    '42':'an unstaffed train','43':'a driver shortage','44':'a conductor shortage',
    '45':'a fleet shortfall','46':'rolling stock short-forming',
    '47':'a shortage of available trains','51':'overhead line damage',
    '52':'power supply problems','53':'a third rail problem',
    '54':'a cable fire','55':'cable theft',
    '101':'a train equipment problem','102':'a door-fault delay',
    '103':'a train fault','104':'a fleet maintenance problem',
    '105':'a rolling stock problem','106':'a train defect',
    '107':'a train crew issue','108':'a driver unavailability',
    '109':'a conductor unavailability','110':'a staff shortage',
    '111':'a train operating company operational problem',
    '112':'a Network Rail operational problem',
    '113':'an earlier trespassing incident','114':'an earlier fatality',
    '115':'an earlier vandalism incident','116':'overhead line problems',
    '117':'a power supply problem','118':'congestion on the network',
    '119':'a late-running connecting service','120':'a delay at another station',
    '121':'congestion causing knock-on delays','122':'station equipment failure',
    '123':'a lineside equipment failure','124':'a bus service failure',
    '125':'adverse weather conditions','126':'a track defect',
    '127':'animals on the line','128':'a fatality on the railway',
    '129':'a landslide','130':'rough shunting movements',
    '131':'an empty stock movement','132':'an earlier delay',
    '133':'an operational problem','134':'loss of electrical supply',
    '135':'congestion','136':'a passenger illness','137':'an earlier incident',
    '138':'a crew-related delay','139':'an Underground delay',
    '140':'industrial action','141':'a disruption at another station',
    '142':'an incident outside the station','143':'a security alert',
    '144':'a signalling problem','145':'a track circuit failure',
    '146':'a signal failure','147':'a points failure',
    '148':'a track defect','149':'a level crossing problem',
    '150':'a lineside fire','151':'a bridge strike',
    '152':'a level crossing breakdown','153':'a junction problem',
    '154':'a train derailment','155':'a train collision',
    '156':'engineering works overrunning','157':'planned engineering overrun',
    '158':'a track worker protection warning','159':'a fire at a station',
    '160':'a fire on a train','161':'a fire near the railway',
    '162':'a lineside fire','163':'an earlier fire','164':'a fire on the track',
    '165':'a person trespassing on the line','166':'a person hit by a train',
    '167':'a police incident','168':'a police request to stop',
    '169':'a suspicious package investigation','170':'a medical emergency',
    '171':'an emergency services incident','172':'poor weather conditions',
    '173':'high winds','174':'flooding','175':'snowfall',
    '176':'slippery rails','177':'an international service delay',
    '178':'a cross-Channel delay','179':'congestion',
    '180':'a road vehicle blocking the line','181':'a person on the line',
    '182':'animals on the line','183':'a previous delay'
};
const CANCEL_REASONS = {
    '1':'cancellation due to an operational problem',
    '2':'cancellation due to the late running of a previous service',
    '17':'a shortage of trains',
    '18':'a late-running freight train',
    '20':'a scheduling change',
    '21':'an operational problem at another station',
    '22':'a delay caused by a freight train',
    '31':'a lineside fire','32':'animals on the line','33':'a person on the line',
    '34':'a fatality on the line','36':'a trespass incident','37':'a security alert',
    '38':'a police incident','51':'overhead line damage','52':'power supply problems',
    '101':'a train equipment problem','107':'a train crew issue',
    '113':'an earlier trespassing incident','114':'an earlier fatality',
    '115':'an earlier vandalism incident','116':'overhead line problems',
    '118':'congestion on the network','121':'congestion causing knock-on delays',
    '125':'adverse weather conditions','128':'a fatality on the railway',
    '131':'an empty stock movement','140':'industrial action',
    '143':'a security alert','144':'a signalling problem',
    '145':'a track circuit failure','146':'a signal failure','147':'a points failure',
    '154':'a train derailment','155':'a train collision',
    '156':'engineering works overrunning','157':'planned engineering overrun',
    '160':'a fire on a train','165':'a person trespassing on the line',
    '166':'a person hit by a train','167':'a police incident',
    '172':'poor weather conditions','173':'high winds','174':'flooding',
    '176':'slippery rails','180':'a road vehicle blocking the line',
    '181':'a person on the line'
};

function resolveReasonCode(raw, table) {
    if (!raw) return null;
    // Darwin JSON encodes XML text content as the empty-string key when attributes are present
    // (same pattern extractPlatform already handles for plat objects).
    const code = (typeof raw === 'object') ? (raw[''] || raw['@value'] || raw['#text'] || '') : String(raw);
    return table[code.trim()] || null;
}

function haversineKm(a1, o1, a2, o2) {
    const R = 6371, toR = Math.PI / 180;
    const dLat = (a2 - a1) * toR, dLon = (o2 - o1) * toR;
    const s = Math.sin(dLat / 2) ** 2 + Math.cos(a1 * toR) * Math.cos(a2 * toR) * Math.sin(dLon / 2) ** 2;
    return 2 * R * Math.asin(Math.sqrt(s));
}

function nearestCrs(lat, lon, maxKm = 1.2) {
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
    let best = null, bestD = Infinity;
    for (const r of refCoords) {
        const d = haversineKm(lat, lon, r.lat, r.lon);
        if (d < bestD) { bestD = d; best = r; }
    }
    return best && bestD <= maxKm ? best.crs : null;
}

// ============================================
// IN-MEMORY STORE (lazy, per-CRS)
// ============================================
const departures = {};
const hot = new Map();
SEED_CRS.forEach(c => hot.set(c, Infinity));

function isHot(crs) {
    const t = hot.get(crs);
    return t !== undefined && (t === Infinity || (Date.now() - t) < HOT_TTL_MS);
}
function markHot(crs) { if (!SEED_CRS_SET.has(crs)) hot.set(crs, Date.now()); }

const lineStatus = {
    'Southern': { status: 'good', message: null, updatedAt: null },
    'Southeastern': { status: 'good', message: null, updatedAt: null },
    'Overground': { status: 'good', message: null, updatedAt: null },
    'Tram': { status: 'good', message: null, updatedAt: null }
};

const serviceMessages = [];
const recentStations = new Set();
const sampleMessages = [];
const msgTypeCounts = {};      // DIAGNOSTIC: tally of uR.* message types seen
let formationSample = null;    // DIAGNOSTIC: first formation/loading message captured

// rid -> terminus TIPLOC: populated by processSchedule DT nodes so that TS
// messages arriving before their schedule can still resolve their destination.
const ridToDestination = new Map();

// rid -> ordered calling pattern [{tpl, t, can}]: the full stop list from the
// schedule (OR + IP + DT), so a departure can show "does this train stop at X?".
const ridToCalling = new Map();

// `${rid}:${crs}` -> [{n, pct}] per-coach loading at that station, from Darwin
// formationLoading messages (TOC-dependent; ~a third of operators populate it).
const ridLoading = new Map();

let lastUpdate = null;
let kafkaConnected = false;
let messageCount = 0;

// ============================================
// CACHED INTL FORMATTERS
// ============================================
// Created once at module load — Intl.DateTimeFormat is expensive to construct
// and options never change. calculateMinutes() is called on every Kafka message
// and every 60-second cleanup tick, so per-call allocation adds up fast.
const _UK_CLOCK_FMT = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', hour: 'numeric', minute: 'numeric', hour12: false
});
// Single formatter for both hour and weekday, used by getCrowdingLevel + /api/crowding.
const _UK_HOUR_WEEKDAY_FMT = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', hour: 'numeric', weekday: 'short', hour12: false
});

// ============================================
// TIME UTILITIES
// ============================================

function ukNowMins() {
    const parts = _UK_CLOCK_FMT.formatToParts(new Date());
    const h = parseInt(parts.find(p => p.type === 'hour').value);
    const m = parseInt(parts.find(p => p.type === 'minute').value);
    return h * 60 + m;
}

// Minutes from now until an "HH:MM" Darwin time (UK local).
// Handles BST, midnight wrap, and delayed/early-morning trains correctly.
function calculateMinutes(timeStr) {
    if (!timeStr || typeof timeStr !== 'string' || !timeStr.includes(':')) return undefined;
    const [hours, mins] = timeStr.split(':').map(Number);
    if (!Number.isFinite(hours) || !Number.isFinite(mins)) return undefined;
    const nowMins = ukNowMins();
    const targetMins = hours * 60 + mins;
    let diff = targetMins - nowMins;
    if (diff > 720) diff -= 1440;
    if (diff < -720) diff += 1440;
    return Math.round(diff);
}

// Shared expected-time extraction — handles both dep.at and dep['@t'] Darwin aliases.
function extractExpectedTime(dep) {
    if (!dep) return undefined;
    if (typeof dep === 'object') return dep.at || dep['@t'] || undefined;
    return dep;
}

// Extract the best available time for eviction decisions.
// Reuses extractExpectedTime for the dep field (single source of truth for dep.at / dep['@t']).
function evictionTime(d) {
    const live = extractExpectedTime(d.dep);
    if (live && typeof live === 'string') return live;
    if (typeof d.dep === 'string' && d.dep) return d.dep;
    return d.ptd || d.wtd || null;
}

function extractPlatform(plat) {
    if (!plat) return '-';
    if (typeof plat === 'string') return plat;
    if (typeof plat === 'object') {
        return plat[''] || plat.plat
            || Object.values(plat).find(v => /^[0-9]+[A-Za-z]?$/.test(String(v))) || '-';
    }
    return '-';
}

// Both ukHourOf and ukWeekdayOf call the same cached formatter to avoid double allocation.
function ukHourOf(date) {
    const parts = _UK_HOUR_WEEKDAY_FMT.formatToParts(date);
    return parseInt(parts.find(p => p.type === 'hour').value);
}
function ukWeekdayOf(date) {
    return _UK_HOUR_WEEKDAY_FMT.formatToParts(date).find(p => p.type === 'weekday').value;
}

// ============================================
// HELPER FUNCTIONS
// ============================================

function getCrowdingLevel(stationCode, date = new Date()) {
    if (!date || !(date instanceof Date) || isNaN(date.getTime())) date = new Date();
    const patterns = crowdingPatterns[stationCode];
    if (!patterns) return { level: 1, label: 'unknown' };
    const hour = ukHourOf(date);
    const isWeekday = !['Sat', 'Sun'].includes(ukWeekdayOf(date));
    if (!isWeekday && patterns.peak?.weekend?.includes(hour)) {
        return { level: Math.max(2, patterns.peak.level - 1), label: 'moderate' };
    }
    if (isWeekday && patterns.peak?.weekday?.includes(hour)) return { level: patterns.peak.level, label: 'busy' };
    if (isWeekday && patterns.moderate?.weekday?.includes(hour)) return { level: patterns.moderate.level, label: 'moderate' };
    return { level: patterns.quiet?.level || 1, label: 'quiet' };
}

function getExitAdvice(destination, fromStation) {
    if (!destination || !fromStation) return null;
    const dn = destination.toLowerCase();
    // Board destinations are full names ("London Victoria") but the dataset is keyed
    // by short names ("victoria"), so match when the full name CONTAINS the key.
    // Only the full→key direction (not key→full), to avoid a short destination like
    // "Rye" falsely matching the key "peckham rye".
    for (const key of Object.keys(EXIT_POSITIONING)) {
        if (dn === key || dn.includes(key)) {
            const adv = EXIT_POSITIONING[key][fromStation];
            if (adv) return adv;
        }
    }
    return null;
}

function getConnectionRisk(destination, connectionLine, bufferMins) {
    const stats = CONNECTION_STATS[destination.toLowerCase()]?.[connectionLine];
    if (!stats) return null;
    if (typeof bufferMins !== 'number' || isNaN(bufferMins)) return { error: 'Invalid buffer time' };
    if (bufferMins < 0) {
        return { walkMins: stats.walkMins, bufferMins, isTight: true, successRate: 0, recommendation: 'Connection likely missed' };
    }
    if (bufferMins <= stats.walkMins) {
        return { walkMins: stats.walkMins, bufferMins, isTight: true, successRate: Math.max(0, stats.successRate.tight - 20), recommendation: 'Very tight — need to run' };
    }
    const isTight = bufferMins <= stats.walkMins + 2;
    return {
        walkMins: stats.walkMins, bufferMins, isTight,
        successRate: isTight ? stats.successRate.tight : stats.successRate.normal,
        recommendation: isTight ? 'Allow extra time or take earlier train' : 'Should make connection'
    };
}

function updateLineStatusFromMessage(crs, message, severity) {
    const line = SE20_LINES[crs];
    if (!line || !lineStatus[line]) return;
    const sev = typeof severity === 'number' ? severity : 10;
    const isDisrupted = sev <= 2 || /delay|cancel|disrupt|suspend|close|reduced|strike|industrial|divert|block|bus replacement/i.test(message);
    const isGood = /good service|normal service|service resumed|running normally/i.test(message);
    if (isDisrupted) lineStatus[line] = { status: 'disrupted', severity: sev, message, updatedAt: new Date() };
    else if (isGood) lineStatus[line] = { status: 'good', severity: 10, message: null, updatedAt: new Date() };
}

async function fetchOvergroundStatus() {
    try {
        const res = await fetch('https://api.tfl.gov.uk/Line/london-overground/Status');
        const data = await res.json();
        if (data?.[0]?.lineStatuses?.[0]) {
            const s = data[0].lineStatuses[0];
            const sev = s.statusSeverity;
            const good = sev === 10 || sev === 18 || sev === 19;
            lineStatus['Overground'] = {
                status: good ? 'good' : 'disrupted', severity: sev,
                message: good ? null : (s.reason || s.statusSeverityDescription),
                updatedAt: new Date()
            };
        }
    } catch (e) { /* TfL outage shouldn't break train boards */ }
}

// ============================================
// KAFKA CONSUMER
// ============================================
const kafka = new Kafka({
    clientId: 'penge-dash-consumer',
    brokers: CONFIG.kafka.brokers,
    ssl: true,
    sasl: {
        mechanism: CONFIG.kafka.sasl.mechanism,
        username: CONFIG.kafka.sasl.username,
        password: CONFIG.kafka.sasl.password
    },
    connectionTimeout: 10000,
    requestTimeout: 30000
});

// Reassigned on every (re)connect — KafkaJS crashed consumers cannot be reused.
let consumer = null;
// Stored so SIGTERM can cancel a pending reconnect timer.
let reconnectTimer = null;

// Central reconnect helper used by both the CRASH listener and the connect catch block.
function scheduleReconnect() {
    kafkaConnected = false;
    reconnectTimer = setTimeout(startKafkaConsumer, 30000);
}

function processDarwinMessage(message) {
    try {
        const wrapper = JSON.parse(message.value.toString());
        // Set kafkaConnected on the first confirmed message (consumer.run() resolves as
        // soon as the loop starts, not when messages actually flow — so we wait for proof).
        if (!kafkaConnected) kafkaConnected = true;
        messageCount++;
        if (!wrapper.bytes) return;
        const data = JSON.parse(wrapper.bytes);

        if (sampleMessages.length < 5) {
            sampleMessages.push({ keys: Object.keys(data), hasUR: !!data.uR, sample: JSON.stringify(data).substring(0, 500) });
        }

        if (data.uR) {
            // DIAGNOSTIC: tally uR.* message types so we can see whether formation /
            // loading data actually flows in this feed (for the coach-loading feature).
            for (const k of Object.keys(data.uR)) msgTypeCounts[k] = (msgTypeCounts[k] || 0) + 1;
            if (!formationSample && (data.uR.scheduleFormations || data.uR.formationLoading)) {
                formationSample = JSON.stringify(data.uR.scheduleFormations || data.uR.formationLoading).substring(0, 1200);
            }
            if (data.uR.TS) {
                const arr = Array.isArray(data.uR.TS) ? data.uR.TS : [data.uR.TS];
                arr.forEach(ts => processTrainStatus(ts));
            }
            if (data.uR.schedule) {
                const arr = Array.isArray(data.uR.schedule) ? data.uR.schedule : [data.uR.schedule];
                arr.forEach(s => processSchedule(s));
            }
            if (data.uR.OW) {
                const arr = Array.isArray(data.uR.OW) ? data.uR.OW : [data.uR.OW];
                arr.forEach(ow => processStationMessage(ow));
            }
            if (data.uR.formationLoading) {
                const arr = Array.isArray(data.uR.formationLoading) ? data.uR.formationLoading : [data.uR.formationLoading];
                arr.forEach(fl => processFormationLoading(fl));
            }
        }
        lastUpdate = new Date();
    } catch (e) { /* ignore malformed frames */ }
}

function processTrainStatus(ts) {
    if (!ts.Location) return;
    const locations = Array.isArray(ts.Location) ? ts.Location : [ts.Location];

    locations.forEach(loc => {
        if (!loc || !loc.tpl) return;
        if (recentStations.size < 15000) recentStations.add(loc.tpl);
        const crs = tiplocToCrs(loc.tpl);
        if (!crs || !isHot(crs)) return;
        updateDeparture(crs, {
            rid: ts.rid, uid: ts.uid, ssd: ts.ssd, tpl: loc.tpl,
            pta: loc.pta, ptd: loc.ptd, wta: loc.wta, wtd: loc.wtd,
            arr: loc.arr, dep: loc.dep,
            ...(loc.plat !== undefined ? { plat: loc.plat } : {}),
            cancelled: loc.can === 'true' || loc.can === true,
            delayed: !!ts.LateReason,
            lateReason: resolveReasonCode(ts.LateReason, LATE_REASONS),
            cancelReason: resolveReasonCode(ts.CancelReason, CANCEL_REASONS)
            // destination intentionally omitted: TS Location lists are partial (only changed
            // stops), so the last element is not reliably the train terminus. The authoritative
            // terminus comes from the schedule's DT node via processSchedule / ridToDestination.
        });
    });
}

function processSchedule(schedule) {
    if (!schedule.OR && !schedule.IP && !schedule.DT) return;
    const points = [
        ...(schedule.OR ? [schedule.OR] : []),
        ...(schedule.IP ? (Array.isArray(schedule.IP) ? schedule.IP : [schedule.IP]) : []),
        ...(schedule.DT ? [schedule.DT] : [])
    ];
    const destTpl = schedule.DT?.tpl || null;

    // Cache the terminus for this RID so TS-first arrivals can resolve their destination
    // before the schedule message propagates to updateDeparture.
    if (destTpl && schedule.rid) {
        ridToDestination.set(schedule.rid, destTpl);
        // Prevent unbounded growth. Half-clear (oldest 25k) instead of a full wipe so a
        // thundering herd of "Unknown" destinations doesn't appear after an overflow.
        if (ridToDestination.size > 50000) {
            const keys = [...ridToDestination.keys()];
            keys.slice(0, 25000).forEach(k => ridToDestination.delete(k));
        }
    }

    // Store the ordered calling pattern for this service (for "does it stop at X?").
    if (schedule.rid && points.length) {
        ridToCalling.set(schedule.rid, points.map(p => ({
            tpl: p.tpl,
            t: p.ptd || p.pta || p.wtd || p.wta || '',
            can: p.can === 'true' || p.can === true
        })));
        if (ridToCalling.size > 50000) {
            const keys = [...ridToCalling.keys()];
            keys.slice(0, 25000).forEach(k => ridToCalling.delete(k));
        }
    }

    points.forEach(p => {
        if (!p || !p.tpl) return;
        if (recentStations.size < 15000) recentStations.add(p.tpl);
        const crs = tiplocToCrs(p.tpl);
        if (!crs || !isHot(crs)) return;
        updateDeparture(crs, {
            rid: schedule.rid, uid: schedule.uid, ssd: schedule.ssd, tpl: p.tpl,
            toc: schedule.toc, pta: p.pta, ptd: p.ptd, wta: p.wta, wtd: p.wtd,
            ...(p.plat !== undefined ? { plat: p.plat } : {}),
            activity: p.act,
            ...(destTpl ? { destination: destTpl } : {})
        });
    });
}

function processStationMessage(ow) {
    if (!ow.Station) return;
    const stations = Array.isArray(ow.Station) ? ow.Station : [ow.Station];
    stations.forEach(station => {
        if (!station.crs) return;
        updateLineStatusFromMessage(station.crs, ow.Msg, ow.Severity);
        if (!SE20_LINES[station.crs]) return;
        serviceMessages.push({ station: station.crs, message: ow.Msg, severity: ow.Severity, timestamp: new Date() });
        if (serviceMessages.length > 20) serviceMessages.shift();
    });
}

// Darwin formationLoading: per-coach loading percentages for a service at one
// calling point. Stored keyed by rid+CRS so a board can show how busy each coach
// is at THAT station. The loading value is under the empty-string key (XML text).
function processFormationLoading(fl) {
    if (!fl || !fl.rid || !fl.tpl) return;
    const crs = tiplocToCrs(fl.tpl);
    if (!crs) return;
    const arr = Array.isArray(fl.loading) ? fl.loading : (fl.loading ? [fl.loading] : []);
    const coaches = arr.map(l => ({
        n: l.coachNumber,
        pct: parseInt(l[''] ?? l['#text'] ?? l.loadingValue, 10)
    })).filter(c => c.n != null && Number.isFinite(c.pct));
    if (!coaches.length) return;
    ridLoading.set(`${fl.rid}:${crs}`, coaches);
    if (ridLoading.size > 30000) {
        const keys = [...ridLoading.keys()];
        keys.slice(0, 15000).forEach(k => ridLoading.delete(k));
    }
}

function keepEntry(d) {
    if (d.mins !== undefined) return d.mins >= -2;
    // Entries with no timed departure (IP-only schedule points) are kept briefly
    // so a follow-up TS/schedule can fill in the time; evict after GHOST_TTL_MS.
    return (Date.now() - (d.createdAt || 0)) < GHOST_TTL_MS;
}

function updateDeparture(crs, trainData) {
    const list = departures[crs] || (departures[crs] = []);
    trainData.stationCode = crs;

    // Backfill destination from the rid cache when the schedule message hasn't arrived yet
    // (TS messages arrive before their schedule on consumer restart / service-day start).
    if (!trainData.destination && trainData.rid && ridToDestination.has(trainData.rid)) {
        trainData.destination = ridToDestination.get(trainData.rid);
    }

    const refTime = evictionTime(trainData);
    if (refTime) trainData.mins = calculateMinutes(refTime);

    const i = list.findIndex(d => d.rid === trainData.rid);
    if (i >= 0) {
        const merged = { ...list[i], ...trainData, updatedAt: Date.now() };
        if (merged.destination == null && list[i].destination != null) merged.destination = list[i].destination;
        if (merged.plat == null && list[i].plat != null) merged.plat = list[i].plat;
        // Also try the rid cache for entries that still lack a destination after the merge.
        if (merged.destination == null && ridToDestination.has(merged.rid)) {
            merged.destination = ridToDestination.get(merged.rid);
        }
        list[i] = merged;
        maybeNotifyPush(crs, merged);
    } else {
        list.push({ ...trainData, createdAt: Date.now(), updatedAt: Date.now() });
    }

    list.sort((a, b) => (a.mins ?? 9999) - (b.mins ?? 9999));
    departures[crs] = list.filter(keepEntry).slice(0, MAX_PER_STATION);
}

setInterval(() => {
    Object.keys(departures).forEach(crs => {
        if (!isHot(crs)) { delete departures[crs]; hot.delete(crs); return; }
        departures[crs].forEach(d => {
            const t = evictionTime(d);
            if (t) d.mins = calculateMinutes(t);
        });
        departures[crs] = departures[crs].filter(keepEntry).slice(0, MAX_PER_STATION);
        if (departures[crs].length === 0) delete departures[crs];
    });
    hot.forEach((t, crs) => { if (t !== Infinity && (Date.now() - t) >= HOT_TTL_MS && !departures[crs]) hot.delete(crs); });
}, 60 * 1000);

setInterval(fetchOvergroundStatus, 5 * 60 * 1000);

async function startKafkaConsumer() {
    if (!CONFIG.kafka.sasl.username || !CONFIG.kafka.sasl.password) {
        console.error('KAFKA_USERNAME / KAFKA_PASSWORD not set — Darwin feed disabled. Set them in the Render environment.');
        return;
    }
    // Create a fresh consumer on every (re)start. KafkaJS crashed consumers are in a
    // terminal state and calling connect() on them again fails silently or throws.
    if (consumer) {
        try { await consumer.disconnect(); } catch (e) { /* already dead */ }
    }
    consumer = kafka.consumer({ groupId: CONFIG.kafka.groupId });
    consumer.on(consumer.events.CRASH, (e) => {
        console.error('Kafka consumer crashed:', e.payload?.error?.message || e.payload?.error);
        scheduleReconnect();
    });
    try {
        console.log('Connecting to Darwin Kafka...');
        await consumer.connect();
        await consumer.subscribe({ topic: CONFIG.topic, fromBeginning: false });
        await consumer.run({ eachMessage: async ({ message }) => { processDarwinMessage(message); } });
        console.log('Darwin Kafka consumer loop started — waiting for first message.');
    } catch (error) {
        console.error('Failed to start Kafka consumer:', error.message);
        scheduleReconnect();
    }
}

// ============================================
// FORMATTING
// ============================================
function formatDepartures(deps, originCrs) {
    return (deps || []).map(d => {
        // Last-chance destination lookup: schedule messages may arrive after TS updates,
        // so the ridToDestination map may now have a TIPLOC that wasn't set at ingest time.
        const destTiploc = d.destination
            || (d.rid && ridToDestination.has(d.rid) ? ridToDestination.get(d.rid) : null);
        const destination = nameForTiploc(destTiploc) || null; // null = frontend drops the row
        const reason = d.lateReason || d.cancelReason || null;
        return {
            destination,
            scheduledTime: d.ptd || d.wtd || '',
            expectedTime: extractExpectedTime(d.dep),
            platform: extractPlatform(d.plat),
            mins: d.mins,
            cancelled: d.cancelled || false,
            delayed: d.delayed || false,
            reason,
            rid: d.rid || null,   // lets the client fetch this service's calling pattern
            // "Which carriage" boarding advice (curated), when we know the origin station.
            exitAdvice: (originCrs && destination) ? getExitAdvice(destination, originCrs) : null,
            // Per-coach loading at this station, when the operator publishes it.
            loading: (originCrs && d.rid && ridLoading.has(`${d.rid}:${originCrs}`)) ? ridLoading.get(`${d.rid}:${originCrs}`) : null
        };
    });
}

// ============================================
// REST API
// ============================================
app.get('/health', (req, res) => {
    res.json({
        status: 'ok', kafkaConnected, lastUpdate, messageCount,
        referenceLoaded: refByCrs.size, hotStations: hot.size, trackedStations: Object.keys(departures).length
    });
});

app.get('/api/board', (req, res) => {
    let crs = (req.query.crs || '').toString().trim().toUpperCase();
    if (!crs && req.query.lat != null && req.query.lon != null) {
        crs = nearestCrs(parseFloat(req.query.lat), parseFloat(req.query.lon)) || '';
    }
    if (!crs || !refByCrs.has(crs)) {
        return res.status(404).json({ error: 'Station not found', crs: crs || null });
    }
    markHot(crs);
    const rec = refByCrs.get(crs);
    const board = departures[crs] || [];
    res.json({
        timestamp: new Date(), lastUpdate,
        station: { crs, name: rec.name, lat: rec.lat, lon: rec.lon },
        warming: board.length === 0,
        departures: formatDepartures(board, crs)
    });
});

// ---- Web push: VAPID public key + subscribe/unsubscribe for a tracked train ----
app.get('/api/push/vapid', (req, res) => {
    res.json({ publicKey: VAPID_PUBLIC || null, enabled: pushEnabled });
});

app.post('/api/push/subscribe', (req, res) => {
    if (!pushEnabled) return res.status(503).json({ error: 'push not configured' });
    const { subscription, rid, crs, dest, scheduledTime, platform } = req.body || {};
    if (!subscription || !subscription.endpoint || !rid || !crs) {
        return res.status(400).json({ error: 'subscription, rid and crs are required' });
    }
    const CRS = String(crs).toUpperCase();
    markHot(CRS);   // keep the tracked station warm so we keep seeing its updates
    pushSubs.set(subscription.endpoint, {
        subscription, rid, crs: CRS, dest: dest || 'your train',
        scheduledTime: scheduledTime || '', lastPlat: platform || '-',
        lastCancelled: false, createdAt: Date.now()
    });
    res.json({ ok: true, tracking: { rid, crs: CRS } });
});

app.post('/api/push/unsubscribe', (req, res) => {
    const b = req.body || {};
    const ep = b.endpoint || (b.subscription && b.subscription.endpoint);
    if (ep) pushSubs.delete(ep);
    res.json({ ok: true });
});

// Calling pattern for a single service (rid), resolved to station names.
// Used by the client to answer "does this train stop at X?".
app.get('/api/service', (req, res) => {
    const rid = (req.query.rid || '').toString().trim();
    const pts = ridToCalling.get(rid);
    if (!pts || !pts.length) return res.json({ rid, callingPoints: [] });
    res.json({
        rid,
        callingPoints: pts.map(p => ({
            name: nameForTiploc(p.tpl) || p.tpl,
            crs: tiplocToCrs(p.tpl) || null,
            time: p.t,
            cancelled: !!p.can
        }))
    });
});

app.get('/api/departures', (req, res) => {
    const stations = {};
    SEED_CRS.forEach(crs => {
        const rec = refByCrs.get(crs) || { name: crs };
        stations[crs] = { name: rec.name, line: SE20_LINES[crs] || '', walkMins: SE20_WALK[crs] || null, departures: formatDepartures(departures[crs] || [], crs) };
    });
    res.json({ timestamp: new Date(), lastUpdate, stations });
});

app.get('/api/departures/:station', (req, res) => {
    const crs = req.params.station.toUpperCase();
    if (!refByCrs.has(crs)) return res.status(404).json({ error: 'Station not found' });
    markHot(crs);
    const rec = refByCrs.get(crs);
    res.json({
        timestamp: new Date(), lastUpdate,
        station: { code: crs, name: rec.name, line: SE20_LINES[crs] || '', walkMins: SE20_WALK[crs] || null, departures: formatDepartures(departures[crs] || [], crs) }
    });
});

// Journey planner — from SE20 stations only (SEED_CRS); nationwide destinations.
app.get('/api/journey/:destination', (req, res) => {
    const query = normaliseName(req.params.destination);

    const matchingTiplocs = new Set();
    if (DESTINATIONS[query]) DESTINATIONS[query].forEach(t => matchingTiplocs.add(t));
    // Normalise stored names the same way as the query so apostrophes (King's Cross)
    // and other punctuation don't break matching.
    refByTiploc.forEach((rec, tiploc) => {
        if (normaliseName(rec.name).includes(query)) matchingTiplocs.add(tiploc);
    });

    if (matchingTiplocs.size === 0) {
        return res.status(404).json({ error: 'Destination not found', hint: 'Try a partial name like "victoria", "london bridge"' });
    }

    const firstTiploc = matchingTiplocs.values().next().value;
    const matchedName = nameForTiploc(firstTiploc) || firstTiploc;

    const options = [];
    SEED_CRS.forEach(crsCode => {
        const deps = departures[crsCode];
        if (!deps) return;
        const walkMins = SE20_WALK[crsCode];
        const stationName = refByCrs.get(crsCode)?.name || crsCode;
        const line = SE20_LINES[crsCode];
        deps.forEach(d => {
            if (!matchingTiplocs.has(d.destination)) return;
            if (d.cancelled) return;
            const minsUntilDeparture = d.mins;
            if (minsUntilDeparture === undefined || minsUntilDeparture < -1) return;
            options.push({
                station: stationName, stationCode: crsCode, line, walkMins,
                scheduledTime: d.ptd || d.wtd || '',
                expectedTime: extractExpectedTime(d.dep),
                platform: extractPlatform(d.plat),
                leaveInMins: minsUntilDeparture - walkMins,
                delayed: d.delayed || false, reason: d.reason || d.lateReason || null
            });
        });
    });

    options.sort((a, b) => a.leaveInMins - b.leaveInMins);
    const status = options.length === 0 ? 'no_trains' : 'ok';
    res.json({ timestamp: new Date(), lastUpdate, destination: matchedName, status, options });
});

// Best option with scoring, crowding, and exit positioning — SE20 departure stations only.
app.get('/api/best/:destination', (req, res) => {
    const query = normaliseName(req.params.destination);
    const speed = ['walk', 'brisk', 'run'].includes(req.query.speed) ? req.query.speed : 'walk';

    const matchingTiplocs = new Set();
    if (DESTINATIONS[query]) DESTINATIONS[query].forEach(t => matchingTiplocs.add(t));
    refByTiploc.forEach((rec, tiploc) => { if (normaliseName(rec.name).includes(query)) matchingTiplocs.add(tiploc); });

    if (matchingTiplocs.size === 0) {
        return res.status(404).json({ error: 'Destination not found', hint: 'Try: victoria, london bridge, crystal palace' });
    }

    const firstTiploc = matchingTiplocs.values().next().value;
    const matchedName = nameForTiploc(firstTiploc) || firstTiploc;

    const options = [];
    SEED_CRS.forEach(crsCode => {
        const deps = departures[crsCode];
        if (!deps) return;
        const travelTimes = SE20_TRAVEL[crsCode];
        if (!travelTimes) return; // defensive: skip if station lacks travel data
        const travelMins = travelTimes[speed];
        const stationName = refByCrs.get(crsCode)?.name || crsCode;
        const line = SE20_LINES[crsCode];
        const lineState = lineStatus[line] || { status: 'good' };

        deps.forEach(d => {
            if (!matchingTiplocs.has(d.destination)) return;
            if (d.cancelled) return;
            const minsUntilDeparture = d.mins;
            if (minsUntilDeparture === undefined || minsUntilDeparture < -3) return;

            const leaveInMins = minsUntilDeparture - travelMins;
            let score = leaveInMins < 0 ? 1000 : leaveInMins;
            if (lineState.status === 'disrupted') {
                const sev = lineState.severity ?? 5;
                score += sev <= 5 ? 40 : (sev <= 8 ? 20 : 10);
            }
            if (d.delayed) score += 15;
            if (leaveInMins < 2 && leaveInMins >= 0) score -= 5;

            options.push({
                station: stationName, stationCode: crsCode, line,
                lineStatus: lineState.status, lineMessage: lineState.message,
                travelMins, travelTimes, speed,
                scheduledTime: d.ptd || d.wtd || '',
                expectedTime: extractExpectedTime(d.dep),
                platform: extractPlatform(d.plat),
                leaveInMins, catchable: leaveInMins >= 0,
                canCatch: { walk: minsUntilDeparture >= travelTimes.walk, brisk: minsUntilDeparture >= travelTimes.brisk, run: minsUntilDeparture >= travelTimes.run },
                delayed: d.delayed || false, reason: d.reason || d.lateReason || null,
                crowding: getCrowdingLevel(crsCode),
                exitAdvice: getExitAdvice(matchedName, crsCode),
                score
            });
        });
    });

    options.sort((a, b) => a.score - b.score);
    const best = options[0] || null;
    const catchableOptions = options.filter(o => o.catchable).slice(0, 5);
    const departureWindow = catchableOptions.length > 1 ? {
        leaveFrom: Math.max(0, Math.min(...catchableOptions.map(o => o.leaveInMins))),
        leaveTo: Math.max(...catchableOptions.map(o => o.leaveInMins)),
        trainCount: catchableOptions.length
    } : null;
    const disruption = best && lineStatus[best.line]?.status === 'disrupted' ? {
        line: best.line, message: lineStatus[best.line].message,
        alternative: options.find(o => o.line !== best.line && lineStatus[o.line]?.status !== 'disrupted') || null
    } : null;
    const quieterOption = best?.crowding?.level >= 3
        ? options.find(o => o !== best && o.catchable && o.crowding?.level < best.crowding.level) || null
        : null;
    const runOption = !best?.catchable
        ? options.find(o => o.canCatch?.run && !o.canCatch?.walk) || null
        : null;

    let status = 'ok';
    if (options.length === 0) status = 'no_trains';
    else if (catchableOptions.length === 0) status = runOption ? 'run_to_catch' : 'all_departed';

    res.json({ timestamp: new Date(), lastUpdate, destination: matchedName, speed, status, best, departureWindow, disruption, quieterOption, runOption, alternatives: options.slice(1, 4) });
});

app.get('/api/status/lines', (req, res) => {
    res.json({ timestamp: new Date(), lines: lineStatus });
});

app.get('/api/connection/:station/:line', (req, res) => {
    const station = req.params.station.replace(/-/g, ' ');
    const line = req.params.line.toLowerCase().replace(/-/g, '_');
    const bufferRaw = parseInt(req.query.buffer);
    const bufferMins = Number.isFinite(bufferRaw) ? bufferRaw : 6;
    const risk = getConnectionRisk(station, line, bufferMins);
    if (!risk) return res.status(404).json({ error: 'Connection not found', hint: 'Try: /api/connection/london-bridge/jubilee?buffer=5' });
    res.json({ timestamp: new Date(), station, connection: line, ...risk });
});

app.get('/api/crowding/:station', (req, res) => {
    const crs = req.params.station.toUpperCase();
    if (!crowdingPatterns[crs]) return res.status(404).json({ error: 'Station not found (crowding data is SE20 stations only)' });
    const now = new Date();
    const predictions = [];
    for (let i = 0; i < 6; i++) {
        const time = new Date(now.getTime() + i * 60 * 60 * 1000);
        const crowding = getCrowdingLevel(crs, time);
        const hour = ukHourOf(time);
        predictions.push({ hour, time: `${String(hour).padStart(2, '0')}:00`, ...crowding });
    }
    res.json({ timestamp: new Date(), station: refByCrs.get(crs)?.name || crs, current: getCrowdingLevel(crs), predictions });
});

app.get('/api/messages', (req, res) => {
    res.json({ timestamp: new Date(), messages: serviceMessages });
});

app.get('/api/status', (req, res) => {
    res.json({
        kafkaConnected, lastUpdate, messageCount,
        referenceLoaded: refByCrs.size,
        hotStations: Array.from(hot.keys()),
        tracked: Object.fromEntries(Object.keys(departures).map(c => [c, departures[c].length]))
    });
});

app.get('/api/debug/samples', (req, res) => {
    res.json({ messageCount, sampleMessages });
});

// DIAGNOSTIC: which uR.* message types are flowing + a captured formation sample.
// Tells us whether coach-loading data is available in this feed before we build on it.
app.get('/api/debug/types', (req, res) => {
    res.json({ messageCount, msgTypeCounts, formationSample });
});

app.get('/api/debug/stations', (req, res) => {
    const all = Array.from(recentStations).sort();
    const se20Related = all.filter(s => /png|peng|aner|birk|anr|pnw|pne|bik/i.test(s));
    res.json({ totalStationsSeen: all.length, se20Related, allStations: all.slice(0, 200), hotCRS: Array.from(hot.keys()) });
});

app.get('/api/debug/raw', (req, res) => {
    const rawData = {};
    Object.keys(departures).forEach(crs => {
        rawData[crs] = departures[crs].map(d => ({
            destination: d.destination, resolvedName: nameForTiploc(d.destination) || 'Unknown',
            scheduledTime: d.ptd || d.wtd, platform: d.plat, mins: d.mins
        }));
    });
    res.json({ rawData, hotStations: Array.from(hot.keys()), trackedCRS: Object.keys(departures) });
});

// ============================================
// START
// ============================================
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
    console.log(`Darwin API server running on port ${PORT}`);
    console.log(`Seed (always-hot) stations: ${SEED_CRS.join(', ')}`);
    fetchOvergroundStatus();
    startKafkaConsumer();
});

process.on('SIGTERM', async () => {
    console.log('Shutting down...');
    if (reconnectTimer) clearTimeout(reconnectTimer);
    try { if (consumer) await consumer.disconnect(); } catch (e) { /* ignore */ }
    process.exit(0);
});

module.exports = app;
