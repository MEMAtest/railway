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

const app = express();
app.use(cors());
app.use(express.json());

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
const SEED_CRS_SET = new Set(SEED_CRS);   // O(1) membership test

const HOT_TTL_MS = 30 * 60 * 1000;
const MAX_PER_STATION = 12;

// SE20 journey planning data
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
            // Exclude LU/junction pseudo-stations (Z/X CRS) and entries with invalid coords.
            // Use Number.isFinite to correctly handle lon=0 (Greenwich meridian).
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

let lastUpdate = null;
let kafkaConnected = false;
let messageCount = 0;

// ============================================
// TIME UTILITIES
// ============================================

// Darwin publishes times in UK local time (BST/GMT). Return minutes-since-midnight
// in UK local time so calculateMinutes is correct year-round on a UTC server.
function ukNowMins() {
    const parts = new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Europe/London', hour: 'numeric', minute: 'numeric', hour12: false
    }).formatToParts(new Date());
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
    // Clamp to ±12 h: resolves midnight ambiguity without the old `hours < 6` heuristic
    if (diff > 720) diff -= 1440;
    if (diff < -720) diff += 1440;
    return Math.round(diff);
}

// Extract the best available time from a Darwin dep field for eviction decisions.
// Prefers the live expected time (dep.at / dep['@t']) over the fixed scheduled time
// so delayed trains aren't evicted at their scheduled departure.
function evictionTime(d) {
    const dep = d.dep;
    if (dep && typeof dep === 'object') {
        const live = dep.at || dep['@t'];
        if (live && typeof live === 'string') return live;
    }
    if (typeof dep === 'string' && dep) return dep;
    return d.ptd || d.wtd || null;
}

// Shared platform-extraction logic (single source of truth for all endpoints).
function extractPlatform(plat) {
    if (!plat) return '-';
    if (typeof plat === 'string') return plat;
    if (typeof plat === 'object') {
        return plat[''] || plat.plat
            || Object.values(plat).find(v => /^[0-9]+[A-Za-z]?$/.test(String(v))) || '-';
    }
    return '-';
}

// Shared expected-time extraction — handles both dep.at and dep['@t'] Darwin aliases.
function extractExpectedTime(dep) {
    if (!dep) return undefined;
    if (typeof dep === 'object') return dep.at || dep['@t'] || undefined;
    return dep;
}

// UK local hour for a given Date (handles BST correctly on a UTC server).
function ukHourOf(date) {
    const parts = new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Europe/London', hour: 'numeric', hour12: false
    }).formatToParts(date);
    return parseInt(parts.find(p => p.type === 'hour').value);
}

// UK local weekday string ('Mon'–'Sun') for a given Date.
function ukWeekdayOf(date) {
    return new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Europe/London', weekday: 'short'
    }).format(date);
}

// ============================================
// HELPER FUNCTIONS
// ============================================

// Use UK local time for crowding comparisons — crowdingPatterns are UK-calibrated.
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
    return EXIT_POSITIONING[destination.toLowerCase()]?.[fromStation] || null;
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

const consumer = kafka.consumer({ groupId: CONFIG.kafka.groupId });

// Reset kafkaConnected and schedule a reconnect if the consumer crashes mid-stream.
consumer.on(consumer.events.CRASH, async (e) => {
    console.error('Kafka consumer crashed:', e.payload?.error?.message || e.payload?.error);
    kafkaConnected = false;
    try { await consumer.disconnect(); } catch (err) { /* ignore */ }
    setTimeout(startKafkaConsumer, 30000);
});

function processDarwinMessage(message) {
    try {
        const wrapper = JSON.parse(message.value.toString());
        messageCount++;
        if (!wrapper.bytes) return;
        const data = JSON.parse(wrapper.bytes);

        if (sampleMessages.length < 5) {
            sampleMessages.push({ keys: Object.keys(data), hasUR: !!data.uR, sample: JSON.stringify(data).substring(0, 500) });
        }

        if (data.uR) {
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
        }
        lastUpdate = new Date();
    } catch (e) { /* ignore malformed frames */ }
}

function processTrainStatus(ts) {
    if (!ts.Location) return;
    const locations = Array.isArray(ts.Location) ? ts.Location : [ts.Location];

    locations.forEach(loc => {
        if (!loc || !loc.tpl) return;
        recentStations.add(loc.tpl);
        const crs = tiplocToCrs(loc.tpl);
        if (!crs || !isHot(crs)) return;
        updateDeparture(crs, {
            rid: ts.rid, uid: ts.uid, ssd: ts.ssd, tpl: loc.tpl,
            pta: loc.pta, ptd: loc.ptd, wta: loc.wta, wtd: loc.wtd,
            arr: loc.arr, dep: loc.dep,
            // Only include plat when Darwin actually provided it; omitting it prevents
            // a TS update with no platform info from erasing a previously known platform.
            ...(loc.plat !== undefined ? { plat: loc.plat } : {}),
            // Darwin XSD declares `can` as xs:boolean; handle both string and boolean forms.
            cancelled: loc.can === 'true' || loc.can === true,
            delayed: !!ts.LateReason, lateReason: ts.LateReason
            // destination intentionally omitted: TS Location lists are partial (only changed
            // stops), so the last element is not reliably the train terminus. The authoritative
            // terminus comes from the schedule's DT node via processSchedule.
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
    // Only set destination when the DT (terminal) node is present; an IP-only
    // schedule update should not overwrite a known terminus with null.
    const destTpl = schedule.DT?.tpl || null;

    points.forEach(p => {
        if (!p || !p.tpl) return;
        recentStations.add(p.tpl);
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
        // Always update line status (it checks SE20 lines internally).
        updateLineStatusFromMessage(station.crs, ow.Msg, ow.Severity);
        // Only queue service messages for SE20 stations — the nationwide Darwin firehose
        // would otherwise fill the 20-slot ring with alerts from unrelated stations.
        if (!SE20_LINES[station.crs]) return;
        serviceMessages.push({ station: station.crs, message: ow.Msg, severity: ow.Severity, timestamp: new Date() });
        if (serviceMessages.length > 20) serviceMessages.shift();
    });
}

function updateDeparture(crs, trainData) {
    const list = departures[crs] || (departures[crs] = []);
    trainData.stationCode = crs;

    // Use the live expected departure time (dep.at) when available so that delayed
    // trains aren't evicted from the board at their scheduled departure time.
    const refTime = evictionTime(trainData);
    if (refTime) trainData.mins = calculateMinutes(refTime);

    const i = list.findIndex(d => d.rid === trainData.rid);
    if (i >= 0) {
        const merged = { ...list[i], ...trainData, updatedAt: Date.now() };
        // Preserve known destination and platform if the incoming update has neither —
        // partial TS updates shouldn't erase data written by a schedule message.
        if (merged.destination == null && list[i].destination != null) merged.destination = list[i].destination;
        if (merged.plat == null && list[i].plat != null) merged.plat = list[i].plat;
        list[i] = merged;
    } else {
        list.push({ ...trainData, createdAt: Date.now(), updatedAt: Date.now() });
    }

    // Sort numerically by calculated mins so midnight-spanning boards stay correct
    // (localeCompare on 'HH:MM' strings puts 00:xx before 23:xx, inverting order).
    list.sort((a, b) => (a.mins ?? 9999) - (b.mins ?? 9999));
    departures[crs] = list.filter(d => d.mins === undefined || d.mins >= -2).slice(0, MAX_PER_STATION);
}

setInterval(() => {
    Object.keys(departures).forEach(crs => {
        if (!isHot(crs)) { delete departures[crs]; hot.delete(crs); return; }
        // Refresh mins using the live expected time (same logic as updateDeparture).
        departures[crs].forEach(d => {
            const t = evictionTime(d);
            if (t) d.mins = calculateMinutes(t);
        });
        departures[crs] = departures[crs].filter(d => d.mins === undefined || d.mins >= -2).slice(0, MAX_PER_STATION);
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
    try {
        console.log('Connecting to Darwin Kafka...');
        await consumer.connect();
        await consumer.subscribe({ topic: CONFIG.topic, fromBeginning: false });
        await consumer.run({ eachMessage: async ({ message }) => { processDarwinMessage(message); } });
        kafkaConnected = true;
        console.log('Darwin Kafka consumer running!');
    } catch (error) {
        console.error('Failed to start Kafka consumer:', error.message);
        kafkaConnected = false;
        // Disconnect before retrying: calling connect() again on an already-connected
        // consumer causes a permanent "already running" error loop.
        try { await consumer.disconnect(); } catch (e) { /* ignore */ }
        setTimeout(startKafkaConsumer, 30000);
    }
}

// ============================================
// FORMATTING
// ============================================
function formatDepartures(deps) {
    return (deps || []).map(d => ({
        destination: nameForTiploc(d.destination) || d.destination || 'Unknown',
        scheduledTime: d.ptd || d.wtd || '',
        expectedTime: extractExpectedTime(d.dep),
        platform: extractPlatform(d.plat),
        mins: d.mins,
        cancelled: d.cancelled || false,
        delayed: d.delayed || false,
        lateReason: d.lateReason
    }));
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
        departures: formatDepartures(board)
    });
});

app.get('/api/departures', (req, res) => {
    const stations = {};
    SEED_CRS.forEach(crs => {
        const rec = refByCrs.get(crs) || { name: crs };
        stations[crs] = { name: rec.name, line: SE20_LINES[crs] || '', walkMins: SE20_WALK[crs] || null, departures: formatDepartures(departures[crs] || []) };
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
        station: { code: crs, name: rec.name, line: SE20_LINES[crs] || '', walkMins: SE20_WALK[crs] || null, departures: formatDepartures(departures[crs] || []) }
    });
});

// Journey planner — from SE20 stations only (SEED_CRS); nationwide destinations.
app.get('/api/journey/:destination', (req, res) => {
    const query = req.params.destination.toLowerCase().replace(/[^a-z0-9 ]/g, '');

    const matchingTiplocs = new Set();
    if (DESTINATIONS[query]) DESTINATIONS[query].forEach(t => matchingTiplocs.add(t));
    refByTiploc.forEach((rec, tiploc) => {
        if (rec.name.toLowerCase().includes(query)) matchingTiplocs.add(tiploc);
    });

    if (matchingTiplocs.size === 0) {
        return res.status(404).json({ error: 'Destination not found', hint: 'Try a partial name like "victoria", "london bridge"' });
    }

    const firstTiploc = matchingTiplocs.values().next().value;
    const matchedName = nameForTiploc(firstTiploc) || firstTiploc;

    const options = [];
    // Restrict to SE20 departure stations — non-SE20 hot stations don't have meaningful walk times.
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
                delayed: d.delayed || false, lateReason: d.lateReason
            });
        });
    });

    options.sort((a, b) => a.leaveInMins - b.leaveInMins);
    const status = options.length === 0 ? 'no_trains' : 'ok';
    res.json({ timestamp: new Date(), lastUpdate, destination: matchedName, status, options });
});

// Best option with scoring, crowding, and exit positioning — SE20 departure stations only.
app.get('/api/best/:destination', (req, res) => {
    const query = req.params.destination.toLowerCase().replace(/[^a-z0-9 ]/g, '');
    const speed = ['walk', 'brisk', 'run'].includes(req.query.speed) ? req.query.speed : 'walk';

    const matchingTiplocs = new Set();
    if (DESTINATIONS[query]) DESTINATIONS[query].forEach(t => matchingTiplocs.add(t));
    refByTiploc.forEach((rec, tiploc) => { if (rec.name.toLowerCase().includes(query)) matchingTiplocs.add(tiploc); });

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
            if (d.delayed) {
                // Guard: lateReason can be a non-string object in some Darwin frames
                const delayMatch = typeof d.lateReason === 'string' ? d.lateReason.match(/(\d+)\s*min/i) : null;
                score += delayMatch ? Math.min(parseInt(delayMatch[1]), 30) : 15;
            }
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
                delayed: d.delayed || false, lateReason: d.lateReason,
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
    // Use ?? to correctly handle buffer=0 (caller already at interchange).
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
    try { await consumer.disconnect(); } catch (e) { /* ignore */ }
    process.exit(0);
});

module.exports = app;
