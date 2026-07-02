/**
 * Darwin Train Data Backend — nationwide
 * ---------------------------------------------------------------------------
 * Consumes the National Rail Darwin Push Port (Kafka) firehose and serves live
 * departures + platform numbers for ANY Great Britain station via a REST API.
 *
 * The Push Port feed already carries every station in the country; this backend
 * used to discard all but four SE20 stations. It now resolves stations through a
 * bundled reference (stations-reference.json: TIPLOC <-> CRS <-> name <-> lat/lon,
 * derived from fasteroute/national-rail-stations, itself built from the Darwin
 * feeds) instead of hand-typed tables.
 *
 * Retention is LAZY so memory stays bounded despite the full-network firehose:
 * departures are stored only for stations that are "hot" — requested within the
 * last HOT_TTL_MS, plus the always-hot SEED_CRS (the SE20 home area). A keep-alive
 * ping to /health keeps the process (and the seed stations) warm on Render's free
 * tier so boards don't start empty.
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
    // Darwin Kafka connection. Credentials come from the environment ONLY (never
    // committed) — set KAFKA_USERNAME / KAFKA_PASSWORD in the Render dashboard.
    // Broker host and group id are not secrets, so they keep sensible defaults and
    // stay env-overridable (a throwaway group id lets the service run locally
    // without rebalancing partitions away from the production consumer).
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

// SE20 home-area stations, kept live permanently so they're always warm.
// (Real CRS codes from the reference — the old backend used made-up ANR/BKB.)
const SEED_CRS = ['PNW', 'PNE', 'ANZ', 'BIK'];

// A station stays "hot" (its departures retained + populated from the firehose)
// for this long after its last board request.
const HOT_TTL_MS = 30 * 60 * 1000;   // 30 minutes
const MAX_PER_STATION = 12;

// ============================================
// STATION REFERENCE (TIPLOC <-> CRS <-> name <-> lat/lon)
// ============================================
const refByTiploc = new Map();   // TIPLOC -> {tiploc, crs, name, lat, lon}
const refByCrs = new Map();      // CRS    -> {tiploc, crs, name, lat, lon}
const refCoords = [];            // records with valid coords, for nearest lookup

function loadReference() {
    try {
        const raw = fs.readFileSync(path.join(__dirname, 'stations-reference.json'), 'utf8');
        const arr = JSON.parse(raw);
        arr.forEach(e => {
            if (!e.tiploc || !e.crs) return;
            const rec = { tiploc: e.tiploc, crs: e.crs, name: e.name || e.crs, lat: +e.lat || 0, lon: +e.lon || 0 };
            refByTiploc.set(e.tiploc, rec);
            // First TIPLOC seen for a CRS becomes the canonical record for that CRS
            if (!refByCrs.has(e.crs)) refByCrs.set(e.crs, rec);
            // Nearest-match pool excludes London Underground / junction pseudo-stations
            // (CRS starting Z or X) which carry no National Rail departures and could
            // otherwise shadow a co-located real station.
            if (rec.lat && rec.lon && !/^[ZX]/.test(rec.crs)) refCoords.push(rec);
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

// Nearest station CRS to a lat/lon within maxKm
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
const departures = {};           // CRS -> [ departure, ... ]
const hot = new Map();           // CRS -> last-requested ms (Infinity = seed, never expires)
SEED_CRS.forEach(c => hot.set(c, Infinity));

function isHot(crs) {
    const t = hot.get(crs);
    return t !== undefined && (t === Infinity || (Date.now() - t) < HOT_TTL_MS);
}
function markHot(crs) { if (!SEED_CRS.includes(crs)) hot.set(crs, Date.now()); }

let lastUpdate = null;
let kafkaConnected = false;
let messageCount = 0;

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

/**
 * Process incoming Darwin messages. Push Port messages wrap the payload as a JSON
 * string inside a 'bytes' field.
 */
function processDarwinMessage(message) {
    try {
        const wrapper = JSON.parse(message.value.toString());
        messageCount++;
        if (!wrapper.bytes) return;
        const data = JSON.parse(wrapper.bytes);

        if (data.uR) {
            if (data.uR.TS) {
                const arr = Array.isArray(data.uR.TS) ? data.uR.TS : [data.uR.TS];
                arr.forEach(ts => processTrainStatus(ts));
            }
            if (data.uR.schedule) {
                const arr = Array.isArray(data.uR.schedule) ? data.uR.schedule : [data.uR.schedule];
                arr.forEach(s => processSchedule(s));
            }
        }
        lastUpdate = new Date();
    } catch (error) {
        // Silently ignore parse errors (firehose has occasional malformed frames)
    }
}

/**
 * Train Status (TS) — real-time running info (delays, platforms, cancellations).
 */
function processTrainStatus(ts) {
    if (!ts.Location) return;
    const locations = Array.isArray(ts.Location) ? ts.Location : [ts.Location];
    const valid = locations.filter(l => l && l.tpl);
    const finalDest = valid.length ? valid[valid.length - 1].tpl : null;

    locations.forEach(loc => {
        if (!loc || !loc.tpl) return;
        const crs = tiplocToCrs(loc.tpl);
        if (!crs || !isHot(crs)) return;   // only store stations someone is watching
        updateDeparture(crs, {
            rid: ts.rid, uid: ts.uid, ssd: ts.ssd, tpl: loc.tpl,
            pta: loc.pta, ptd: loc.ptd, wta: loc.wta, wtd: loc.wtd,
            arr: loc.arr, dep: loc.dep, plat: loc.plat,
            cancelled: loc.can === 'true',
            delayed: !!ts.LateReason, lateReason: ts.LateReason,
            destination: finalDest
        });
    });
}

/**
 * Schedule — timetable info: which trains call at which stations and when.
 */
function processSchedule(schedule) {
    if (!schedule.OR && !schedule.IP && !schedule.DT) return;
    const points = [
        ...(schedule.OR ? [schedule.OR] : []),
        ...(schedule.IP ? (Array.isArray(schedule.IP) ? schedule.IP : [schedule.IP]) : []),
        ...(schedule.DT ? [schedule.DT] : [])
    ];
    const destTpl = (schedule.DT && schedule.DT.tpl) || null;

    points.forEach(p => {
        if (!p || !p.tpl) return;
        const crs = tiplocToCrs(p.tpl);
        if (!crs || !isHot(crs)) return;
        updateDeparture(crs, {
            rid: schedule.rid, uid: schedule.uid, ssd: schedule.ssd, tpl: p.tpl,
            toc: schedule.toc, pta: p.pta, ptd: p.ptd, wta: p.wta, wtd: p.wtd,
            plat: p.plat, activity: p.act, destination: destTpl
        });
    });
}

/**
 * Insert/update one departure for a station, keeping the soonest MAX_PER_STATION.
 */
function updateDeparture(crs, trainData) {
    const list = departures[crs] || (departures[crs] = []);
    trainData.stationCode = crs;

    const timeStr = trainData.ptd || trainData.wtd;
    if (timeStr) trainData.mins = calculateMinutes(timeStr);

    const i = list.findIndex(d => d.rid === trainData.rid);
    if (i >= 0) list[i] = { ...list[i], ...trainData, updatedAt: Date.now() };
    else list.push({ ...trainData, createdAt: Date.now(), updatedAt: Date.now() });

    list.sort((a, b) => String(a.ptd || a.wtd || '99:99').localeCompare(String(b.ptd || b.wtd || '99:99')));
    departures[crs] = list
        .filter(d => d.mins === undefined || d.mins >= -2)
        .slice(0, MAX_PER_STATION);
}

/**
 * Minutes from now until an "HH:MM" time (24h). Returns undefined for non-times.
 */
function calculateMinutes(timeStr) {
    if (!timeStr || typeof timeStr !== 'string' || !timeStr.includes(':')) return undefined;
    const [hours, mins] = timeStr.split(':').map(Number);
    if (!Number.isFinite(hours) || !Number.isFinite(mins)) return undefined;
    const now = new Date();
    const target = new Date();
    target.setHours(hours, mins, 0, 0);
    // Times just after midnight belong to tomorrow
    if (target < now && hours < 6) target.setDate(target.getDate() + 1);
    return Math.round((target - now) / 60000);
}

/**
 * Periodic cleanup: recompute minutes, drop departed trains, evict cold stations.
 */
setInterval(() => {
    Object.keys(departures).forEach(crs => {
        if (!isHot(crs)) { delete departures[crs]; hot.delete(crs); return; }
        departures[crs].forEach(d => { const t = d.ptd || d.wtd; if (t) d.mins = calculateMinutes(t); });
        departures[crs] = departures[crs]
            .filter(d => d.mins === undefined || d.mins >= -2)
            .slice(0, MAX_PER_STATION);
        if (departures[crs].length === 0) delete departures[crs];
    });
    // Drop expired hot markers (non-seed) with no stored data
    hot.forEach((t, crs) => { if (t !== Infinity && (Date.now() - t) >= HOT_TTL_MS && !departures[crs]) hot.delete(crs); });
}, 60 * 1000);

async function startKafkaConsumer() {
    if (!CONFIG.kafka.sasl.username || !CONFIG.kafka.sasl.password) {
        console.error('KAFKA_USERNAME / KAFKA_PASSWORD not set — Darwin feed disabled. ' +
            'Set them in the Render environment for this service.');
        return;
    }
    try {
        console.log('Connecting to Darwin Kafka...');
        await consumer.connect();
        await consumer.subscribe({ topic: CONFIG.topic, fromBeginning: false });
        await consumer.run({
            eachMessage: async ({ message }) => { processDarwinMessage(message); }
        });
        kafkaConnected = true;
        console.log('Darwin Kafka consumer running!');
    } catch (error) {
        console.error('Failed to start Kafka consumer:', error.message);
        kafkaConnected = false;
        setTimeout(startKafkaConsumer, 30000);   // retry
    }
}

// ============================================
// FORMATTING
// ============================================
function formatDepartures(deps) {
    return (deps || []).map(d => {
        // Platform: Darwin sends a string, or an object like {platsrc,conf,"":"2"}
        let platform = '-';
        if (d.plat) {
            if (typeof d.plat === 'string') platform = d.plat;
            else if (typeof d.plat === 'object') {
                platform = d.plat[''] || d.plat.plat
                    || Object.values(d.plat).find(v => /^[0-9]+[A-Za-z]?$/.test(String(v))) || '-';
            }
        }
        return {
            destination: nameForTiploc(d.destination) || d.destination || 'Unknown',
            scheduledTime: d.ptd || d.wtd || '',
            expectedTime: typeof d.dep === 'object' ? (d.dep && (d.dep.at || d.dep['@t'])) : d.dep,
            platform,
            mins: d.mins,
            cancelled: d.cancelled || false,
            delayed: d.delayed || false,
            lateReason: d.lateReason
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

// Live board for ANY station — by ?crs=XXX or by ?lat=..&lon=.. (nearest station).
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
        timestamp: new Date(),
        lastUpdate,
        station: { crs, name: rec.name, lat: rec.lat, lon: rec.lon },
        warming: board.length === 0,   // just went hot — will fill from the feed shortly
        departures: formatDepartures(board)
    });
});

// Back-compat: the SE20 home-area board in the old shape.
app.get('/api/departures', (req, res) => {
    const stations = {};
    SEED_CRS.forEach(crs => {
        const rec = refByCrs.get(crs) || { name: crs };
        stations[crs] = { name: rec.name, line: '', departures: formatDepartures(departures[crs] || []) };
    });
    res.json({ timestamp: new Date(), lastUpdate, stations });
});

// Back-compat: single station by CRS (old path).
app.get('/api/departures/:station', (req, res) => {
    const crs = req.params.station.toUpperCase();
    if (!refByCrs.has(crs)) return res.status(404).json({ error: 'Station not found' });
    markHot(crs);
    const rec = refByCrs.get(crs);
    res.json({
        timestamp: new Date(), lastUpdate,
        station: { code: crs, name: rec.name, departures: formatDepartures(departures[crs] || []) }
    });
});

app.get('/api/status', (req, res) => {
    res.json({
        kafkaConnected, lastUpdate, messageCount,
        referenceLoaded: refByCrs.size,
        hotStations: Array.from(hot.keys()),
        tracked: Object.fromEntries(Object.keys(departures).map(c => [c, departures[c].length]))
    });
});

// ============================================
// START
// ============================================
const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
    console.log(`Darwin API server running on port ${PORT}`);
    console.log(`Seed (always-hot) stations: ${SEED_CRS.join(', ')}`);
    startKafkaConsumer();
});

process.on('SIGTERM', async () => {
    console.log('Shutting down...');
    try { await consumer.disconnect(); } catch (e) { /* ignore */ }
    process.exit(0);
});

module.exports = app;
