/**
 * Darwin Train Data Backend
 * Consumes National Rail Darwin Push Port (Kafka) and serves departure data via REST API
 */

const express = require('express');
const cors = require('cors');
const { Kafka } = require('kafkajs');

const app = express();
app.use(cors());
app.use(express.json());

// ============================================
// CONFIGURATION
// ============================================
const CONFIG = {
    // Darwin Kafka credentials
    kafka: {
        brokers: ['pkc-z3p1v0.europe-west2.gcp.confluent.cloud:9092'],
        sasl: {
            mechanism: 'plain',
            username: 'DX2TUIY2J7VV7JU4',
            password: 'cflto5Fc8uXfjuSq3ILrEGARa2IQtFQhp1mqBhrNWWAKCpuiRp0qGFv2imN3OwDg'
        },
        ssl: true,
        groupId: 'SC-989188c0-5e4a-49fd-98e6-767a0ba6a66c'
    },
    topic: 'prod-1010-Darwin-Train-Information-Push-Port-IIII2_0-JSON',

    // Travel times in minutes from home to each station
    // walk = normal pace, brisk = fast walk, run = light jog
    travelTimes: {
        'PNW': { walk: 4, brisk: 3, run: 2 },
        'PNE': { walk: 5, brisk: 4, run: 3 },
        'ANR': { walk: 11, brisk: 9, run: 7 },
        'BKB': { walk: 14, brisk: 11, run: 8 }
    },

    // Backwards compat - default to walk
    walkingTimes: {
        'PNW': 4,
        'PNE': 5,
        'ANR': 11,
        'BKB': 14
    },

    // Stations to monitor - both TIPLOC and CRS codes
    // TIPLOC codes are typically 7 chars, derived from station name
    stations: {
        // CRS codes
        'PNW': { name: 'Penge West', line: 'Southern', crs: 'PNW' },
        'PNE': { name: 'Penge East', line: 'Southeastern', crs: 'PNE' },
        'BKB': { name: 'Birkbeck', line: 'Tram', crs: 'BKB' },
        'ANR': { name: 'Anerley', line: 'Overground', crs: 'ANR' },
        // Actual TIPLOC codes from Darwin data
        'ANERLEY': { name: 'Anerley', line: 'Overground', crs: 'ANR' },
        'BKBY': { name: 'Birkbeck', line: 'Tram', crs: 'BKB' },
        'PNGEW': { name: 'Penge West', line: 'Southern', crs: 'PNW' },
        'PENGEWT': { name: 'Penge West', line: 'Southern', crs: 'PNW' },
        'PNGEE': { name: 'Penge East', line: 'Southeastern', crs: 'PNE' },
        'PENGEET': { name: 'Penge East', line: 'Southeastern', crs: 'PNE' }
    },

    // Map TIPLOC to CRS for storage
    // Based on actual Darwin data: ANERLEY, BKBY are the real codes
    toCRS: {
        // Actual Darwin TIPLOC codes
        'ANERLEY': 'ANR',
        'BKBY': 'BKB',  // Birkbeck actual TIPLOC
        'PNGEW': 'PNW',
        'PENGEWT': 'PNW',
        'PNGEE': 'PNE',
        'PENGEET': 'PNE',
        // CRS codes (fallback)
        'PNW': 'PNW', 'PNE': 'PNE', 'BKB': 'BKB', 'ANR': 'ANR'
    },

    // Station name lookup by TIPLOC
    stationNames: {
        'ANERLEY': 'Anerley',
        'BKBY': 'Birkbeck',
        'PNGEW': 'Penge West',
        'PENGEWT': 'Penge West',
        'PNGEE': 'Penge East',
        'PENGEET': 'Penge East',
        // Victoria variations
        'VICTRIC': 'Victoria',
        'VICTRIA': 'Victoria',
        'VICTRIE': 'Victoria',
        'VICTRI': 'Victoria',
        // London Bridge variations
        'LNDNBDE': 'London Bridge',
        'LNDNBDG': 'London Bridge',
        'LONBDGE': 'London Bridge',
        'LONDONB': 'London Bridge',
        // Charing Cross
        'CHRX': 'Charing Cross',
        'CHARING': 'Charing Cross',
        'CHARX': 'Charing Cross',
        // Cannon Street
        'CANNON': 'Cannon Street',
        'CANNS': 'Cannon Street',
        'CANONST': 'Cannon Street',
        // Orpington
        'ORPNGTN': 'Orpington',
        'ORPINTN': 'Orpington',
        'ORPINGT': 'Orpington',
        // Beckenham
        'BCKJN': 'Beckenham Jct',
        'BNKCHSX': 'Beckenham Jct',
        'BCKHMJN': 'Beckenham Jct',
        'BCKNHMJ': 'Beckenham Jct',
        'BECKHM': 'Beckenham Jct',
        // Wimbledon
        'WIMBLDN': 'Wimbledon',
        'WMBLEDN': 'Wimbledon',
        'WIMBLED': 'Wimbledon',
        // Crystal Palace
        'CRYSTLP': 'Crystal Palace',
        'CRYSTPL': 'Crystal Palace',
        'CRSTLPL': 'Crystal Palace',
        // West Croydon
        'WCROYDN': 'West Croydon',
        'WSTCROY': 'West Croydon',
        'WCRDON': 'West Croydon',
        // Other stations
        'HGHBYIS': 'Highbury & Islington',
        'HIGHBRY': 'Highbury & Islington',
        'CATFORD': 'Catford',
        'CTFD': 'Catford',
        'HAYES': 'Hayes',
        'HAYESRL': 'Hayes',
        'BROMLEY': 'Bromley South',
        'BRMLEYS': 'Bromley South',
        'BICKLEY': 'Bickley',
        'BICKLYJ': 'Bickley',
        'STNBS': 'St Johns',
        'STJOHNS': 'St Johns',
        'ELTHAM': 'Eltham',
        'ELTNHMR': 'Eltham',
        'GRWICH': 'Greenwich',
        'GREENW': 'Greenwich',
        'BLKHTH': 'Blackheath',
        'BLCKHTH': 'Blackheath',
        'LEWISHM': 'Lewisham',
        'LEWISHJ': 'Lewisham',
        'LADWELL': 'Ladywell',
        'CTFDBGE': 'Catford Bridge',
        'CATFDBR': 'Catford Bridge',
        'BELNGHM': 'Bellingham',
        'RAVPRKS': 'Ravensbourne',
        'SHORTLD': 'Shortlands',
        'BRMLYNS': 'Bromley North',
        'SNDRSTD': 'Sanderstead',
        'NRWD JN': 'Norwood Jct',
        'NRWDJN': 'Norwood Jct',
        'SYDENHM': 'Sydenham',
        'SYDENH': 'Sydenham',
        'FORHILL': 'Forest Hill',
        'FRSTHL': 'Forest Hill',
        'HONROPK': 'Honor Oak Park',
        'BROCKY': 'Brockley',
        'NEWXGTE': 'New Cross Gate',
        'NEWX': 'New Cross',
        'SURREYQ': 'Surrey Quays',
        'DENMARKH': 'Denmark Hill',
        'DNMKHL': 'Denmark Hill',
        'PCKHMRY': 'Peckham Rye',
        'PCKMRYE': 'Peckham Rye',
        // Southeastern via Penge East (feed-observed codes)
        'KENTHOS': 'Kent House',
        'KENTHSE': 'Kent House',
        'GRVPK': 'Grove Park',
        'GROVEPK': 'Grove Park',
        'WDULWCH': 'West Dulwich',
        'SYDENHH': 'Sydenham Hill',
        'SYDNHMH': 'Sydenham Hill',
        'HERNEHL': 'Herne Hill',
        'HERNEH': 'Herne Hill',
        'BRIX': 'Brixton',
        'BRIXTON': 'Brixton',
        'ELMERSE': 'Elmers End',
        'CLTHRPS': 'Cleethorpes',
        'NUNHEAD': 'Nunhead',
        'CROFTON': 'Crofton Park',
        'ECROYDN': 'East Croydon',
        'EASTCRY': 'East Croydon',
        // Overground destinations
        'HGHI': 'Highbury & Islington',
        'HIGHBIS': 'Highbury & Islington',
        'HIGHBYI': 'Highbury & Islington',
        'WCROYDO': 'West Croydon',
        'WESTCRO': 'West Croydon',
        'WSTCRDN': 'West Croydon',
        // More London terminals
        'EUSTON': 'Euston',
        'EUSTNMS': 'Euston',
        'STPX': 'St Pancras',
        'STPANCI': 'St Pancras',
        'STPANCR': 'St Pancras',
        'KNGX': 'Kings Cross',
        'KGXMSLS': 'Kings Cross',
        'KNGSCRS': 'Kings Cross',
        'LIVST': 'Liverpool Street',
        'LIVSTLL': 'Liverpool Street',
        'LVRPLST': 'Liverpool Street',
        'WATRLMN': 'Waterloo',
        'WATERLM': 'Waterloo',
        'WATERLE': 'Waterloo',
        'PADTON': 'Paddington',
        'PADTONL': 'Paddington',
        'PADDNTN': 'Paddington',
        'FENCHST': 'Fenchurch Street',
        'FENCHRC': 'Fenchurch Street',
        'MRGT': 'Moorgate',
        'MOORGAT': 'Moorgate',
        // South London
        'CLPHMJN': 'Clapham Junction',
        'CLPHMJC': 'Clapham Junction',
        'CLAPHMJ': 'Clapham Junction',
        'BATRSEA': 'Battersea Park',
        'BATSPK': 'Battersea Park',
        'PCKHMQS': 'Peckham Queens Road',
        'QNSRDPK': 'Queens Road Peckham',
        'BERMSEY': 'Bermondsey',
        'CWATERJ': 'Canada Water',
        'CANWATE': 'Canada Water',
        'SURQYS': 'Surrey Quays',
        'SUREYQY': 'Surrey Quays',
        'ROTHRTH': 'Rotherhithe',
        'WAPING': 'Wapping',
        'WAPPING': 'Wapping',
        'SHADWEL': 'Shadwell',
        'WHTCHPL': 'Whitechapel',
        'WHTECHP': 'Whitechapel',
        'SHOREDH': 'Shoreditch High Street',
        'SHRDHST': 'Shoreditch High Street',
        'HOXTN': 'Hoxton',
        'HOXTON': 'Hoxton',
        'HGGRSTN': 'Haggerston',
        'DALSTNK': 'Dalston Kingsland',
        'DALSKNG': 'Dalston Kingsland',
        'DALSTJN': 'Dalston Junction',
        'DALSJN': 'Dalston Junction',
        'CNNONBY': 'Canonbury',
        'CANONBY': 'Canonbury',
        // Croydon area
        'NRWDJCT': 'Norwood Junction',
        'CRDONCS': 'Croydon Central',
        'STHCROY': 'South Croydon',
        'PURLEY': 'Purley',
        'PURLEYO': 'Purley Oaks',
        'SANDRST': 'Sanderstead',
        'RIDDLSD': 'Riddlesdown',
        'UPPERWA': 'Upper Warlingham',
        'WARLNGH': 'Warlingham',
        'WHYTELF': 'Whyteleafe',
        'CATHAMS': 'Caterham',
        // Tram destinations
        'ELMERSD': 'Elmers End',
        'ELMERSE': 'Elmers End',
        'BECKROD': 'Beckenham Road',
        'BCKROAD': 'Beckenham Road',
        'AVENUE': 'Avenue Road',
        'AVENURD': 'Avenue Road',
        'WOODSID': 'Woodside',
        'BLKHRSE': 'Blackhorse Lane',
        'ADDSCMB': 'Addiscombe',
        'ADDISCO': 'Addiscombe',
        'LLOYD': 'Lloyd Park',
        'LLYDPRK': 'Lloyd Park',
        'COOMBE': 'Coombe Lane',
        'GRNGWOD': 'Gravel Hill',
        'ADNGTNV': 'Addington Village',
        'KING HY': 'King Henrys Drive',
        'NEWADNG': 'New Addington',
        // Additional from debug
        'THBDGS': 'Theobalds Grove',
        'GRVRBGJ': 'Grove Park',
        'GROVEPK': 'Grove Park',
        'GRVPKJN': 'Grove Park',
        'GRAVSND': 'Gravesend',
        'GRVSEND': 'Gravesend',
        'DARTFRD': 'Dartford',
        'DARTFD': 'Dartford',
        'GILLGM': 'Gillingham',
        'GILNGHM': 'Gillingham',
        'RAINHM': 'Rainham',
        'SLADE': 'Slade Green',
        'SLDEGRN': 'Slade Green',
        'ELTHAMW': 'Eltham Well Hall',
        'KIDBRKE': 'Kidbrooke',
        'CHARLTN': 'Charlton',
        'WOLWCHA': 'Woolwich Arsenal',
        'WOLWCHD': 'Woolwich Dockyard',
        'ABYWOOD': 'Abbey Wood',
        'ABBEYW': 'Abbey Wood',
        'BELVDER': 'Belvedere',
        'ERITH': 'Erith',
        'BARNHRS': 'Barnehurst',
        'BEXLEYH': 'Bexleyheath',
        'WELLING': 'Welling',
        'FALCONW': 'Falconwood'
    }
};

// ============================================
// IN-MEMORY STORE FOR DEPARTURES
// ============================================
const departures = {
    PNW: [],
    PNE: [],
    BKB: [],
    ANR: []
};

// Store service messages (delays, cancellations)
const serviceMessages = [];

// Track line disruptions
const lineStatus = {
    'Southern': { status: 'good', message: null, updatedAt: null },
    'Southeastern': { status: 'good', message: null, updatedAt: null },
    'Overground': { status: 'good', message: null, updatedAt: null },
    'Tram': { status: 'good', message: null, updatedAt: null }
};

// Common destinations with their TIPLOC codes
const DESTINATIONS = {
    'victoria': ['VICTRIC', 'VICTRIA', 'VICTRIE', 'VICTRI'],
    'london bridge': ['LNDNBDE', 'LNDNBDG', 'LONBDGE', 'LONDONB'],
    'crystal palace': ['CRYSTLP', 'CRYSTPL', 'CRSTLPL'],
    'beckenham junction': ['BCKHMJN', 'BCKNHMJ', 'BCKJN'],
    'denmark hill': ['DNMKHL', 'DENMRKH'],
    'peckham rye': ['PCKHMRY', 'PCKMRYE']
};

// Platform exit positioning advice
// Which carriage to board for quickest exit at destination
const EXIT_POSITIONING = {
    'victoria': {
        'PNE': { carriage: 'front', exit: 'barriers', note: 'Front 2 carriages for main exit' },
        'PNW': { carriage: 'middle', exit: 'barriers', note: 'Middle carriages for main concourse' }
    },
    'london bridge': {
        'PNE': { carriage: 'rear', exit: 'jubilee', note: 'Rear for Jubilee line, front for Northern' },
        'PNW': { carriage: 'front', exit: 'northern', note: 'Front carriages for Northern line' },
        'ANR': { carriage: 'middle', exit: 'barriers', note: 'Middle for main exit' }
    },
    'crystal palace': {
        'PNW': { carriage: 'any', exit: 'lift', note: 'Exit via lifts to concourse above - position less important' },
        'ANR': { carriage: 'any', exit: 'lift', note: 'Exit via lifts to concourse above - position less important' }
    },
    'east croydon': {
        'PNW': { carriage: 'middle', exit: 'barriers', note: 'Middle for ticket barriers' },
        'ANR': { carriage: 'front', exit: 'barriers', note: 'Front for main exit' }
    },
    'beckenham junction': {
        'PNE': { carriage: 'front', exit: 'tram', note: 'Front carriages for tram platforms' }
    },
    'denmark hill': {
        'ANR': { carriage: 'rear', exit: 'thameslink', note: 'Rear for Thameslink platforms' }
    },
    'peckham rye': {
        'ANR': { carriage: 'middle', exit: 'interchange', note: 'Middle for Southern/Southeastern interchange' }
    }
};

// Crowding patterns by hour and day (0=Sun, 1=Mon, etc.)
// Scale: 1=quiet, 2=moderate, 3=busy, 4=packed
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
    'ANR': {
        peak: { weekday: [7, 8, 9, 17, 18], weekend: [12, 13, 17, 18], level: 3 },
        moderate: { weekday: [6, 10, 16, 19], level: 2 },
        quiet: { level: 1 }
    },
    'BKB': {
        peak: { weekday: [8, 9, 15, 16, 17, 18], level: 2 },
        quiet: { level: 1 }
    }
};

// Connection success rates at interchange stations
// Walk times are realistic worst-case; success rates based on typical experience
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

// Debug: store recent station codes seen
const recentStations = new Set();
const sampleMessages = [];

// Last update timestamp
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
 * Process incoming Darwin messages
 * Darwin Push Port messages have train data inside a 'bytes' field as JSON string
 */
function processDarwinMessage(message) {
    try {
        const wrapper = JSON.parse(message.value.toString());
        messageCount++;

        // The actual data is inside the 'bytes' field as a JSON string
        if (!wrapper.bytes) return;

        const data = JSON.parse(wrapper.bytes);

        // Store sample messages for debugging (keep last 5)
        if (sampleMessages.length < 5) {
            sampleMessages.push({
                keys: Object.keys(data),
                hasUR: !!data.uR,
                hasTS: data.uR ? !!data.uR.TS : false,
                sample: JSON.stringify(data).substring(0, 800)
            });
        }

        // Process uR (update) messages
        if (data.uR) {
            // Handle Train Status updates
            if (data.uR.TS) {
                const tsArray = Array.isArray(data.uR.TS) ? data.uR.TS : [data.uR.TS];
                tsArray.forEach(ts => processTrainStatus(ts));
            }

            // Handle schedule updates
            if (data.uR.schedule) {
                const schedArray = Array.isArray(data.uR.schedule) ? data.uR.schedule : [data.uR.schedule];
                schedArray.forEach(s => processSchedule(s));
            }

            // Handle station messages (OW)
            if (data.uR.OW) {
                const owArray = Array.isArray(data.uR.OW) ? data.uR.OW : [data.uR.OW];
                owArray.forEach(ow => processStationMessage(ow));
            }
        }

        lastUpdate = new Date();
    } catch (error) {
        // Silently ignore parse errors
    }
}

/**
 * Process Train Status (TS) messages
 * Contains real-time running information
 */
function processTrainStatus(ts) {
    if (!ts.LateReason && !ts.Location) return;

    // Extract locations this train is calling at
    const locations = Array.isArray(ts.Location) ? ts.Location : [ts.Location];

    // Find the final destination (last location in the array)
    const validLocations = locations.filter(l => l && l.tpl);
    const finalDestination = validLocations.length > 0 ? validLocations[validLocations.length - 1].tpl : null;

    locations.forEach(loc => {
        if (!loc) return;

        const stationCode = loc.tpl; // TIPLOC code

        // Track all stations for debugging
        if (stationCode) {
            recentStations.add(stationCode);
        }

        // Check if this is one of our monitored stations (by TIPLOC or CRS)
        const crsCode = CONFIG.toCRS[stationCode];
        if (crsCode) {
            updateDeparture(stationCode, {
                rid: ts.rid, // Unique train ID
                uid: ts.uid,
                ssd: ts.ssd, // Scheduled start date
                tpl: stationCode,
                pta: loc.pta, // Public time of arrival
                ptd: loc.ptd, // Public time of departure
                wta: loc.wta, // Working time of arrival
                wtd: loc.wtd, // Working time of departure
                arr: loc.arr, // Actual/estimated arrival
                dep: loc.dep, // Actual/estimated departure
                plat: loc.plat, // Platform
                cancelled: loc.can === 'true',
                delayed: ts.LateReason ? true : false,
                lateReason: ts.LateReason,
                destination: finalDestination // Add final destination from TS message
            });
        }
    });
}

/**
 * Process Schedule messages
 * Contains timetable information for trains
 */
function processSchedule(schedule) {
    if (!schedule.OR && !schedule.IP && !schedule.DT) return;

    // Get all calling points
    const callingPoints = [
        ...(schedule.OR ? [schedule.OR] : []),
        ...(schedule.IP ? (Array.isArray(schedule.IP) ? schedule.IP : [schedule.IP]) : []),
        ...(schedule.DT ? [schedule.DT] : [])
    ];

    callingPoints.forEach(point => {
        if (!point || !point.tpl) return;

        const stationCode = point.tpl;

        // Track all stations for debugging
        if (stationCode) {
            recentStations.add(stationCode);
        }

        // Check if this is one of our monitored stations
        const crsCode = CONFIG.toCRS[stationCode];
        if (crsCode) {
            updateDeparture(stationCode, {
                rid: schedule.rid,
                uid: schedule.uid,
                ssd: schedule.ssd,
                tpl: stationCode,
                trainId: schedule.trainId,
                toc: schedule.toc, // Train operating company
                pta: point.pta,
                ptd: point.ptd,
                wta: point.wta,
                wtd: point.wtd,
                plat: point.plat,
                activity: point.act,
                destination: getDestination(schedule)
            });
        }
    });
}

/**
 * Get final destination from schedule
 */
function getDestination(schedule) {
    if (schedule.DT && schedule.DT.tpl) {
        return schedule.DT.tpl;
    }
    return null;
}

/**
 * Process station messages (alerts, announcements)
 */
function processStationMessage(ow) {
    if (!ow.Station) return;

    const stations = Array.isArray(ow.Station) ? ow.Station : [ow.Station];

    stations.forEach(station => {
        if (CONFIG.stations[station.crs]) {
            serviceMessages.push({
                station: station.crs,
                message: ow.Msg,
                severity: ow.Severity,
                timestamp: new Date()
            });

            // Update line status based on message
            updateLineStatusFromMessage(station.crs, ow.Msg, ow.Severity);

            // Keep only last 20 messages
            if (serviceMessages.length > 20) {
                serviceMessages.shift();
            }
        }
    });
}

/**
 * Update departure information for a station
 */
function updateDeparture(stationCode, trainData) {
    // Convert TIPLOC to CRS for storage
    const crsCode = CONFIG.toCRS[stationCode] || stationCode;
    const stationDepartures = departures[crsCode];
    if (!stationDepartures) return;

    // Add station info
    trainData.stationCode = crsCode;
    trainData.stationName = CONFIG.stations[stationCode]?.name || crsCode;

    // Find existing entry for this train
    const existingIndex = stationDepartures.findIndex(d => d.rid === trainData.rid);

    // Calculate minutes until departure
    const departureTime = trainData.ptd || trainData.wtd || trainData.dep;
    if (departureTime) {
        trainData.mins = calculateMinutes(departureTime);
    }

    if (existingIndex >= 0) {
        // Update existing
        stationDepartures[existingIndex] = {
            ...stationDepartures[existingIndex],
            ...trainData,
            updatedAt: new Date()
        };
    } else {
        // Add new
        stationDepartures.push({
            ...trainData,
            createdAt: new Date(),
            updatedAt: new Date()
        });
    }

    // Sort by departure time and keep only next 10
    stationDepartures.sort((a, b) => {
        const timeA = a.ptd || a.wtd || '99:99';
        const timeB = b.ptd || b.wtd || '99:99';
        return timeA.localeCompare(timeB);
    });

    // Remove past departures and keep only next 10
    const now = new Date();
    departures[crsCode] = stationDepartures
        .filter(d => {
            if (d.mins !== undefined && d.mins < -2) return false; // Already departed
            return true;
        })
        .slice(0, 10);
}

/**
 * Calculate minutes from now until given time (HH:MM or HH:MM:SS format)
 */
function calculateMinutes(timeStr) {
    if (!timeStr) return null;

    const parts = timeStr.split(':').map(Number);
    const hours = parts[0];
    const mins = parts[1];
    const now = new Date();
    const target = new Date();
    target.setHours(hours, mins, 0, 0);

    // Handle times that appear to be in the past - likely tomorrow's train
    const diffMs = target - now;
    const diffHours = diffMs / (1000 * 60 * 60);

    if (diffHours < -12) {
        target.setDate(target.getDate() + 1);
    }

    return Math.round((target - now) / 60000);
}

/**
 * Get crowding prediction for a station at a given time
 */
function getCrowdingLevel(stationCode, date = new Date()) {
    // Validate date input
    if (!date || !(date instanceof Date) || isNaN(date.getTime())) {
        date = new Date();
    }

    const patterns = crowdingPatterns[stationCode];
    if (!patterns) return { level: 1, label: 'unknown' };

    const hour = date.getHours();
    const day = date.getDay();
    const isWeekday = day >= 1 && day <= 5;
    const isWeekend = !isWeekday;

    // Check weekend patterns first
    if (isWeekend && patterns.peak?.weekend?.includes(hour)) {
        return { level: Math.max(2, patterns.peak.level - 1), label: 'moderate' };
    }

    if (isWeekday && patterns.peak?.weekday?.includes(hour)) {
        return { level: patterns.peak.level, label: 'busy' };
    }
    if (isWeekday && patterns.moderate?.weekday?.includes(hour)) {
        return { level: patterns.moderate.level, label: 'moderate' };
    }
    return { level: patterns.quiet?.level || 1, label: 'quiet' };
}

/**
 * Get connection risk for an interchange
 */
function getConnectionRisk(destination, connectionLine, bufferMins) {
    const destKey = destination.toLowerCase();
    const stats = CONNECTION_STATS[destKey]?.[connectionLine];
    if (!stats) return null;

    // Validate bufferMins
    if (typeof bufferMins !== 'number' || isNaN(bufferMins)) {
        return { error: 'Invalid buffer time' };
    }

    // Handle negative buffer (already missed)
    if (bufferMins < 0) {
        return {
            walkMins: stats.walkMins,
            bufferMins,
            isTight: true,
            successRate: 0,
            recommendation: 'Connection likely missed - negative buffer'
        };
    }

    // Handle zero/very tight buffer
    if (bufferMins <= stats.walkMins) {
        return {
            walkMins: stats.walkMins,
            bufferMins,
            isTight: true,
            successRate: Math.max(0, stats.successRate.tight - 20),
            recommendation: 'Very tight - need to run'
        };
    }

    const isTight = bufferMins <= stats.walkMins + 2;
    return {
        walkMins: stats.walkMins,
        bufferMins,
        isTight,
        successRate: isTight ? stats.successRate.tight : stats.successRate.normal,
        recommendation: isTight ? 'Allow extra time or take earlier train' : 'Should make connection'
    };
}

/**
 * Get exit positioning advice
 */
function getExitAdvice(destination, fromStation) {
    const destKey = destination.toLowerCase();
    return EXIT_POSITIONING[destKey]?.[fromStation] || null;
}

/**
 * Fetch TfL Overground status
 */
async function fetchOvergroundStatus() {
    try {
        const response = await fetch('https://api.tfl.gov.uk/Line/london-overground/Status');
        const data = await response.json();

        if (data && data[0] && data[0].lineStatuses && data[0].lineStatuses[0]) {
            const status = data[0].lineStatuses[0];
            const sev = status.statusSeverity;
            const isGood = sev === 10 || sev === 18 || sev === 19; // Good Service, No Issues, Information

            lineStatus['Overground'] = {
                status: isGood ? 'good' : 'disrupted',
                severity: sev,
                message: isGood ? null : status.reason || status.statusSeverityDescription,
                updatedAt: new Date()
            };
        }
    } catch (error) {
        console.error('Failed to fetch TfL status:', error.message);
    }
}

/**
 * Update line status from Darwin messages
 */
function updateLineStatusFromMessage(station, message, severity) {
    const stationInfo = CONFIG.stations[station];
    if (!stationInfo) return;

    const line = stationInfo.line;
    if (!lineStatus[line]) return;

    const sev = typeof severity === 'number' ? severity : 10;

    // Check for disruption patterns
    const isDisrupted = sev <= 2 ||
        /delay|cancel|disrupt|suspend|close|reduced|strike|industrial|divert|block|bus replacement/i.test(message);

    // Check for good service patterns
    const isGoodService = /good service|normal service|service resumed|running normally/i.test(message);

    if (isDisrupted) {
        lineStatus[line] = {
            status: 'disrupted',
            severity: sev,
            message: message,
            updatedAt: new Date()
        };
    } else if (isGoodService) {
        lineStatus[line] = {
            status: 'good',
            severity: 10,
            message: null,
            updatedAt: new Date()
        };
    }
}

/**
 * Start Kafka consumer
 */
async function startKafkaConsumer() {
    try {
        console.log('Connecting to Darwin Kafka...');
        await consumer.connect();
        console.log('Connected! Subscribing to topic...');

        await consumer.subscribe({
            topic: CONFIG.topic,
            fromBeginning: false
        });

        console.log('Subscribed! Starting consumer...');

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                processDarwinMessage(message);
            }
        });

        kafkaConnected = true;
        console.log('Darwin Kafka consumer running!');

    } catch (error) {
        console.error('Failed to start Kafka consumer:', error.message);
        kafkaConnected = false;

        // Retry after 30 seconds
        setTimeout(startKafkaConsumer, 30000);
    }
}

// ============================================
// REST API ENDPOINTS
// ============================================

// Health check
app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        kafkaConnected,
        lastUpdate,
        messageCount,
        stationsMonitored: Object.keys(CONFIG.stations)
    });
});

// Get all departures for all stations
app.get('/api/departures', (req, res) => {
    const response = {};

    Object.keys(departures).forEach(station => {
        response[station] = {
            name: CONFIG.stations[station].name,
            line: CONFIG.stations[station].line,
            walkMins: CONFIG.walkingTimes[station] || null,
            departures: formatDepartures(departures[station], station)
        };
    });

    res.json({
        timestamp: new Date(),
        lastUpdate,
        stations: response
    });
});

// Get departures for a specific station
app.get('/api/departures/:station', (req, res) => {
    const station = req.params.station.toUpperCase();
    const crsCode = CONFIG.toCRS[station] || station;

    if (!CONFIG.stations[station] || !departures[crsCode]) {
        return res.status(404).json({ error: 'Station not found' });
    }

    res.json({
        timestamp: new Date(),
        lastUpdate,
        station: {
            code: crsCode,
            name: CONFIG.stations[station].name,
            line: CONFIG.stations[station].line,
            walkMins: CONFIG.walkingTimes[crsCode] || null,
            departures: formatDepartures(departures[crsCode], crsCode)
        }
    });
});

// Journey planner - find trains to a destination with leave-by times
app.get('/api/journey/:destination', (req, res) => {
    const query = req.params.destination.toLowerCase().replace(/[^a-z0-9 ]/g, '');

    // Build a reverse lookup: destination name -> TIPLOC codes
    const matchingTiplocs = new Set();
    Object.entries(CONFIG.stationNames).forEach(([tiploc, name]) => {
        if (name.toLowerCase().includes(query)) {
            matchingTiplocs.add(tiploc);
        }
    });

    if (matchingTiplocs.size === 0) {
        return res.status(404).json({
            error: 'Destination not found',
            hint: 'Try a partial name like "victoria", "london bridge", "crystal palace"'
        });
    }

    const firstTiploc = matchingTiplocs.values().next().value;
    const matchedName = CONFIG.stationNames[firstTiploc] || firstTiploc;

    // Search all stations for trains heading to this destination
    const options = [];
    Object.entries(departures).forEach(([crsCode, deps]) => {
        const walkMins = CONFIG.walkingTimes[crsCode] || 5;
        const stationInfo = CONFIG.stations[crsCode];

        deps.forEach(d => {
            if (!matchingTiplocs.has(d.destination)) return;
            if (d.cancelled) return;

            const scheduledTime = d.ptd || d.wtd;
            if (!scheduledTime) return;

            const minsUntilDeparture = calculateMinutes(scheduledTime);
            if (minsUntilDeparture === null || minsUntilDeparture < -1) return;

            const leaveInMins = minsUntilDeparture - walkMins;

            // Extract platform
            let platform = '-';
            if (d.plat) {
                if (typeof d.plat === 'string') {
                    platform = d.plat;
                } else if (typeof d.plat === 'object') {
                    platform = d.plat[''] || d.plat.plat ||
                        Object.values(d.plat).find(v => typeof v === 'string' && /^[0-9]+[A-Za-z]?$|^[A-Za-z]$/.test(v)) || '-';
                }
            }

            options.push({
                station: stationInfo?.name || crsCode,
                stationCode: crsCode,
                line: stationInfo?.line || '-',
                walkMins,
                scheduledTime,
                expectedTime: typeof d.dep === 'object' ? d.dep?.at : d.dep,
                platform,
                leaveInMins,
                delayed: d.delayed || false,
                lateReason: d.lateReason
            });
        });
    });

    // Sort by leaveInMins so the most urgent option is first
    options.sort((a, b) => a.leaveInMins - b.leaveInMins);

    res.json({
        timestamp: new Date(),
        lastUpdate,
        destination: matchedName,
        options
    });
});

// Best Option Now - smart recommendation considering disruptions
// Query params: ?speed=walk|brisk|run (default: walk)
app.get('/api/best/:destination', (req, res) => {
    const query = req.params.destination.toLowerCase().replace(/[^a-z0-9 ]/g, '');
    const speed = ['walk', 'brisk', 'run'].includes(req.query.speed) ? req.query.speed : 'walk';

    // Use predefined destinations or search
    let matchingTiplocs = new Set();
    if (DESTINATIONS[query]) {
        DESTINATIONS[query].forEach(t => matchingTiplocs.add(t));
    } else {
        Object.entries(CONFIG.stationNames).forEach(([tiploc, name]) => {
            if (name.toLowerCase().includes(query)) {
                matchingTiplocs.add(tiploc);
            }
        });
    }

    if (matchingTiplocs.size === 0) {
        return res.status(404).json({
            error: 'Destination not found',
            hint: 'Try: victoria, london bridge, crystal palace'
        });
    }

    const firstTiploc = matchingTiplocs.values().next().value;
    const matchedName = CONFIG.stationNames[firstTiploc] || firstTiploc;

    // Gather all options with scoring
    const options = [];
    Object.entries(departures).forEach(([crsCode, deps]) => {
        // Get travel time based on speed (walk/brisk/run)
        const travelTimes = CONFIG.travelTimes[crsCode] || { walk: 5, brisk: 4, run: 3 };
        const travelMins = travelTimes[speed] || travelTimes.walk;
        const stationInfo = CONFIG.stations[crsCode];
        const line = stationInfo?.line;
        const lineState = lineStatus[line] || { status: 'good' };

        deps.forEach(d => {
            if (!matchingTiplocs.has(d.destination)) return;
            if (d.cancelled) return;

            const scheduledTime = d.ptd || d.wtd;
            if (!scheduledTime) return;

            const minsUntilDeparture = calculateMinutes(scheduledTime);
            if (minsUntilDeparture === null) return;

            // Allow trains up to 3 mins past scheduled (platform delays happen)
            if (minsUntilDeparture < -3) return;

            const leaveInMins = minsUntilDeparture - travelMins;

            // Extract platform
            let platform = '-';
            if (d.plat) {
                if (typeof d.plat === 'string') {
                    platform = d.plat;
                } else if (typeof d.plat === 'object') {
                    platform = d.plat[''] || d.plat.plat ||
                        Object.values(d.plat).find(v => typeof v === 'string' && /^[0-9]+[A-Za-z]?$|^[A-Za-z]$/.test(v)) || '-';
                }
            }

            // Score: lower is better
            // Base score is minutes until you need to leave (negative = already late)
            let score = leaveInMins < 0 ? 1000 : leaveInMins;

            // Penalties - scaled by severity
            if (lineState.status === 'disrupted') {
                // TfL severity: 1-5 = severe (40 penalty), 6-8 = moderate (20), 9 = minor (10)
                const sev = lineState.severity ?? 5;
                const disruptionPenalty = sev <= 5 ? 40 : (sev <= 8 ? 20 : 10);
                score += disruptionPenalty;
            }
            // Scale delay penalty - try to extract minutes from reason
            if (d.delayed) {
                const delayMatch = d.lateReason?.match(/(\d+)\s*min/i);
                score += delayMatch ? Math.min(parseInt(delayMatch[1]), 30) : 15;
            }
            if (leaveInMins < 2 && leaveInMins >= 0) score -= 5; // Slight bonus for "leave now" options

            // Get crowding prediction
            const crowding = getCrowdingLevel(crsCode);

            // Get exit positioning advice
            const exitAdvice = getExitAdvice(matchedName, crsCode);

            // Calculate if catchable at different speeds
            const canCatchWalking = minsUntilDeparture - travelTimes.walk >= 0;
            const canCatchBrisk = minsUntilDeparture - travelTimes.brisk >= 0;
            const canCatchRunning = minsUntilDeparture - travelTimes.run >= 0;

            options.push({
                station: stationInfo?.name || crsCode,
                stationCode: crsCode,
                line: line || '-',
                lineStatus: lineState.status,
                lineMessage: lineState.message,
                travelMins,
                travelTimes,
                speed,
                scheduledTime,
                expectedTime: typeof d.dep === 'object' ? d.dep?.at : d.dep,
                platform,
                leaveInMins,
                catchable: leaveInMins >= 0,
                canCatch: { walk: canCatchWalking, brisk: canCatchBrisk, run: canCatchRunning },
                delayed: d.delayed || false,
                lateReason: d.lateReason,
                crowding,
                exitAdvice,
                score
            });
        });
    });

    // Sort by score (best first)
    options.sort((a, b) => a.score - b.score);

    // Best option
    const best = options[0] || null;

    // Calculate departure window (range of leave times for next few options)
    const catchableOptions = options.filter(o => o.catchable).slice(0, 5);
    const departureWindow = catchableOptions.length > 1 ? {
        leaveFrom: Math.max(0, Math.min(...catchableOptions.map(o => o.leaveInMins))),
        leaveTo: Math.max(...catchableOptions.map(o => o.leaveInMins)),
        trainCount: catchableOptions.length
    } : null;

    // Check for disruption affecting best route
    const disruption = best && lineStatus[best.line]?.status === 'disrupted' ? {
        line: best.line,
        message: lineStatus[best.line].message,
        alternative: options.find(o => o.line !== best.line && lineStatus[o.line]?.status !== 'disrupted')
    } : null;

    // Find quieter alternative if best option is busy
    const quieterOption = best?.crowding?.level >= 3
        ? options.find(o => o !== best && o.catchable && o.crowding?.level < best.crowding.level)
        : null;

    // Find option catchable if running but not walking
    const runOption = !best?.catchable
        ? options.find(o => o.canCatch?.run && !o.canCatch?.walk)
        : null;

    // Status message for edge cases
    let status = 'ok';
    if (options.length === 0) {
        status = 'no_trains';
    } else if (catchableOptions.length === 0) {
        status = runOption ? 'run_to_catch' : 'all_departed';
    }

    res.json({
        timestamp: new Date(),
        lastUpdate,
        destination: matchedName,
        speed,
        status,
        best,
        departureWindow,
        disruption,
        quieterOption,
        runOption,
        alternatives: options.slice(1, 4) // Next 3 options
    });
});

// Line status endpoint
app.get('/api/status/lines', (req, res) => {
    res.json({
        timestamp: new Date(),
        lines: lineStatus
    });
});

// Connection risk calculator
// e.g., /api/connection/london-bridge/jubilee?buffer=5
app.get('/api/connection/:station/:line', (req, res) => {
    const station = req.params.station.replace(/-/g, ' ');
    const line = req.params.line.toLowerCase().replace(/-/g, '_');
    const bufferMins = parseInt(req.query.buffer) || 6;

    const risk = getConnectionRisk(station, line, bufferMins);

    if (!risk) {
        return res.status(404).json({
            error: 'Connection not found',
            hint: 'Try: /api/connection/london-bridge/jubilee?buffer=5'
        });
    }

    res.json({
        timestamp: new Date(),
        station,
        connection: line,
        ...risk
    });
});

// Crowding prediction endpoint
app.get('/api/crowding/:station', (req, res) => {
    const station = req.params.station.toUpperCase();
    const crsCode = CONFIG.toCRS[station] || station;

    if (!crowdingPatterns[crsCode]) {
        return res.status(404).json({ error: 'Station not found' });
    }

    // Get predictions for next few hours
    const now = new Date();
    const predictions = [];
    for (let i = 0; i < 6; i++) {
        const time = new Date(now.getTime() + i * 60 * 60 * 1000);
        const crowding = getCrowdingLevel(crsCode, time);
        predictions.push({
            hour: time.getHours(),
            time: `${time.getHours().toString().padStart(2, '0')}:00`,
            ...crowding
        });
    }

    res.json({
        timestamp: new Date(),
        station: CONFIG.stations[crsCode]?.name || crsCode,
        current: getCrowdingLevel(crsCode),
        predictions
    });
});

// Get service messages/alerts
app.get('/api/messages', (req, res) => {
    res.json({
        timestamp: new Date(),
        messages: serviceMessages
    });
});

// Get status (for debugging)
app.get('/api/status', (req, res) => {
    res.json({
        kafkaConnected,
        lastUpdate,
        messageCount,
        departuresCount: {
            PNW: departures.PNW.length,
            PNE: departures.PNE.length,
            BKB: departures.BKB.length,
            ANR: departures.ANR.length
        }
    });
});

// Debug endpoint - see sample messages
app.get('/api/debug/samples', (req, res) => {
    res.json({
        messageCount,
        sampleMessages
    });
});

// Debug endpoint - see what stations are coming through
app.get('/api/debug/stations', (req, res) => {
    // Filter for stations that might be Penge-related
    const allStations = Array.from(recentStations).sort();
    const pengeRelated = allStations.filter(s =>
        s.toLowerCase().includes('png') ||
        s.toLowerCase().includes('peng') ||
        s.toLowerCase().includes('aner') ||
        s.toLowerCase().includes('birk') ||
        s.toLowerCase().includes('anr') ||
        s.toLowerCase().includes('pnw') ||
        s.toLowerCase().includes('pne') ||
        s.toLowerCase().includes('bkb')
    );

    res.json({
        totalStationsSeen: allStations.length,
        pengeRelated,
        allStations: allStations.slice(0, 200), // First 200 stations
        monitoredCodes: Object.keys(CONFIG.toCRS)
    });
});

// Debug endpoint - see raw departures with TIPLOCs
app.get('/api/debug/raw', (req, res) => {
    const rawData = {};
    Object.keys(departures).forEach(station => {
        rawData[station] = departures[station].map(d => ({
            destination: d.destination,
            resolvedName: CONFIG.stationNames[d.destination] || 'Unknown',
            scheduledTime: d.ptd || d.wtd,
            platform: d.plat
        }));
    });
    res.json({
        rawData,
        knownMappings: Object.keys(CONFIG.stationNames).length
    });
});

/**
 * Format departures for API response
 */
function formatDepartures(deps, crsCode) {
    if (!deps || !Array.isArray(deps)) return [];
    const walkMins = crsCode ? (CONFIG.walkingTimes[crsCode] || 5) : null;
    const ownName = crsCode ? (CONFIG.stations[crsCode]?.name || null) : null;
    return deps.map(d => {
        // Extract platform number - Darwin can send it as object or string
        let platform = '-';
        if (d.plat) {
            if (typeof d.plat === 'string') {
                platform = d.plat;
            } else if (typeof d.plat === 'object') {
                // Darwin sends platform as object like {platsrc: "A", conf: "true", "": "2"}
                // or {platsrc: "P", cisPlatsup: "true", "": "1"}
                // The actual platform value is in the empty string key or 'plat' key
                platform = d.plat[''] || d.plat.plat ||
                    Object.values(d.plat).find(v => typeof v === 'string' && /^[0-9]+[A-Za-z]?$|^[A-Za-z]$/.test(v)) || '-';
            }
        }

        // Format destination from TIPLOC
        const destName = CONFIG.stationNames[d.destination] || d.destination || 'Unknown';

        const minsUntilDeparture = calculateMinutes(d.ptd || d.wtd);

        return {
            destination: destName,
            scheduledTime: d.ptd || d.wtd,
            expectedTime: typeof d.dep === 'object' ? d.dep?.at : d.dep,
            platform: platform,
            // Recompute from the scheduled clock time at response time — the stored
            // d.mins goes stale between Darwin updates (was the "stuck at 59" bug)
            mins: minsUntilDeparture !== null ? minsUntilDeparture : d.mins,
            walkMins: walkMins,
            leaveInMins: (minsUntilDeparture !== null && walkMins !== null) ? minsUntilDeparture - walkMins : null,
            cancelled: d.cancelled || false,
            delayed: d.delayed || false,
            lateReason: d.lateReason
        };
    })
    // Drop self-referential rows (a train "to" the station whose board this is —
    // Darwin association artifacts, e.g. "Penge East → Penge East")
    .filter(d => !ownName || d.destination !== ownName);
}

// ============================================
// START SERVER
// ============================================
const PORT = process.env.PORT || 10000; // Render uses port 10000 by default

app.listen(PORT, () => {
    console.log(`Darwin API server running on port ${PORT}`);
    console.log(`Monitoring stations: ${Object.keys(CONFIG.stations).join(', ')}`);

    // Start Kafka consumer
    startKafkaConsumer();

    // Fetch TfL Overground status immediately and every 2 minutes
    fetchOvergroundStatus();
    setInterval(fetchOvergroundStatus, 2 * 60 * 1000);

    // Auto-decay stale disruption status (clear after 30 mins if no updates)
    setInterval(() => {
        const now = Date.now();
        const DECAY_MS = 30 * 60 * 1000;
        Object.keys(lineStatus).forEach(line => {
            const ls = lineStatus[line];
            if (ls.status === 'disrupted' && ls.updatedAt &&
                now - ls.updatedAt.getTime() > DECAY_MS) {
                lineStatus[line] = { status: 'good', message: null, updatedAt: new Date() };
                console.log(`Auto-cleared stale disruption on ${line}`);
            }
        });
    }, 5 * 60 * 1000);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
    console.log('Shutting down...');
    await consumer.disconnect();
    process.exit(0);
});

module.exports = app;
