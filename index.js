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

    // Walking time in minutes from home to each station
    walkingTimes: {
        'PNW': 4,   // Penge West
        'PNE': 5,   // Penge East
        'ANR': 7,   // Anerley
        'BKB': 10   // Birkbeck
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
        'PCKHMRY': 'Peckham Rye',
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
 * Calculate minutes from now until given time (HH:MM format)
 */
function calculateMinutes(timeStr) {
    if (!timeStr) return null;

    const [hours, mins] = timeStr.split(':').map(Number);
    const now = new Date();
    const target = new Date();
    target.setHours(hours, mins, 0, 0);

    // Handle times past midnight
    if (target < now && hours < 6) {
        target.setDate(target.getDate() + 1);
    }

    return Math.round((target - now) / 60000);
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

    if (!CONFIG.stations[station]) {
        return res.status(404).json({ error: 'Station not found' });
    }

    res.json({
        timestamp: new Date(),
        lastUpdate,
        station: {
            code: station,
            name: CONFIG.stations[station].name,
            line: CONFIG.stations[station].line,
            walkMins: CONFIG.walkingTimes[station] || null,
            departures: formatDepartures(departures[station], station)
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

    const matchedName = CONFIG.stationNames[matchingTiplocs.values().next().value];

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
    const walkMins = crsCode ? (CONFIG.walkingTimes[crsCode] || 5) : null;
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
            mins: d.mins,
            walkMins: walkMins,
            leaveInMins: (minsUntilDeparture !== null && walkMins !== null) ? minsUntilDeparture - walkMins : null,
            cancelled: d.cancelled || false,
            delayed: d.delayed || false,
            lateReason: d.lateReason
        };
    });
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
});

// Graceful shutdown
process.on('SIGTERM', async () => {
    console.log('Shutting down...');
    await consumer.disconnect();
    process.exit(0);
});

module.exports = app;
