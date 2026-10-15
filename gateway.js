// gateway.js — Virtual Gateway (Fog Layer) IoTeam
// Menerima telemetri, menjalankan Edge Computing Rules, simulasi Multi-Sig,
// verifikasi aliran air, lalu membuat log ber-hash (simulasi blockchain).
// Hasil tiap pesan dicetak sebagai JSON dan disimpan ke results.json.

const mqtt = require("mqtt");
const crypto = require("crypto");
const fs = require("fs");

const BROKER_URL = "mqtt://broker.hivemq.com"; // harus sama dengan sensor.js
const TOPIC = "ioteam/irrigation/telemetry";
const RESULT_FILE = "results.json";

// ---- Aturan Komputasi Tepi (Edge Computing Rules) ----
const RULES = {
  MAX_AIR_TEMP: 40,          // IF temp < 40° THEN pompa boleh menyala
  MIN_SOIL_MOISTURE: 40,     // tanah dianggap kering di bawah ini
  CRITICAL_SOIL_MOISTURE: 25,// di bawah ini = irigasi kritis → butuh Multi-Sig
  RAIN_HUMIDITY: 85,         // kelembapan udara >= ini dianggap hujan → pompa dimatikan
  PH_MIN: 5.5,
  PH_MAX: 7.5,
};

// Rantai hash sederhana untuk mensimulasikan immutable ledger
let previousHash = "0".repeat(64);
const results = loadResults();

function loadResults() {
  try {
    const existing = JSON.parse(fs.readFileSync(RESULT_FILE, "utf8"));
    if (Array.isArray(existing) && existing.length) {
      previousHash = existing[existing.length - 1].blockchain_log.hash;
      return existing;
    }
  } catch (_) { /* file belum ada atau rusak → mulai baru */ }
  return [];
}

// Langkah 3: Pemeriksaan aturan di gateway
function evaluateRules(t) {
  const reasons = [];
  const tempOk = t.air_temperature_celsius < RULES.MAX_AIR_TEMP;
  const soilDry = t.soil_moisture_percent < RULES.MIN_SOIL_MOISTURE;
  const raining = t.air_humidity_percent >= RULES.RAIN_HUMIDITY;
  const phOk = t.water_ph >= RULES.PH_MIN && t.water_ph <= RULES.PH_MAX;

  if (!tempOk) reasons.push(`Suhu udara ${t.air_temperature_celsius}°C ≥ ${RULES.MAX_AIR_TEMP}°C`);
  if (!soilDry) reasons.push(`Tanah cukup lembap (${t.soil_moisture_percent}%)`);
  if (raining) reasons.push(`Kemungkinan hujan (kelembapan ${t.air_humidity_percent}%)`);

  const warnings = [];
  if (!phOk) warnings.push(`pH air ${t.water_ph} di luar rentang ${RULES.PH_MIN}-${RULES.PH_MAX}`);

  const irrigationNeeded = tempOk && soilDry && !raining;
  return {
    irrigation_needed: irrigationNeeded,
    critical: irrigationNeeded && t.soil_moisture_percent < RULES.CRITICAL_SOIL_MOISTURE,
    reasons: irrigationNeeded ? ["Tanah kering, suhu aman, tidak hujan"] : reasons,
    warnings,
  };
}

// Langkah 5: Simulasi konsensus Multi-Sig (Pengawas + Ahli Agronomi)
function multiSigConsensus(isCritical) {
  if (!isCritical) {
    return { required: false, approvals: {}, approved: true };
  }
  // Simulasi: tiap penandatangan menyetujui dengan peluang 80%
  const approvals = {
    pengawas: Math.random() > 0.2,
    ahli_agronomi: Math.random() > 0.2,
  };
  return {
    required: true,
    approvals,
    approved: approvals.pengawas && approvals.ahli_agronomi,
  };
}

// Langkah 6: Verifikasi fisik dari sensor aliran air
function verifyActuator(shouldPump, a) {
  let status;
  if (shouldPump && a.pump_status && a.water_flow_liters > 0) status = "VERIFIED";
  else if (shouldPump && a.pump_status && a.water_flow_liters === 0) status = "FAILED_NO_FLOW";
  else if (shouldPump && !a.pump_status) status = "FAILED_PUMP_OFF";
  else if (!shouldPump && a.pump_status) status = "ANOMALY_PUMP_ON_WITHOUT_APPROVAL";
  else status = "IDLE";

  return {
    pump_command: shouldPump ? "ON" : "OFF",
    pump_status_reported: a.pump_status,
    water_flow_liters: a.water_flow_liters,
    status,
  };
}

// Langkah 7: Log kriptografis (SHA-256, berantai ke hash sebelumnya)
function createBlockchainLog(record) {
  const data = JSON.stringify(record);
  const hash = crypto.createHash("sha256").update(previousHash + data).digest("hex");
  const log = { previous_hash: previousHash, hash };
  previousHash = hash;
  return log;
}

function processPayload(payload) {
  const { device_id, timestamp, telemetry, actuator_verification } = payload;

  const rule = evaluateRules(telemetry);
  const consensus = rule.irrigation_needed
    ? multiSigConsensus(rule.critical)
    : { required: false, approvals: {}, approved: false };

  const shouldPump = rule.irrigation_needed && consensus.approved;
  const verification = verifyActuator(shouldPump, actuator_verification);

  let decision;
  if (!rule.irrigation_needed) decision = "STANDBY";
  else if (!consensus.approved) decision = "REJECTED_BY_MULTISIG";
  else decision = "IRRIGATE";

  const record = {
    device_id,
    sensor_timestamp: new Date(timestamp * 1000).toISOString(),
    processed_at: new Date().toISOString(),
    telemetry,
    rule_check: rule,
    multisig: consensus,
    decision,
    actuator_verification: verification,
  };

  return { ...record, blockchain_log: createBlockchainLog(record) };
}

function isValidPayload(p) {
  return p && p.device_id && typeof p.timestamp === "number" &&
    p.telemetry && p.actuator_verification;
}

// ---- MQTT ----
const client = mqtt.connect(BROKER_URL, {
  clientId: "ioteam_gateway_" + Math.random().toString(16).slice(2, 8),
  connectTimeout: 10_000,
  reconnectPeriod: 2_000,
});

client.on("connect", () => {
  console.log("✅ Gateway connected:", BROKER_URL);
  client.subscribe(TOPIC, { qos: 1 }, (err) => {
    if (err) console.error("❌ Subscribe error:", err.message);
    else console.log("👂 Listening on topic:", TOPIC);
  });
});

client.on("message", (_topic, message) => {
  let payload;
  try {
    payload = JSON.parse(message.toString());
  } catch {
    console.error("❌ Payload bukan JSON valid:", message.toString());
    return;
  }
  if (!isValidPayload(payload)) {
    console.error("❌ Format payload tidak sesuai spesifikasi:", message.toString());
    return;
  }

  const result = processPayload(payload);
  console.log(JSON.stringify(result, null, 2));

  results.push(result);
  fs.writeFileSync(RESULT_FILE, JSON.stringify(results, null, 2));
});

client.on("error", (err) => console.error("❌ MQTT error:", err.message));