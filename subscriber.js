// subscriber.js — Menerima payload sensor IoTeam dan menampilkannya sebagai JSON
// Setiap data yang diterima dicetak rapi di terminal dan disimpan ke results.json.

const mqtt = require("mqtt");
const fs = require("fs");

const BROKER_URL = "mqtt://broker.hivemq.com"; // harus sama dengan sensor.js
const TOPIC = "ioteam/irrigation/telemetry";
const RESULT_FILE = "results.json";

// Muat data lama (kalau ada) supaya results.json terus bertambah
let results = [];
try {
  const existing = JSON.parse(fs.readFileSync(RESULT_FILE, "utf8"));
  if (Array.isArray(existing)) results = existing;
} catch (_) { /* file belum ada → mulai baru */ }

const client = mqtt.connect(BROKER_URL, {
  clientId: "ioteam_sub_" + Math.random().toString(16).slice(2, 8),
  connectTimeout: 10_000,
  reconnectPeriod: 2_000,
});

client.on("connect", () => {
  console.log("✅ Subscriber connected:", BROKER_URL);
  client.subscribe(TOPIC, { qos: 1 }, (err) => {
    if (err) console.error("❌ Subscribe error:", err.message);
    else console.log("👂 Listening on topic:", TOPIC, "\n");
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

  // Tampilkan persis seperti format payload, dengan indentasi 2 spasi
  console.log(JSON.stringify(payload, null, 2));
  console.log("-".repeat(40));

  results.push(payload);
  fs.writeFileSync(RESULT_FILE, JSON.stringify(results, null, 2));
});

client.on("error", (err) => console.error("❌ MQTT error:", err.message));