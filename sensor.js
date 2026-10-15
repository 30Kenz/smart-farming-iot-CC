const mqtt = require("mqtt");

const BROKER_URL = "mqtt://broker.hivemq.com";
const TOPIC = "ioteam/irrigation/telemetry";

const client = mqtt.connect(BROKER_URL, {
  clientId: "ioteam_sensor_" + Math.random().toString(16).slice(2, 8),
  connectTimeout: 10_000,
  reconnectPeriod: 2_000,
});

const rand = (min, max, d = 1) =>
  Number((Math.random() * (max - min) + min).toFixed(d));

function buildPayload() {
  const pumpStatus = Math.random() > 0.5;
  return {
    device_id: "ESP32_NODE_01",
    timestamp: Math.floor(Date.now() / 1000),
    telemetry: {
      soil_moisture_percent: Math.round(rand(15, 80)),
      soil_temperature_celsius: rand(20, 35),
      air_temperature_celsius: rand(22, 42),
      air_humidity_percent: rand(40, 95),
      water_ph: rand(5.0, 8.5),
    },
    actuator_verification: {
      pump_status: pumpStatus,
      water_flow_liters: pumpStatus ? rand(5, 20) : 0,
    },
  };
}

client.on("connect", () => {
  console.log("✅ Sensor connected:", BROKER_URL);
  setInterval(() => {
    const payload = buildPayload();
    client.publish(TOPIC, JSON.stringify(payload), { qos: 1 });
    console.log("📤 Data dummy terkirim");
  }, 5000);
});

client.on("error", (err) => console.error("❌ MQTT error:", err.message));