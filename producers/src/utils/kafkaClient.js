import { Kafka } from "kafkajs";

const brokers = (process.env.KAFKA_BROKER_URL || "")
  .split(",")
  .map((broker) => broker.trim())
  .filter(Boolean);

if (brokers.length === 0) {
  throw new Error("KAFKA_BROKER_URL must contain at least one broker address.");
}

const kafka = new Kafka({
  clientId: 'webhook-relay-producer',
  brokers,
});

export default kafka;