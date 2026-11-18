import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: 'webhook-relay',
  brokers: [process.env.KAFKA_URL],
});

export default kafka;