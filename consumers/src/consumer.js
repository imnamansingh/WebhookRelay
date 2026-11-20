import "dotenv/config";
import { connectDB, disconnectDB } from "./db/index.js";
import consumer, { connectConsumer, disconnectConsumer } from "./utils/kafkaConsumer.js";
import { handleWebhook } from "./controllers/webhook.controllers.js"
import { DeadLetter } from "./models/dead_letter.models.js"

const TOPICS = {
  MAIN: 'webhooks',
  RETRY_10S: 'webhooks-retry-10s',
  RETRY_60S: 'webhooks-retry-60s',
};


async function handleFailure(topic, partition, message, errorReason) {
  const currentRetry = Number(message.headers?.['x-retry-count']?.toString() || 0);
  const nextRetry = currentRetry + 1;

  const url = process.env.PRODUCER_URL;

  // Send to 10-second delay topic
  if (nextRetry === 1) {

    const payload = {
      topicName: TOPICS.RETRY_10S,
      webhook: JSON.parse(message.value.toString()).body
    }

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-retry-count": String(nextRetry)
      },
      body: JSON.stringify(payload)
    })

    if(!response.ok){
      throw new Error(`Fetch call to provider failed: topic=${topic}, partition=${partition}, offset=${message.offset}, eventId=${message.key?.toString() ?? "n/a"}`)
    }

    console.log(`Failed webhook from topic ${TOPICS.MAIN} has been sent to Producer`);
    return;
  }

  // Send to 60-second delay topic
  if (nextRetry === 2) {

    const payload = {
      topicName: TOPICS.RETRY_60S,
      webhook: JSON.parse(message.value.toString()).body
    }

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-retry-count": String(nextRetry)
      },
      body: JSON.stringify(payload)
    })

    if(!response.ok){
      throw new Error(`Fetch call to provider failed: topic=${topic}, partition=${partition}, offset=${message.offset}, eventId=${message.key?.toString() ?? "n/a"}`)
    }

    console.log(`Failed webhook from topic ${TOPICS.RETRY_10S} has been sent to Producer`);
    return;
  }

  // Persist to MongoDB Dead Letter Queue
  const saved = await DeadLetter.create({
    topic,
    partition,
    offset: message.offset,
    payload: JSON.parse(message.value.toString()),
    errorReason,
  });

  console.error(`Retries exhausted for offset ${message.offset} from topic ${topic} and partition ${partition}. Saved to MongoDB DLQ with id ${saved._id}`);

}


const main = async () => {

    await connectDB();
    await connectConsumer();
    
    const DELAY_MAP = {
        [TOPICS.MAIN]: 0,
        [TOPICS.RETRY_10S]: 10_000,
        [TOPICS.RETRY_60S]: 60_000,
    };

    await consumer.subscribe({
        topics: [TOPICS.MAIN, TOPICS.RETRY_10S, TOPICS.RETRY_60S],
        fromBeginning: true,
    });

    await consumer.run({
        eachBatchAutoResolve: false,
        eachBatch: async ({ batch, resolveOffset }) => {

          const {
            topic,
            partition
          } = batch;

          

          for(const message of batch.messages){

            const requiredDelay = DELAY_MAP[topic] || 0;
            if (requiredDelay > 0) {
                const elapsed = Date.now() - Number(message.timestamp);
                const remaining = requiredDelay - elapsed;

                if (remaining > 0) {

                    consumer.pause([{ topic, partitions: [partition] }]);
                    setTimeout(() => {
                      consumer.resume([{ topic, partitions: [partition] }]);
                    }, remaining);

                    return;
                }
            }
    
            try {
              
                const data = JSON.parse(message.value.toString());

                const dataToHandle = {
                  delayTime: (DELAY_MAP[topic]/1000),
                  data: data.body
                }

                await handleWebhook(dataToHandle);

                console.info(`Webhook processed successfully: topic=${topic}, partition=${partition}, offset=${message.offset}, eventId=${message.key?.toString() ?? "n/a"}`)
                
            } catch (error) {

                try {

                  await handleFailure(topic, partition, message, error.message);

                } catch (error) {

                  console.log(`Error while handling failed webhook: topic=${topic}, partition=${partition}, offset=${message.offset}, eventId=${message.key?.toString() ?? "n/a"}`)
        
                }

            }

            resolveOffset(message.offset);

          }

            
        },
    });
}

let shuttingDown = false;

const shutdown = async (signal) => {

  if(shuttingDown) return;

  shuttingDown = true;

  console.log(`${signal} received; shutting down`);

  try {

    await consumer.stop();
    await disconnectConsumer();
    await disconnectDB();

  } catch (error) {
    
    console.log("Error during shutdown: ", error)
    process.exitCode = 1;

  }

}

process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));

main()
.catch((error) => {
  process.exitCode = 1;
})



