const DEFAULT_URL =
  process.env.PRODUCER_URL ||
  "http://localhost:3000/api/v1/producer/webhook";

const args = Object.fromEntries(
  process.argv.slice(2).map((arg) => {
    const [key, value] = arg.replace(/^--/, "").split("=");
    return [key, value ?? true];
  }),
);

const count = Number(args.count ?? 400);
const intervalMs = Number(args.interval ?? 100);
const url = String(args.url ?? DEFAULT_URL);

if (!Number.isInteger(count) || count < 1) {
  throw new Error("--count must be a positive integer");
}

if (!Number.isFinite(intervalMs) || intervalMs < 0) {
  throw new Error("--interval must be zero or a positive number");
}

const payloadTypes = [
  { timeToPersist: 0, data: "instant webhook" },
  { timeToPersist: 10, data: "10s webhook" },
  { timeToPersist: 60, data: "60s webhook" },
  { timeToPersist: 120, data: "fDLQ webhook" },
];

const delay = (milliseconds) =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

let successCount = 0;
let failureCount = 0;

const main = async () => {

  while(true){

    console.log(
      `Sending ${count} webhooks to ${url} at ${Date.now()}`,
    );

    for (let index = 0; index < count; index++) {
      const payload = payloadTypes[index % payloadTypes.length];

      try {
        const response = await fetch(url, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "x-event-id": `test-event-${index + 1}`,
          },
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          const responseBody = await response.json();
          throw new Error(
            `HTTP ${responseBody.status}: ${responseBody.statusText || "Unknown Error"}`
          );
        }

        successCount++;
      } catch (error) {

        failureCount++;
        console.error(`Webhook ${index + 1} failed: ${error.message}`);

        if(failureCount >= 5){
          throw new Error("Test script aborted: failure count exceeded the safe limit")
        }
      }
    }

    if(intervalMs > 0){

      await delay(intervalMs)

    }
  }
}

main()
.catch((error) => {
  console.log(error.message);
  process.exit(1);
})

