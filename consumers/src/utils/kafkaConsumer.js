import kafka from "./kafkaClient.js"

const consumer = kafka.consumer({
    groupId: "webhook-relay-consumer",
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    rebalanceTimeout: 60000,
    maxWaitTimeInMs: 20000,
});

const connectConsumer = async () => {
    try {
        await consumer.connect()
        console.log("Consumer connected successfully")
    } catch (error) {
        console.log("Error while connecting Consumer: ", error)
        process.exit(1);
    }
}

const disconnectConsumer = async () => {
    try {
        await consumer.disconnect()
        console.log("Consumer disconnected successfully")
    } catch (error) {
        console.log("Error while disconnecting Consumer: ", error)
        process.exitCode = 1;
    }
}


export default consumer;

export {

    connectConsumer,
    disconnectConsumer

}