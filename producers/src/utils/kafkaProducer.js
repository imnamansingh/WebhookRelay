import kafka from "./kafkaClient.js"

const producer = kafka.producer();

const connectProducer = async () => {
    try {

        await producer.connect()
        console.log("Kafka Producer connected successfully")

    } catch (error) {

        console.log("Error while connecting to Kafka Producer", error)
        process.exit(1)
    }
}

const disconnectProducer = async () => {

    try {
        
        await producer.disconnect()
        console.log("Kafka Producer disconnected successfully")

    } catch (error) {
        
        console.log("Error while disconnecting Kafka Producer", error)
        process.exitCode = 1;
    }

}

export default producer;

export {
    connectProducer,
    disconnectProducer
}