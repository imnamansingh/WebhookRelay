import "dotenv/config";
import app from "./app.js";
import { connectDB, disconnectDB } from "./db/index.js";
import { connectProducer, disconnectProducer } from "./utils/kafkaProducer.js";

const connectDBAndProducer = async () => {
    try {

        await connectDB();
        await connectProducer();

    } catch (error) {
        console.log("Error while connection DB and Kafka Producer: ", error)
    }
}

const port = Number(process.env.PORT || 3000)
let server;
let shuttingDown = false;

async function shutdown(signal){

    if(shuttingDown) return;

    shuttingDown = true;

    console.log(`${signal} received; shutting down`);

    try {
        
        if(server?.listening){
            await new Promise((resolve, reject) => {
                server.close((error) => {
                if (error) {
                    reject(error);
                } else {
                    resolve();
                }
                });
            });
        }

        await disconnectProducer()
        await disconnectDB()
        console.log("shutdown completed")
    } catch (error) {
        console.log("Error while shutting down: ", error)
        process.exitCode = 1;
    }
}

process.once("SIGINT", () => {
    void shutdown("SIGINT");
})

process.once("SIGTERM", () => {
    void shutdown("SIGTERM");
})

connectDBAndProducer()
.then(() => {
    server = app.listen(port, () => {
        console.log(`Server is listening on port ${port}`)
    })

    server.on("error", (error) => {
        console.log("server error: ", error)
        process.exitCode = 1;
        void shutdown("SIGINT");
    })
})
.catch((error) => {
    console.log("server error: ", error)
    process.exitCode = 1;
    void shutdown("SIGINT");
})