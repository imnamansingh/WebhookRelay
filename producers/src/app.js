import express from "express"

const app = express()

app.use(express.json());
app.use(express.urlencoded({ extended: true }));


import producerRouter from "./routes/producer.routes.js"
app.use("/api/v1/producer", producerRouter);


app.use((error, req, res, next) => {

    return res.status(500).json({
        status: 500,
        statusText: "Internal Server Error"
    })

})

export default app;