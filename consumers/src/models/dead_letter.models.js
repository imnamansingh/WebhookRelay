import mongoose from "mongoose";

const deadLetterSchema = new mongoose.Schema({
    topic: String,
    partition: Number,
    offset: String,
    payload: Object,
    errorReason: String,
    failedAt: { type: Date, default: Date.now },
})

export const DeadLetter = mongoose.model("DeadLetter", deadLetterSchema)

