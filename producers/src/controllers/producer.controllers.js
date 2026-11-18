import producer from "../utils/kafkaProducer.js"

const handleHooks = async (req, res, next) => {
    try {
        
        const payload = {
            headers: req.headers,
            body: req.body,
            receivedAt: new Date().toISOString(),
        };
        await producer.send({
            topic: 'failed-webhooks',
            messages: [
                {
                    key: req.headers['x-event-id'] || Date.now().toString(),
                    value: JSON.stringify(payload),
                },
            ],
        });

        return res.status(200).json({ status: 'queued' })

    } catch (error) {
        console.log("Error while handling hooks: ", error)
        next(error);
    }
}

export {
    handleHooks
}