import producer from "../utils/kafkaProducer.js"

const handleWebhooks = async (req, res, next) => {
    try {
        
        const topic = "webhooks"

        const payload = {
            headers: req.headers,
            body: req.body,
            receivedAt: new Date().toISOString(),
        };
        await producer.send({
            topic,
            messages: [
                {
                    key: req.headers['x-event-id'],
                    value: JSON.stringify(payload),
                },
            ],
        });

        console.log(`Webhook has been sent to kafka topic: ${topic}`)

        return res.status(200).json({ status: 'queued' })

    } catch (error) {
        console.log(`Error while handling webhook for topic: ${topic}`)
        next(error);
    }
}

const handleFailedWebhooks = async (req, res, next) => {
    try {

        const topic = req.body?.topicName || "webhooks-retry-60s"

        const payload = {
            headers: req.headers,
            body: req.body.webhook,
            receivedAt: new Date().toISOString(),
        };

        const retryCount = req.headers['x-retry-count'] || "2";
        await producer.send({
            topic,
            messages: [
                {
                    key: retryCount,
                    headers: {
                        'x-retry-count': retryCount
                    },
                    value: JSON.stringify(payload),
                },
            ],
        });

        console.log(`Failed webhook has been sent to kafka topic: ${topic}`)

        return res.status(200).json({ status: 'queued' })
        
    } catch (error) {
        console.log(`Error while handling failed webhook for topic:  ${topic}`)
        next(error)
    }
}

export {
    handleWebhooks,
    handleFailedWebhooks
}