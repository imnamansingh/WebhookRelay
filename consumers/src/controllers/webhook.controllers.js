
const handleWebhook = async (dataToHandle) => {
    const {
        delayTime,
        data
    } = dataToHandle;

    const { timeToPersist } = data;

    if(timeToPersist <= delayTime) return;

    else{

        throw new Error(`Webhook processing failed: delay time is ${delayTime}s while time to persist is ${timeToPersist}s`)

    }
}

export {
    handleWebhook
}