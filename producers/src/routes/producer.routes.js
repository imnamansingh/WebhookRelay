import { Router } from "express";
import {
    handleWebhooks,
    handleFailedWebhooks
} from "../controllers/producer.controllers.js"

const router = Router()

router.route("/webhook").post(handleWebhooks)
router.route("/failed_webhook").post(handleFailedWebhooks)


export default router;