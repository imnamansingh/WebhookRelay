import { Router } from "express";
import {
    handleHooks
} from "../controllers/producer.controllers.js"

const router = Router()

router.route("/webhook").post(handleHooks)


export default router;