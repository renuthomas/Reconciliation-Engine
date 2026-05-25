import express from "express";
import { startReconcile } from "../controllers/reconcile.controller.js";


const reconcileRouter = express.Router();

reconcileRouter.post("/",startReconcile);

export {reconcileRouter};