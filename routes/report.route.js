import express from "express";
import {downloadReportController, summaryReport, unmatchedReport} from "../controllers/report.controller.js";
import {validateRunIdParam} from "../utils/validationId.js";

const reportRouter = express.Router();

reportRouter.get("/:runId",validateRunIdParam,downloadReportController);
reportRouter.get("/:runId/summary",validateRunIdParam,summaryReport);
reportRouter.get("/:runId/unmatched",validateRunIdParam,unmatchedReport);

export {reportRouter};