import { ReportService } from "../services/ReportService.js";
import { Run } from "../models/run.model.js";
import { Transaction } from "../models/transaction.model.js";

const reportService = new ReportService();

/**
 * GET /report/:runId
 * Streams a side-by-side transaction reconciliation report CSV file down to the client.
 */
const downloadReportController = async (req, res) => {
    const { runId } = req.params;

    try {
        const runExists = await Run.exists({ _id: runId });
        if (!runExists) {
            return res.status(404).json({ success: false, message: `Reconciliation run ${runId} does not exist.` });
        }

        // Instruct client browser to treat this stream as an attachment download
        res.setHeader('Content-Type', 'text/csv');
        res.setHeader('Content-Disposition', `attachment; filename="reconciliation_report_${runId}.csv"`);
        
        // Stream the CSV generation payload directly down the wire
        await reportService.generateFullCsvReport(runId, res);

    } catch (error) {
        console.error(`[Report Error] Generation failed for run ${runId}:`, error);
        
        if (!res.headersSent) {
            return res.status(500).json({ success: false, message: "Internal server error compiled during CSV stream serialization." });
        }
        
        res.end();
    }
};

/**
 * GET /report/:runId/summary
 * Fetches just the counts: matched, conflicting, unmatched
 */
const summaryReport = async (req, res) => {
    try {
        const { runId } = req.params;
        
        const run = await Run.findById(runId).select('summary status').lean();
        
        if (!run) {
            return res.status(404).json({ success: false, message: `Reconciliation run summary for ID ${runId} could not be located.` });
        }

        return res.status(200).json({
            success: true,
            runId,
            status: run.status,
            summary: run.summary
        });
        
    } catch (error) {
        console.error(`[API Error] Failed to retrieve summary counts for run ${req.params.runId}:`, error);
        return res.status(500).json({ success: false, message: "Failed to compile run summary totals calculation metrics." });
    }
};

/**
 * GET /report/:runId/unmatched
 * Fetch only unmatched rows along with their explicit validation or matching reasons
 */
const unmatchedReport = async (req, res) => {
    try {
        const { runId } = req.params;

        const runExists = await Run.exists({ _id: runId });
        if (!runExists) {
            return res.status(404).json({ success: false, message: `Reconciliation run matching records for ID ${runId} do not exist.` });
        }
        
        const unmatchedTransactions = await Transaction.find({ 
            runId, 
            matchingStatus: "UNMATCHED" 
        })
        .select('externalId source timestamp asset quantity type validationErrors reconciliationReason isValid')
        .lean();

        return res.status(200).json({
            success: true,
            runId,
            count: unmatchedTransactions.length,
            records: unmatchedTransactions
        });
        
    } catch (error) {
        console.error(`[API Error] Failed to aggregate unmatched data arrays for run ${req.params.runId}:`, error);
        return res.status(500).json({ success: false, message: "Internal application breakdown searching unmatched data ledgers." });
    }
};

export { downloadReportController, summaryReport, unmatchedReport };