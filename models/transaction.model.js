import mongoose from "mongoose";

const { ObjectId, Decimal128 } = mongoose.Schema.Types;

const transactionSchema = new mongoose.Schema({
    _id: { 
        type: String 
    },
    runId: {
        type: ObjectId,
        ref: 'Run', 
        required: true
    },
    source: {
        type: String,
        required: true,
        enum: ["EXCHANGE", "USER"]     
    },
    externalId: {
        type: String,
        required: true
    },
    timestamp: {
        type: Date,
        required: true
    },
    asset: {
        type: String,
        required: true
    },
    quantity: {
        type: Decimal128,
        required: true
    },
    type: {
        type: String,
        required: true,
        enum: ["BUY", "SELL", "TRANSFER_IN", "TRANSFER_OUT"]
    },
    rawRow: {
        type: Object,
        required: true     
    },
    isValid: {
        type: Boolean,
        default: true
    },
    validationErrors: {
        type: [String],
        default: []
    },
    matchingStatus: {
        type: String,
        enum: ["UNMATCHED", "MATCHED", "CONFLICTING"],
        default: "UNMATCHED"
    },
    reconciliationReason: {
        type: String,
        default: ""
    },
    pairedTxId: { 
        type: String, 
        ref: 'Transaction' 
    }
}, { timestamps: true }); 

// --- THE PRODUCTION PERFORMANCE BOOST ---
// We create a compound index. When the matching engine searches for rows, 
// it filters by runId, splits by source, and sorts by timeline.
// This index reduces query scanning costs to virtually zero.
transactionSchema.index({ runId: 1, source: 1, timestamp: 1 });

const Transaction = mongoose.model("Transaction", transactionSchema);
export { Transaction };