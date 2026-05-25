import mongoose from "mongoose";

const runSchema = new mongoose.Schema({
    runSignature:{
        type:String,
        required:true,
        unique:true
    },
    status: {
        type: String,
        enum: ["PROCESSING", "COMPLETED", "FAILED"],
        default: "PROCESSING"
    },
    config: {
        timestampToleranceSeconds: {
            type: Number,
            required: true,
            default: 300
        },
        quantityTolerancePct: {
            type: Number,
            required: true,
            default: 0.01
        }
    },
    summary: {
        matchedCount: {
            type: Number,
            default: 0
        },
        conflictingCount: {
            type: Number,
            default: 0
        },
        unmatchedUserCount: {
            type: Number,
            default: 0
        },
        unmatchedExchangeCount: {
            type: Number,
            default: 0
        }
    },
    errorMessage: {
        type: String,
        default: ""
    }
}, { timestamps: true });

const Run = mongoose.model("Run", runSchema);
export { Run };