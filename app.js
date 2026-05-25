import dotenv from "dotenv";
dotenv.config();

import express from "express";
import {rateLimit} from "express-rate-limit"
import { reconcileRouter } from "./routes/reconcile.route.js";
import { reportRouter } from "./routes/report.route.js";
import { connectDB } from "./config/db.config.js";
import cors from "cors";

const app = express();
const PORT = process.env.PORT || 3000;
app.set("trust proxy", 1);

const globalRateLimiter=rateLimit({
    windowMs:15*60*1000,
    limit:100,
    standardHeaders:"draft-7",
    legacyHeaders:false,
    message:{
        success:false,
        message:"Too many requests from this IP, please try again after 15 minutes."
    }
})
app.use(globalRateLimiter);
app.use(express.json());
app.use(cors({
    origin:process.env.CORS_ORIGIN || "http://127.0.0.1:3000"
}));

app.use("/api/v1/reconcile", reconcileRouter);
app.use("/api/v1/report", reportRouter);

app.get("/", (req, res) => { 
    return res.json("Server is Live"); 
}); 

// Self-pinging every 14 minutes
setInterval(() => { 
    fetch("https://reconciliation-engine-p2dy.onrender.com/")
        .then(() => console.log("Self-ping successful"))
        .catch((err) => console.error("Ping failed:", err.message));
}, 840000);



app.use((err, req, res, next) => {
    console.error(`[Global Unhandled App Error]:`, err);

    if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
        return res.status(400).json({
            success: false,
            message: "Malformed data transmission: The provided JSON body string is unparsable."
        });
    }

    return res.status(err.status || 500).json({
        success: false,
        message: err.message || "A fatal runtime exception broke inside the core processing module."
    });
});

const startServer = async () => {
    try {
        console.log("Connecting to core database infrastructure cluster...");
        await connectDB();
        console.log("Database connectivity validated successfully.");

        app.listen(PORT, () => {
            console.log(`[Reconciliation Engine Services Online] Hosting active cluster target on port: ${PORT}`);
        });

    } catch (connectionError) {
        console.error("CRITICAL BOOT FAILURE: Application layer failed to link with database clusters.", connectionError);
        process.exit(1);
    }
};

startServer();