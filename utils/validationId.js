import mongoose from "mongoose";

const validateRunIdParam = (req, res, next) => {
    const { runId } = req.params;
    
    if (!mongoose.Types.ObjectId.isValid(runId)) {
        return res.status(400).json({
            success: false,
            message: `Malformed parameter input: '${runId}' is not a valid MongoDB Hexadecimal ObjectId format.`
        });
    }
    next();
};
export {validateRunIdParam};