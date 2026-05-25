import mongoose from "mongoose";
const connectDB=async()=>{
    try{
        const connectionInstance=await mongoose.connect(process.env.MONGODB_URL);
        console.log(`MongoDB connected successfully:${connectionInstance.connection.host}`);
    }catch(err){
        console.log(`Error while connecting MongoDB:${err}`);
        process.exit(1);
    }
}

export {connectDB};