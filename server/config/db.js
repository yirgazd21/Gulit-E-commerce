const mongoose = require('mongoose');
const dotenv = require('dotenv');

dotenv.config();

// Primary URI: replica set across both PCs (full distributed mode)
// Fallback URI: local MongoDB on this machine only (standalone mode)
const REPLICA_URI = process.env.MONGO_URI;
const LOCAL_URI   = process.env.MONGO_LOCAL_URI || 'mongodb://127.0.0.1:27017/gulit';

let isConnected = false;

const connectDB = async () => {
  // --- Attempt 1: Full replica set (both PCs online) ---
  try {
    const conn = await mongoose.connect(REPLICA_URI, {
      bufferCommands: false,
      serverSelectionTimeoutMS: 4000,   // give up quickly if peer is down
      connectTimeoutMS: 4000,
    });

    isConnected = true;
    console.log(`✅ [DB] Distributed replica set connected: ${conn.connection.host}`);
    return;
  } catch (replicaErr) {
    console.warn(`⚠️  [DB] Replica set unavailable (${replicaErr.message}). Falling back to local MongoDB...`);
  }

  // --- Attempt 2: Local standalone MongoDB (this PC only) ---
  try {
    const conn = await mongoose.connect(LOCAL_URI, {
      bufferCommands: false,
      serverSelectionTimeoutMS: 4000,
      connectTimeoutMS: 4000,
    });

    isConnected = true;
    console.log(`✅ [DB] Local standalone MongoDB connected: ${conn.connection.host}`);
    console.log(`ℹ️  [DB] Running in STANDALONE mode — peer data will sync when the other PC comes back online.`);
    return;
  } catch (localErr) {
    console.error(`❌ [DB] Local MongoDB also failed: ${localErr.message}`);
    console.error(`❌ [DB] Server is running but ALL database operations will fail until MongoDB is available.`);
  }
};

// Periodically try to reconnect to the replica set when in standalone mode.
// When the peer comes back online, Mongoose will automatically upgrade the
// connection without a server restart.
const startReconnectWatcher = () => {
  setInterval(async () => {
    const state = mongoose.connection.readyState;
    // 0 = disconnected, 3 = disconnecting
    if (state === 0 || state === 3) {
      console.log('[DB] Connection lost — attempting reconnect...');
      await connectDB();
    }
  }, 15000); // check every 15 seconds
};

module.exports = { connectDB, startReconnectWatcher };
