const http = require('http');
const { Server } = require('socket.io');
const mongoose = require('mongoose');
const { createClient } = require('redis');
const { createAdapter } = require('@socket.io/redis-adapter');

const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
const { connectDB, startReconnectWatcher } = require('./config/db');
const userRoutes = require('./routes/userRoutes');
const productRoutes = require('./routes/productRoutes');
const path = require('path');
const uploadRoutes = require('./routes/uploadRoutes');
const orderRoutes = require('./routes/orderRoutes');
const sellerRoutes = require('./routes/sellerRoutes');
const sellerProductRoutes = require('./routes/sellerProductRoutes');
const adminAuthRoutes = require('./routes/adminAuthRoutes');
const adminSellerRoutes = require('./routes/adminSellerRoutes');
const adminUserRoutes = require('./routes/adminUserRoutes');
const adminOrderRoutes = require('./routes/adminOrderRoutes');
const adminFinanceRoutes = require('./routes/adminFinanceRoutes');
const adminSupportRoutes = require('./routes/adminSupportRoutes');
const adminSystemRoutes = require('./routes/adminSystemRoutes');
const platformUpdateRoutes = require('./routes/platformUpdateRoutes');
const categoryRoutes = require('./routes/categoryRoutes');
const webhookRoutes = require('./routes/webhookRoutes');

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// ─── HTTP + SOCKET.IO SETUP ───────────────────────────────────────────────────
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

// ─── REDIS ADAPTER (OPTIONAL — server starts even if Redis is down) ───────────
const REDIS_URLS = [
  process.env.REDIS_URL          || 'redis://10.40.210.101:6379',
  process.env.REDIS_URL_FALLBACK || 'redis://10.40.210.21:6379',
].filter(Boolean);

let redisConnected = false;

const tryConnectRedis = async () => {
  for (const redisUrl of REDIS_URLS) {
    const pubClient = createClient({
      url: redisUrl,
      socket: { connectTimeout: 4000, reconnectStrategy: false },
    });
    const subClient = pubClient.duplicate();

    const onError = (label) => (err) => {
      if (redisConnected) {
        console.warn(`⚠️  [Redis] ${label} error: ${err.message}`);
      }
    };
    pubClient.on('error', onError('pub'));
    subClient.on('error', onError('sub'));

    try {
      await Promise.all([pubClient.connect(), subClient.connect()]);
      io.adapter(createAdapter(pubClient, subClient));
      redisConnected = true;
      console.log(`🚀 [Redis] Socket.IO cluster bridge active via ${redisUrl}`);

      const onEnd = () => {
        if (redisConnected) {
          redisConnected = false;
          console.warn(`⚠️  [Redis] Connection lost (${redisUrl}). Trying all nodes in 15s...`);
          setTimeout(tryConnectRedis, 15000);
        }
      };
      pubClient.on('end', onEnd);
      subClient.on('end', onEnd);

      return; // connected — stop trying other URLs
    } catch (_) {
      // This URL failed — try next one
      try { pubClient.disconnect(); } catch (_) {}
      try { subClient.disconnect(); } catch (_) {}
    }
  }

  // All URLs failed
  if (redisConnected) redisConnected = false;
  console.warn(`⚠️  [Redis] All nodes unavailable. Socket.IO in SINGLE-NODE mode. Retrying in 15s...`);
  setTimeout(tryConnectRedis, 15000);
};

tryConnectRedis();
// ─────────────────────────────────────────────────────────────────────────────

// ─── MIDDLEWARES ──────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// ─── DATABASE ─────────────────────────────────────────────────────────────────
// connectDB tries replica set first, falls back to local MongoDB automatically
connectDB();
startReconnectWatcher();

// ─── ROUTES ───────────────────────────────────────────────────────────────────
app.use('/api/users', userRoutes);
app.use('/api/products', productRoutes);
app.use('/api/upload', uploadRoutes);
app.use('/api/orders', orderRoutes);
app.use('/api/sellers', sellerRoutes);
app.use('/api/sellers/products', sellerProductRoutes);
app.use('/api/admin/auth', adminAuthRoutes);
app.use('/api/admin/sellers', adminSellerRoutes);
app.use('/api/admin/users', adminUserRoutes);
app.use('/api/admin/orders', adminOrderRoutes);
app.use('/api/admin/finance', adminFinanceRoutes);
app.use('/api/admin/support', adminSupportRoutes);
app.use('/api/admin/system', adminSystemRoutes);
app.use('/api/platform', platformUpdateRoutes);
app.use('/api/categories', categoryRoutes);
app.use('/api/webhooks', webhookRoutes);

app.use('/uploads', express.static(path.join(__dirname, '/uploads')));

// ─── ENV CHECK ────────────────────────────────────────────────────────────────
console.log('--- ENV CHECK ---');
console.log('CHAPA_SECRET_KEY exists:', !!process.env.CHAPA_SECRET_KEY);
console.log('NODE_ENV:', process.env.NODE_ENV);
console.log('-----------------');

// ─── CHANGE STREAMS (peer-to-peer real-time sync) ────────────────────────────
// Only start change streams after MongoDB is open.
// Change streams require a replica set — if we're in standalone mode they will
// fail gracefully without crashing the server.
mongoose.connection.once('open', () => {
  console.log('[DB] Connection open. Starting peer sync watchers...');

  const db = mongoose.connection.db;

  const watchCollection = (collectionName, eventName) => {
    try {
      const stream = db.collection(collectionName).watch();

      stream.on('change', (change) => {
        if (change.operationType === 'insert') {
          console.log(`[Sync] New ${collectionName} doc from peer — broadcasting '${eventName}'`);
          io.emit(eventName, change.fullDocument);
        }
      });

      stream.on('error', (err) => {
        // Change streams only work on replica sets.
        // In standalone mode this fires once — log and move on.
        if (err.codeName === 'CommandNotSupported' || err.code === 40573) {
          console.warn(`ℹ️  [Sync] Change streams not available in standalone mode (${collectionName}). Peer sync disabled until replica set is restored.`);
        } else {
          console.warn(`⚠️  [Sync] Change stream error on '${collectionName}': ${err.message}`);
        }
      });
    } catch (err) {
      console.warn(`⚠️  [Sync] Could not start watcher for '${collectionName}': ${err.message}`);
    }
  };

  watchCollection('products', 'peer-product-added');
  watchCollection('orders', 'peer-order-placed');
});

// ─── START SERVER ─────────────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', (err) => {
  if (err) console.error(err);
  console.log(`✅ Backend listening on PORT ${PORT} (ZeroTier + local network)`);
});
