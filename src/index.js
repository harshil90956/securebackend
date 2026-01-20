import express from 'express';
import mongoose from 'mongoose';
import cors from 'cors';
import dotenv from 'dotenv';
import bcrypt from 'bcryptjs';

import User from './models/User.js';

import authRoutes from './routes/auth.js';
import adminRoutes from './routes/admin.js';
import adminUsersRoutes from './routes/adminUsers.js';
import docsRoutes from './routes/docs.js';
import pdfRoutes from './routes/pdfRoutes.js';
import securityRoutes from './routes/security.js';

import {
  ipSecurity,
  checkLoginAttempts,
  checkIPWhitelist,
} from './middleware/ipSecurity.js';

// Load environment variables
dotenv.config();

const app = express();
const PORT = process.env.PORT || 4000;

// Middlewares
app.use(cors());
app.use(express.json({ limit: '900mb' }));
app.use(express.urlencoded({ extended: true, limit: '900mb' }));

// Security middlewares (same as old working behavior)
app.use(ipSecurity);
app.use(checkLoginAttempts);

// Routes
app.use('/api/auth', authRoutes);

app.use(checkIPWhitelist);
app.use('/api/security', securityRoutes);
app.use('/api/admin', adminRoutes);
app.use('/api/admin', adminUsersRoutes);

app.use('/api/docs', docsRoutes);
app.use('/api', pdfRoutes);

// Health check
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok' });
});

// Ensure admin user exists
async function ensureAdminUser() {
  const adminEmail = 'akshit@gmail.com';
  const adminPassword = 'akshit';

  const existing = await User.findOne({
    email: adminEmail.toLowerCase(),
  });

  if (existing) {
    console.log('Admin user already exists');
    return;
  }

  const passwordHash = await bcrypt.hash(adminPassword, 10);

  await User.create({
    email: adminEmail.toLowerCase(),
    passwordHash,
    role: 'admin',
  });

  console.log('Admin user created:', adminEmail);
}

// Start server
async function start() {
  try {
    const mongoUri =
      process.env.MONGO_URI ||
      'mongodb+srv://gajeraakshit53_db_user:lvbGcIFW0ul5Bao6@akshit.thyfwea.mongodb.net/securepdf?retryWrites=true&w=majority';

    if (!mongoUri) {
      console.error('MONGO_URI is not set in environment');
      process.exit(1);
    }

    await mongoose.connect(mongoUri);
    console.log('Connected to MongoDB');

    await ensureAdminUser();

    app.listen(PORT, () => {
      console.log(`Backend listening on port ${PORT}`);
    });
  } catch (err) {
    console.error('Failed to start backend', err);
    process.exit(1);
  }
}

start();
