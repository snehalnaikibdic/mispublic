// server.js
require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const bcrypt = require('bcrypt');
const crypto = require('crypto');
const nodemailer = require('nodemailer');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

// MongoDB Connection
mongoose.connect(process.env.MONGODB_URI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch((error) => console.error('MongoDB connection error:', error));

// User Schema and Model
const userSchema = new mongoose.Schema({
  idpId: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  mobile: { type: String, required: true },
  twoFACode: { type: String },
});

const User = mongoose.model('User', userSchema);

// Middleware for checking if IDP ID exists
const checkUserExists = async (req, res, next) => {
  const { idpId } = req.body;
  const user = await User.findOne({ idpId });
  if (!user) {
    return res.status(400).json({ message: 'Incorrect ID/password' });
  }
  req.user = user;
  next();
};

// Endpoint for Login
app.post('/login', checkUserExists, async (req, res) => {
  const { password, mobile } = req.body;
  const user = req.user;

  // Verify password
  const isPasswordCorrect = await bcrypt.compare(password, user.password);
  if (!isPasswordCorrect) {
    return res.status(400).json({ message: 'Incorrect ID/password' });
  }

  // Verify mobile number
  if (user.mobile !== mobile) {
    return res.status(400).json({ message: 'Incorrect mobile number' });
  }

  // Generate 2FA code
  const twoFACode = crypto.randomInt(100000, 999999).toString();
  user.twoFACode = twoFACode;
  await user.save();

  // Send 2FA code via email
  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: process.env.EMAIL_USER,
      pass: process.env.EMAIL_PASS,
    },
  });

  const mailOptions = {
    from: process.env.EMAIL_USER,
    to: user.idpId, // Assuming the IDP ID is an email
    subject: 'Your 2FA Code',
    text: `Your 2FA code is ${twoFACode}`,
  };

  transporter.sendMail(mailOptions, (error, info) => {
    if (error) {
      return res.status(500).json({ message: 'Error sending 2FA code' });
    }
    res.status(200).json({ message: '2FA code sent, please verify to login' });
  });
});

// Endpoint for Verifying 2FA Code
app.post('/verify-2fa', checkUserExists, async (req, res) => {
  const { twoFACode } = req.body;
  const user = req.user;

  if (user.twoFACode !== twoFACode) {
    return res.status(400).json({ message: 'Invalid 2FA code' });
  }

  // Clear 2FA code after successful verification
  user.twoFACode = null;
  await user.save();

  res.status(200).json({ message: 'Login successful' });
});

// Server listening
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
