// api/auth.js
//
// جایگزین کامل Google OAuth: کاربر با ایمیل/پسورد دلخواه خودش ثبت‌نام و
// لاگین می‌کند. پسورد هرگز خام ذخیره نمی‌شود - فقط هش bcrypt آن.
//
// حالا یک لایه‌ی تایید ایمیل هم اضافه شده:
//   - register: به‌جای ساخت مستقیم یوزر، یک کد ۶ رقمی ساخته و با Resend
//     به ایمیل کاربر ارسال می‌شود. کاربر واقعی در جدول users فقط بعد از
//     verify ساخته می‌شود.
//   - login: اگر device_id ارسالی برای این ایمیل قبلاً دیده نشده باشد
//     (دستگاه جدید)، به‌جای توکن، یک کد تایید فرستاده می‌شود.
//
// POST /api/auth?action=register        body: { email, password, deviceId }
//      -> { needsVerification: true, purpose: 'register' }
// POST /api/auth?action=login           body: { email, password, deviceId }
//      -> { token, email }  یا  { needsVerification: true, purpose: 'login' }
// POST /api/auth?action=verify          body: { email, code, purpose, deviceId }
//      -> { token, email }
// POST /api/auth?action=resend-code     body: { email, purpose, deviceId }
//      -> { ok: true }
//
// نیازمندی‌های محیطی:
//   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
//   GMAIL_USER (آدرس جیمیلی که ازش ایمیل می‌فرستیم)
//   GMAIL_APP_PASSWORD (App Password ۱۶ رقمی، نه پسورد اصلی جیمیل)

const bcrypt = require('bcryptjs');
const crypto = require('crypto');
const nodemailer = require('nodemailer');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const GMAIL_USER = process.env.GMAIL_USER;
const GMAIL_APP_PASSWORD = process.env.GMAIL_APP_PASSWORD;

let cachedTransporter = null;
function getTransporter() {
    if (!cachedTransporter) {
        cachedTransporter = nodemailer.createTransport({
            service: 'gmail',
            auth: { user: GMAIL_USER, pass: GMAIL_APP_PASSWORD }
        });
    }
    return cachedTransporter;
}

const SESSION_TTL_MS = 90 * 24 * 60 * 60 * 1000; // ۹۰ روز
const CODE_TTL_MS = 15 * 60 * 1000; // ۱۵ دقیقه
const MAX_ATTEMPTS = 5; // حداکثر تلاش غلط برای هر کد
const RESEND_COOLDOWN_MS = 60 * 1000; // حداقل فاصله بین دو ارسال کد

function setCors(res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

async function supaFetch(path, options = {}) {
    return fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
        ...options,
        headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': 'application/json',
            ...(options.headers || {})
        }
    });
}

function normalizeEmail(email) {
    return String(email || '').trim().toLowerCase();
}

function isValidEmailShape(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function normalizeDeviceId(deviceId) {
    const id = String(deviceId || '').trim();
    return id.slice(0, 128) || null;
}

function generateCode() {
    return String(crypto.randomInt(0, 1000000)).padStart(6, '0');
}

function hashCode(code) {
    return crypto.createHash('sha256').update(code).digest('hex');
}

async function sendVerificationEmail(email, code, purpose) {
    if (!GMAIL_USER || !GMAIL_APP_PASSWORD) {
        throw new Error('GMAIL_USER/GMAIL_APP_PASSWORD تنظیم نشده است.');
    }
    const subject = purpose === 'register'
        ? 'کد تایید ثبت‌نام VirtualChat'
        : 'کد تایید ورود از دستگاه جدید';
    await getTransporter().sendMail({
        from: `VirtualChat <${GMAIL_USER}>`,
        to: email,
        subject,
        html: `
            <div dir="rtl" style="font-family: sans-serif; text-align: center; padding: 24px;">
                <h2>${subject}</h2>
                <p>کد تایید شما:</p>
                <p style="font-size: 32px; font-weight: bold; letter-spacing: 8px;">${code}</p>
                <p style="color: #666;">این کد تا ۱۵ دقیقه دیگر معتبر است. اگر این درخواست را نداده‌اید، این ایمیل را نادیده بگیرید.</p>
            </div>
        `
    });
}

module.exports = async function handler(req, res) {
    setCors(res);
    if (req.method === 'OPTIONS') return res.status(200).end();
    if (req.method !== 'POST') {
        return res.status(405).json({ error: 'متد پشتیبانی نمی‌شود.' });
    }
    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
        return res.status(500).json({ error: 'سرور تنظیم نشده (SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY موجود نیست).' });
    }

    const action = req.query?.action;
    const email = normalizeEmail(req.body?.email);
    const deviceId = normalizeDeviceId(req.body?.deviceId);

    if (!isValidEmailShape(email)) {
        return res.status(400).json({ error: 'فرمت ایمیل معتبر نیست.' });
    }

    try {
        if (action === 'register') {
            const password = String(req.body?.password || '');
            if (password.length < 6) {
                return res.status(400).json({ error: 'پسورد باید حداقل ۶ کاراکتر باشد.' });
            }

            const existingResp = await supaFetch(`users?email=eq.${encodeURIComponent(email)}&select=email`);
            const existing = await existingResp.json();
            if (Array.isArray(existing) && existing.length) {
                return res.status(409).json({ error: 'این ایمیل قبلاً ثبت شده. اگر خودتی، وارد شو.' });
            }

            const passwordHash = await bcrypt.hash(password, 10);
            await createAndSendCode({ email, purpose: 'register', passwordHash, deviceId });
            return res.status(200).json({ needsVerification: true, purpose: 'register' });
        }

        if (action === 'login') {
            const password = String(req.body?.password || '');

            const rowsResp = await supaFetch(`users?email=eq.${encodeURIComponent(email)}&select=password_hash`);
            const rows = await rowsResp.json();
            if (!Array.isArray(rows) || !rows.length) {
                return res.status(401).json({ error: 'ایمیل یا پسورد اشتباه است.' });
            }

            const isMatch = await bcrypt.compare(password, rows[0].password_hash);
            if (!isMatch) {
                return res.status(401).json({ error: 'ایمیل یا پسورد اشتباه است.' });
            }

            const isKnownDevice = deviceId ? await isDeviceKnown(email, deviceId) : false;
            if (isKnownDevice) {
                const token = await createSession(email);
                return res.status(200).json({ token, email });
            }

            await createAndSendCode({ email, purpose: 'login', deviceId });
            return res.status(200).json({ needsVerification: true, purpose: 'login' });
        }

        if (action === 'verify') {
            const code = String(req.body?.code || '').trim();
            const purpose = req.body?.purpose === 'register' ? 'register' : 'login';
            if (!/^\d{6}$/.test(code)) {
                return res.status(400).json({ error: 'کد باید ۶ رقم باشد.' });
            }

            const pending = await getLatestPending(email, purpose);
            if (!pending) {
                return res.status(400).json({ error: 'کد منقضی شده یا وجود ندارد. یک کد جدید بگیرید.' });
            }
            if (Date.now() > Number(pending.expires_at)) {
                await deletePending(pending.id);
                return res.status(400).json({ error: 'کد منقضی شده است. یک کد جدید بگیرید.' });
            }
            if (pending.attempts >= MAX_ATTEMPTS) {
                await deletePending(pending.id);
                return res.status(429).json({ error: 'تعداد تلاش‌های مجاز تمام شد. یک کد جدید بگیرید.' });
            }

            if (hashCode(code) !== pending.code_hash) {
                await incrementAttempts(pending.id, pending.attempts);
                return res.status(401).json({ error: 'کد نادرست است.' });
            }

            await deletePending(pending.id);

            if (purpose === 'register') {
                const createUserResp = await supaFetch('users', {
                    method: 'POST',
                    body: JSON.stringify([{ email, password_hash: pending.password_hash, created_at: Date.now() }])
                });
                if (!createUserResp.ok) {
                    const errBody = await createUserResp.text().catch(() => '');
                    console.error('[auth] register insert failed:', errBody);
                    return res.status(500).json({ error: 'ثبت‌نام ناموفق بود.' });
                }
            }

            if (deviceId) {
                await rememberDevice(email, deviceId);
            }

            const token = await createSession(email);
            return res.status(200).json({ token, email });
        }

        if (action === 'resend-code') {
            const purpose = req.body?.purpose === 'register' ? 'register' : 'login';
            const pending = await getLatestPending(email, purpose);
            if (pending && (Date.now() - Number(pending.created_at)) < RESEND_COOLDOWN_MS) {
                return res.status(429).json({ error: 'کمی صبر کن و دوباره درخواست بده.' });
            }
            if (!pending) {
                return res.status(400).json({ error: 'درخواست تاییدی برای این ایمیل پیدا نشد. دوباره از اول امتحان کن.' });
            }
            await deletePending(pending.id);
            await createAndSendCode({
                email,
                purpose,
                passwordHash: pending.password_hash || undefined,
                deviceId: pending.device_id || deviceId
            });
            return res.status(200).json({ ok: true });
        }

        return res.status(400).json({ error: 'action نامعتبر است.' });
    } catch (err) {
        console.error('[auth] error:', err?.message || err);
        return res.status(500).json({ error: 'خطای داخلی سرور.' });
    }
};

async function createSession(email) {
    const token = crypto.randomBytes(32).toString('hex');
    const now = Date.now();
    await supaFetch('sessions', {
        method: 'POST',
        body: JSON.stringify([{ token, email, created_at: now, expires_at: now + SESSION_TTL_MS }])
    });
    return token;
}

async function createAndSendCode({ email, purpose, passwordHash, deviceId }) {
    const code = generateCode();
    const now = Date.now();
    const insertResp = await supaFetch('pending_verifications', {
        method: 'POST',
        body: JSON.stringify([{
            email,
            code_hash: hashCode(code),
            purpose,
            password_hash: passwordHash || null,
            device_id: deviceId || null,
            attempts: 0,
            created_at: now,
            expires_at: now + CODE_TTL_MS
        }])
    });
    if (!insertResp.ok) {
        const errBody = await insertResp.text().catch(() => '');
        throw new Error(`ثبت کد تایید ناموفق بود: ${errBody}`);
    }
    await sendVerificationEmail(email, code, purpose);
}

async function getLatestPending(email, purpose) {
    const resp = await supaFetch(
        `pending_verifications?email=eq.${encodeURIComponent(email)}&purpose=eq.${purpose}&order=created_at.desc&limit=1`
    );
    const rows = await resp.json();
    return Array.isArray(rows) && rows.length ? rows[0] : null;
}

async function deletePending(id) {
    await supaFetch(`pending_verifications?id=eq.${id}`, { method: 'DELETE' });
}

async function incrementAttempts(id, currentAttempts) {
    await supaFetch(`pending_verifications?id=eq.${id}`, {
        method: 'PATCH',
        body: JSON.stringify({ attempts: currentAttempts + 1 })
    });
}

async function isDeviceKnown(email, deviceId) {
    const resp = await supaFetch(
        `known_devices?email=eq.${encodeURIComponent(email)}&device_id=eq.${encodeURIComponent(deviceId)}&select=device_id`
    );
    const rows = await resp.json();
    return Array.isArray(rows) && rows.length > 0;
}

async function rememberDevice(email, deviceId) {
    await supaFetch('known_devices', {
        method: 'POST',
        headers: { 'Prefer': 'resolution=merge-duplicates' },
        body: JSON.stringify([{ email, device_id: deviceId, created_at: Date.now() }])
    });
}
