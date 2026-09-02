// api/auth.js
//
// جایگزین کامل Google OAuth: کاربر با ایمیل/پسورد دلخواه خودش (نه لزوماً
// ایمیل واقعی) ثبت‌نام و لاگین می‌کند. پسورد هرگز خام ذخیره نمی‌شود -
// فقط هش bcrypt آن در جدول users می‌رود. بعد از لاگین موفق، یک session
// token تصادفی (نه ایمیل/پسورد) برمی‌گردد که کلاینت در هر درخواست بعدی
// (به api/chats.js) در هدر Authorization می‌فرستد.
//
// POST /api/auth?action=register  body: { email, password } -> { token, email }
// POST /api/auth?action=login     body: { email, password } -> { token, email }
//
// نیازمندی‌های محیطی: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

const bcrypt = require('bcryptjs');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SESSION_TTL_MS = 90 * 24 * 60 * 60 * 1000; // ۹۰ روز - چون کاربر دیگر لازم نیست هر ساعت دوباره وارد شود

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
    // فقط یک چک شکلی ساده - این ایمیل لازم نیست واقعاً وجود داشته باشد،
    // فقط به‌عنوان شناسه‌ی یکتای کاربر استفاده می‌شود.
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
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
    const password = String(req.body?.password || '');

    if (!isValidEmailShape(email)) {
        return res.status(400).json({ error: 'فرمت ایمیل معتبر نیست.' });
    }
    if (password.length < 6) {
        return res.status(400).json({ error: 'پسورد باید حداقل ۶ کاراکتر باشد.' });
    }

    try {
        if (action === 'register') {
            const existingResp = await supaFetch(`users?email=eq.${encodeURIComponent(email)}&select=email`);
            const existing = await existingResp.json();
            if (Array.isArray(existing) && existing.length) {
                return res.status(409).json({ error: 'این ایمیل قبلاً ثبت شده. اگر خودتی، وارد شو.' });
            }

            // FIX: هزینه‌ی هش (salt rounds) روی ۱۰ گذاشته شده - تعادل استاندارد
            // بین امنیت و سرعت برای این مقیاس کاربر.
            const passwordHash = await bcrypt.hash(password, 10);
            const createUserResp = await supaFetch('users', {
                method: 'POST',
                body: JSON.stringify([{ email, password_hash: passwordHash, created_at: Date.now() }])
            });
            if (!createUserResp.ok) {
                const errBody = await createUserResp.text().catch(() => '');
                console.error('[auth] register insert failed:', errBody);
                return res.status(500).json({ error: 'ثبت‌نام ناموفق بود.' });
            }

            const token = await createSession(email);
            return res.status(200).json({ token, email });
        }

        if (action === 'login') {
            const rowsResp = await supaFetch(`users?email=eq.${encodeURIComponent(email)}&select=password_hash`);
            const rows = await rowsResp.json();
            if (!Array.isArray(rows) || !rows.length) {
                return res.status(401).json({ error: 'ایمیل یا پسورد اشتباه است.' });
            }

            const isMatch = await bcrypt.compare(password, rows[0].password_hash);
            if (!isMatch) {
                return res.status(401).json({ error: 'ایمیل یا پسورد اشتباه است.' });
            }

            const token = await createSession(email);
            return res.status(200).json({ token, email });
        }

        return res.status(400).json({ error: 'action نامعتبر است (register یا login باشد).' });
    } catch (err) {
        console.error('[auth] error:', err?.message || err);
        return res.status(500).json({ error: 'خطای داخلی سرور.' });
    }
};

async function createSession(email) {
    // FIX: توکن با crypto.randomBytes ساخته می‌شود - یعنی کاملاً تصادفی و
    // غیرقابل‌حدس است (نه چیزی مشتق از ایمیل یا زمان که قابل پیش‌بینی باشد).
    const token = crypto.randomBytes(32).toString('hex');
    const now = Date.now();
    await supaFetch('sessions', {
        method: 'POST',
        body: JSON.stringify([{ token, email, created_at: now, expires_at: now + SESSION_TTL_MS }])
    });
    return token;
}
