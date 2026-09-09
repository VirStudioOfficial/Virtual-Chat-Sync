// api/memory.js
//
// FEATURE (حافظه‌ی بلندمدت کاربر): وقتی کاربر یک اطلاعات شخصی/دائمی
// می‌گوید (اسم، مدل کارت گرافیک، رنگ مورد علاقه، یا هر چیز دیگری که
// خودِ مدل مهم تشخیص دهد)، مدل در همان پاسخ یک بلاک نامرئی
// widget-memory-save می‌سازد (به همان الگوی widget-clock/widget-weather/
// widget-suggestions در api/chat.js) و index.html بلافاصله همان لحظه با
// PUT اینجا ذخیره می‌کند - نه با تأخیر یا پردازش دوره‌ای جدا.
//
// همان الگوی احراز هویت و supaFetch که در api/preferences.js استفاده
// شده، اینجا هم عیناً تکرار شده (بدون وابستگی جدید npm).
//
// GET /api/memory                 -> تمام حافظه‌ی ذخیره‌شده‌ی کاربر
//     (برای تزریق به system prompt هر پیام، شبیه responsePreferenceSummary)
// PUT /api/memory                 -> ذخیره/به‌روزرسانی یک کلید
//     body: { key, value }

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
const MAX_KEY_SIZE = 200;       // کلید باید کوتاه باشد (مثل «رنگ_مورد_علاقه»، نه یک جمله)
const MAX_VALUE_SIZE = 2000;    // مقدار می‌تواند کمی بلندتر باشد ولی نباید کل پاسخ باشد
const MAX_KEYS_PER_USER = 200;  // سقف تعداد کلید برای هر کاربر تا این جدول رشد بی‌رویه نداشته باشد

function setCors(res) {
    res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
    res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// ===== همان الگوی تأیید هویت با session token داخلی که در api/preferences.js است =====
async function verifySessionToken(token) {
    if (!token) return null;
    try {
        const resp = await supaFetch(`sessions?token=eq.${encodeURIComponent(token)}&select=email,expires_at`);
        if (!resp.ok) return null;
        const rows = await resp.json();
        if (!Array.isArray(rows) || !rows.length) return null;
        const session = rows[0];
        if (Number(session.expires_at) < Date.now()) return null;
        return String(session.email).toLowerCase();
    } catch (err) {
        console.error('[AUTH] verifySessionToken threw:', err?.message || err);
        return null;
    }
}

function getBearerToken(req) {
    const header = req.headers['authorization'] || '';
    const match = /^Bearer\s+(.+)$/i.exec(header);
    return match ? match[1] : null;
}

async function supaFetch(path, options = {}) {
    const resp = await fetch(`${SUPABASE_URL}/rest/v1/${path}`, {
        ...options,
        headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': 'application/json',
            ...(options.headers || {})
        }
    });
    return resp;
}

module.exports = async function handler(req, res) {
    setCors(res);

    if (req.method === 'OPTIONS') {
        return res.status(200).end();
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
        return res.status(500).json({ error: 'سرور برای ذخیره‌ی حافظه تنظیم نشده (SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY موجود نیست).' });
    }

    const ownerEmail = await verifySessionToken(getBearerToken(req));
    if (!ownerEmail) {
        return res.status(401).json({ error: 'ورود تأیید نشد. دوباره وارد شو.' });
    }

    try {
        // ===== GET: تمام حافظه‌ی ذخیره‌شده‌ی این کاربر (برای تزریق به system prompt) =====
        if (req.method === 'GET') {
            const listRes = await supaFetch(
                `user_memory?owner_email=eq.${encodeURIComponent(ownerEmail)}&select=key,value&order=updated_at.desc`
            );
            const rows = await listRes.json();
            const memory = Array.isArray(rows)
                ? rows.map(r => ({ key: r.key, value: r.value }))
                : [];

            return res.status(200).json({ memory });
        }

        // ===== PUT: ذخیره/به‌روزرسانی یک کلید (upsert) =====
        if (req.method === 'PUT') {
            const { key, value } = req.body || {};

            if (typeof key !== 'string' || !key.trim()) {
                return res.status(400).json({ error: 'key مشخص نشده.' });
            }
            if (typeof value !== 'string' || !value.trim()) {
                return res.status(400).json({ error: 'value مشخص نشده.' });
            }
            const safeKey = key.trim().slice(0, MAX_KEY_SIZE);
            const safeValue = value.trim().slice(0, MAX_VALUE_SIZE);

            // FIX (جلوگیری از رشد بی‌رویه): اگر کاربر از قبل به سقف تعداد
            // کلید رسیده و این یک کلید کاملاً تازه است (نه به‌روزرسانی
            // یک کلید موجود)، درخواست را رد می‌کنیم - upsert روی کلید
            // موجود همیشه مجاز است چون تعداد ردیف را زیاد نمی‌کند.
            const existsRes = await supaFetch(
                `user_memory?owner_email=eq.${encodeURIComponent(ownerEmail)}&key=eq.${encodeURIComponent(safeKey)}&select=key`
            );
            const existsRows = await existsRes.json();
            const isNewKey = !(Array.isArray(existsRows) && existsRows.length);

            if (isNewKey) {
                const countRes = await supaFetch(
                    `user_memory?owner_email=eq.${encodeURIComponent(ownerEmail)}&select=key`,
                    { headers: { 'Prefer': 'count=exact' } }
                );
                const countHeader = countRes.headers.get('content-range') || '';
                const totalCount = Number((countHeader.split('/')[1] || '0'));
                if (totalCount >= MAX_KEYS_PER_USER) {
                    return res.status(429).json({ error: 'سقف تعداد اطلاعات ذخیره‌شده پر شده است.' });
                }
            }

            await supaFetch('user_memory?on_conflict=owner_email,key', {
                method: 'POST',
                headers: { 'Prefer': 'resolution=merge-duplicates' },
                body: JSON.stringify([{
                    owner_email: ownerEmail,
                    key: safeKey,
                    value: safeValue,
                    updated_at: Date.now()
                }])
            });

            return res.status(200).json({ ok: true });
        }

        return res.status(405).json({ error: 'متد پشتیبانی نمی‌شود.' });
    } catch (err) {
        console.error('memory handler error:', err?.message || err);
        return res.status(500).json({ error: 'خطای داخلی سرور.', detail: err?.message || String(err) });
    }
};
