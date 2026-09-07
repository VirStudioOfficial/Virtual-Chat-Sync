// api/preferences.js
//
// FEATURE (dual-response A/B learning - مرحله ۴/۵): ذخیره‌ی انتخاب کاربر
// بین دو پاسخ (الف/ب) در حالت dual-response و تحلیل الگوی این انتخاب‌ها
// برای ساخت یک خلاصه‌ی کوتاه از سلیقه‌ی کاربر که به system prompt تزریق
// می‌شود (مصرف‌کننده‌ی این خلاصه: فیلد responsePreferenceSummary که
// index.html قبل از فرستادن به api/chat.js می‌سازد).
//
// همان الگوی احراز هویت و supaFetch که در api/chats.js استفاده شده،
// اینجا هم عیناً تکرار شده (بدون وابستگی جدید npm).
//
// PUT  /api/preferences                 -> ذخیره‌ی یک انتخاب تازه
//      body: { chatId, userMessage, responseA, responseB, chosen: 'a'|'b' }
// GET  /api/preferences?action=analyze  -> خلاصه‌ی متنی کوتاه از الگوی
//      ترجیح کاربر (بر اساس N انتخاب اخیر) برای تزریق به system prompt

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
const MAX_MESSAGE_SIZE = 200 * 1024; // 200KB برای هر پیام/پاسخ - خیلی بالاتر از یک پیام معمولی
const ANALYZE_LOOKBACK = 20; // چند انتخاب اخیر برای ساخت خلاصه‌ی ترجیح در نظر گرفته شود

function setCors(res) {
    res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
    res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// ===== همان الگوی تأیید هویت با session token داخلی که در api/chats.js است =====
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

// خلاصه‌ی طول یک متن (تعداد کاراکتر و تعداد جمله‌ی تقریبی) - برای
// response_a_meta/response_b_meta؛ فقط برای تحلیل بعدی، نه نمایش به کاربر.
function textMeta(text) {
    const str = String(text || '');
    const sentenceCount = (str.match(/[.!؟?]+/g) || []).length;
    return { length: str.length, sentenceCount };
}

// از N انتخاب اخیر کاربر، یک خلاصه‌ی کوتاه فارسی می‌سازد که مستقیماً
// داخل system prompt قابل استفاده است. عمداً ساده و بر پایه‌ی آمار طول
// نگه داشته شده (نه یک مدل جدا) تا این endpoint سبک و سریع بماند.
function buildPreferenceSummary(rows) {
    if (!rows.length) return '';

    let chosenShorterCount = 0;
    let chosenLongerCount = 0;
    let totalChosenLength = 0;
    let aPicks = 0;
    let bPicks = 0;

    for (const row of rows) {
        const metaA = row.response_a_meta || {};
        const metaB = row.response_b_meta || {};
        const chosen = row.chosen;
        if (chosen === 'a') aPicks++; else if (chosen === 'b') bPicks++;

        const chosenLen = chosen === 'a' ? (metaA.length || 0) : (metaB.length || 0);
        const otherLen = chosen === 'a' ? (metaB.length || 0) : (metaA.length || 0);
        totalChosenLength += chosenLen;
        if (chosenLen && otherLen) {
            if (chosenLen < otherLen) chosenShorterCount++;
            else if (chosenLen > otherLen) chosenLongerCount++;
        }
    }

    const avgLength = Math.round(totalChosenLength / rows.length);
    const lengthPref =
        chosenShorterCount > chosenLongerCount
            ? 'کاربر معمولاً پاسخ کوتاه‌تر و مستقیم‌تر را ترجیح می‌دهد.'
            : chosenLongerCount > chosenShorterCount
                ? 'کاربر معمولاً پاسخ کامل‌تر و توضیح‌دارتر را ترجیح می‌دهد.'
                : 'ترجیح مشخصی بین پاسخ کوتاه و بلند دیده نشده.';

    return `${lengthPref} (میانگین طول پاسخ‌های انتخاب‌شده: تقریباً ${avgLength} کاراکتر، بر اساس ${rows.length} انتخاب اخیر)`;
}

module.exports = async function handler(req, res) {
    setCors(res);

    if (req.method === 'OPTIONS') {
        return res.status(200).end();
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
        return res.status(500).json({ error: 'سرور برای ذخیره‌ی ترجیحات تنظیم نشده (SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY موجود نیست).' });
    }

    const ownerEmail = await verifySessionToken(getBearerToken(req));
    if (!ownerEmail) {
        return res.status(401).json({ error: 'ورود تأیید نشد. دوباره وارد شو.' });
    }

    try {
        // ===== PUT: ذخیره‌ی یک انتخاب تازه (کاربر بین پاسخ الف/ب یکی را زد) =====
        if (req.method === 'PUT') {
            const { chatId, userMessage, responseA, responseB, chosen } = req.body || {};

            if (!chatId || typeof chatId !== 'string') {
                return res.status(400).json({ error: 'chatId مشخص نشده.' });
            }
            if (chosen !== 'a' && chosen !== 'b') {
                return res.status(400).json({ error: 'chosen باید a یا b باشد.' });
            }
            const safeUserMessage = typeof userMessage === 'string' ? userMessage : '';
            const safeResponseA = typeof responseA === 'string' ? responseA : '';
            const safeResponseB = typeof responseB === 'string' ? responseB : '';

            if (
                safeUserMessage.length > MAX_MESSAGE_SIZE ||
                safeResponseA.length > MAX_MESSAGE_SIZE ||
                safeResponseB.length > MAX_MESSAGE_SIZE
            ) {
                return res.status(413).json({ error: 'متن ارسالی خیلی بزرگ است.' });
            }

            await supaFetch('response_preferences', {
                method: 'POST',
                body: JSON.stringify([{
                    owner_email: ownerEmail,
                    chat_id: String(chatId),
                    user_message: safeUserMessage,
                    response_a: safeResponseA,
                    response_b: safeResponseB,
                    chosen: chosen,
                    response_a_meta: textMeta(safeResponseA),
                    response_b_meta: textMeta(safeResponseB),
                    created_at: Date.now()
                }])
            });

            return res.status(200).json({ ok: true });
        }

        // ===== GET: خلاصه‌ی تحلیل‌شده‌ی ترجیحات اخیر کاربر =====
        if (req.method === 'GET') {
            const action = req.query?.action;
            if (action !== 'analyze') {
                return res.status(400).json({ error: 'اکشن نامعتبر است.' });
            }

            const listRes = await supaFetch(
                `response_preferences?owner_email=eq.${encodeURIComponent(ownerEmail)}&select=chosen,response_a_meta,response_b_meta&order=created_at.desc&limit=${ANALYZE_LOOKBACK}`
            );
            const rows = await listRes.json();
            const summary = buildPreferenceSummary(Array.isArray(rows) ? rows : []);

            return res.status(200).json({ summary, basedOn: Array.isArray(rows) ? rows.length : 0 });
        }

        return res.status(405).json({ error: 'متد پشتیبانی نمی‌شود.' });
    } catch (err) {
        console.error('preferences handler error:', err?.message || err);
        return res.status(500).json({ error: 'خطای داخلی سرور.', detail: err?.message || String(err) });
    }
};
