// api/shared-chats.js
//
// FEATURE: چت مشترک - دو (یا چند) کاربر لاگین‌شده همزمان با هم و با ربات
// در یک گفتگو صحبت می‌کنند. کاملاً جدا از چت‌های شخصی (جدول‌های
// chats/chat_history) است؛ اینجا جدول‌های shared_chats/
// shared_chat_participants/shared_chat_messages/shared_chat_locks
// استفاده می‌شود (نگاه کن به schema_additions.sql).
//
// امنیت مثل api/chats.js: کلاینت token را در Authorization می‌فرستد،
// اینجا در جدول sessions بررسی و ایمیل معتبر آن استخراج می‌شود. کاربر
// روی هیچ عملیاتی (خواندن/نوشتن پیام) دسترسی ندارد مگر واقعاً عضو
// shared_chat_participants همان چت باشد - این چک همه‌جا تکرار می‌شود،
// چون اینجا (برخلاف چت شخصی) owner_email به‌تنهایی کافی نیست، هر
// عضوی (نه فقط سازنده) باید بتواند بخواند/بفرستد.
//
// GET  /api/shared-chats                                -> لیست چت‌های مشترکی که کاربر عضو آنهاست
// GET  /api/shared-chats?chatId=..&since=<id>            -> پیام‌های جدیدتر از id داده‌شده (polling)
// POST /api/shared-chats?action=create   body:{title?}   -> چت جدید + inviteCode
// POST /api/shared-chats?action=join     body:{inviteCode} -> عضو شدن با کد دعوت
// POST /api/shared-chats?action=send     body:{chatId,text} -> ارسال پیام + پاسخ Gemini
//
// نیازمندی‌های محیطی: همان SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY پروژه
// (نگاه کن به api/chats.js) + GEMINI_API_KEYS (یا GEMINI_API_KEY) برای
// پاسخ ربات.

const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';

const MAX_SHARED_CHATS_PER_USER = 50; // سقف امنیتی مشابه MAX_CHATS_PER_USER در api/chats.js
const MAX_MESSAGE_CHARS = 8000;
const MAX_MESSAGES_PER_POLL = 200;
const LOCK_STALE_MS = 30 * 1000; // اگر قفل قدیمی‌تر از این بود، یعنی درخواست قبلی هنگ/کرش کرده - نادیده‌اش می‌گیریم

function setCors(res) {
    res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// ===== همان الگوی api/chats.js: تأیید هویت با session token داخلی =====
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
        console.error('[shared-chats] verifySessionToken threw:', err?.message || err);
        return null;
    }
}

function getBearerToken(req) {
    const header = req.headers['authorization'] || '';
    const match = /^Bearer\s+(.+)$/i.exec(header);
    return match ? match[1] : null;
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

function generateChatId() {
    return crypto.randomBytes(16).toString('hex');
}

// کد دعوت کوتاه و خوانا (بدون کاراکترهای شبیه‌به‌هم مثل 0/O یا 1/I).
const INVITE_CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function generateInviteCode() {
    let code = '';
    for (let i = 0; i < 6; i++) {
        code += INVITE_CODE_ALPHABET[crypto.randomInt(0, INVITE_CODE_ALPHABET.length)];
    }
    return code;
}

// بررسی می‌کند که این ایمیل واقعاً عضو این چت مشترک است؛ همه‌جا قبل از
// خواندن/نوشتن صدا زده می‌شود چون این‌جا (برخلاف چت شخصی) مالکیت به
// چند نفر تعلق دارد، نه فقط owner_email.
async function isParticipant(chatId, email) {
    const resp = await supaFetch(
        `shared_chat_participants?chat_id=eq.${encodeURIComponent(chatId)}&email=eq.${encodeURIComponent(email)}&select=email`
    );
    if (!resp.ok) return false;
    const rows = await resp.json();
    return Array.isArray(rows) && rows.length > 0;
}

// ===== پاسخ ربات: یک generateContent ساده (بدون استریم/ابزار) با
// چرخش بین چند کلید API، دقیقاً هم‌الگو با تابع تولید عنوان در
// chat.js. چت مشترک برای شروع نیازی به search/file-edit ندارد. =====
async function getBotReply(historyForPrompt) {
    const geminiKeys = (process.env.GEMINI_API_KEYS || process.env.GEMINI_API_KEY || '')
        .split(',').map(k => k.trim()).filter(Boolean);
    if (!geminiKeys.length) {
        throw new Error('GEMINI_API_KEYS/GEMINI_API_KEY تنظیم نشده است.');
    }

    const systemText =
        'تو Virtual Bot هستی؛ دستیار هوش مصنوعی گرم و صمیمی به فارسی. ' +
        'در این گفتگو ممکن است بیش از یک نفر با تو صحبت کند - هر پیام با ' +
        'نام فرستنده مشخص شده؛ به هر دو نفر با توجه به کل زمینه‌ی گفتگو ' +
        'پاسخ بده، نه فقط آخرین پیام را جدا از بقیه در نظر بگیر.';

    for (const key of geminiKeys) {
        try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 25000);
            let response;
            try {
                response = await fetch(
                    'https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent',
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'x-goog-api-key': key },
                        body: JSON.stringify({
                            systemInstruction: { parts: [{ text: systemText }] },
                            contents: historyForPrompt
                        }),
                        signal: controller.signal
                    }
                );
            } finally {
                clearTimeout(timeoutId);
            }

            if (!response.ok) {
                // کلید بعدی را امتحان کن؛ جزئیات دقیق خطای هر کلید برای این
                // فیچر مهم نیست (برخلاف chat.js که کاربر مستقیم پیامش را
                // می‌بیند، اینجا فقط اگر همه‌ی کلیدها شکست خوردند خطا می‌دهیم).
                continue;
            }

            const data = await response.json();
            const text = data?.candidates?.[0]?.content?.parts?.map(p => p?.text || '').join('').trim();
            if (text) return text;
        } catch (_) {
            continue;
        }
    }

    throw new Error('پاسخ از سرویس هوش مصنوعی دریافت نشد.');
}

// قفل ساده روی chat_id: تلاش برای insert - چون chat_id همان‌جا primary
// key است، دو تلاش هم‌زمان فقط یکی‌شان موفق می‌شود (رقابت واقعی روی
// سطح دیتابیس حل می‌شود، نه فقط با چک‌کردن قبلی که خودش race دارد).
async function acquireLock(chatId, byEmail) {
    // قفل بیات (locked_at خیلی قدیمی) یعنی درخواست قبلی کرش/تایم‌اوت
    // کرده؛ آزادش می‌کنیم تا این چت برای همیشه گیر نکند.
    await supaFetch(
        `shared_chat_locks?chat_id=eq.${encodeURIComponent(chatId)}&locked_at=lt.${Date.now() - LOCK_STALE_MS}`,
        { method: 'DELETE' }
    );

    const resp = await supaFetch('shared_chat_locks', {
        method: 'POST',
        body: JSON.stringify([{ chat_id: chatId, locked_by: byEmail, locked_at: Date.now() }])
    });
    return resp.ok; // 201 یعنی قفل گرفته شد؛ 409 (unique violation) یعنی یکی دیگر مشغول است
}

async function releaseLock(chatId) {
    await supaFetch(`shared_chat_locks?chat_id=eq.${encodeURIComponent(chatId)}`, { method: 'DELETE' });
}

async function insertMessage(chatId, role, senderEmail, text) {
    const resp = await supaFetch('shared_chat_messages', {
        method: 'POST',
        headers: { 'Prefer': 'return=representation' },
        body: JSON.stringify([{
            chat_id: chatId,
            role,
            sender_email: senderEmail || null,
            text,
            created_at: Date.now()
        }])
    });
    const rows = await resp.json().catch(() => null);
    return Array.isArray(rows) && rows.length ? rows[0] : null;
}

module.exports = async function handler(req, res) {
    setCors(res);

    if (req.method === 'OPTIONS') {
        return res.status(200).end();
    }

    if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
        return res.status(500).json({ error: 'سرور برای چت مشترک تنظیم نشده (SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY موجود نیست).' });
    }

    const email = await verifySessionToken(getBearerToken(req));
    if (!email) {
        return res.status(401).json({ error: 'ورود تأیید نشد. دوباره وارد شو.' });
    }

    try {
        // ===== GET: لیست چت‌های مشترک کاربر، یا پیام‌های جدید یک چت =====
        if (req.method === 'GET') {
            const chatId = req.query?.chatId;

            if (chatId) {
                if (!(await isParticipant(chatId, email))) {
                    return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
                }
                const since = Number(req.query?.since) || 0;
                const msgsResp = await supaFetch(
                    `shared_chat_messages?chat_id=eq.${encodeURIComponent(chatId)}&id=gt.${since}&select=*&order=id.asc&limit=${MAX_MESSAGES_PER_POLL}`
                );
                const messages = await msgsResp.json();
                return res.status(200).json({ messages: Array.isArray(messages) ? messages : [] });
            }

            // لیست چت‌هایی که کاربر عضوشان است - از participants شروع
            // می‌کنیم (نه از shared_chats) چون معیار دسترسی عضویت است، نه
            // مالکیت.
            const partResp = await supaFetch(
                `shared_chat_participants?email=eq.${encodeURIComponent(email)}&select=chat_id`
            );
            const partRows = await partResp.json();
            const chatIds = Array.isArray(partRows) ? partRows.map(r => r.chat_id) : [];
            if (!chatIds.length) {
                return res.status(200).json({ items: [] });
            }
            const idsFilter = chatIds.map(id => encodeURIComponent(id)).join(',');
            const chatsResp = await supaFetch(
                `shared_chats?chat_id=in.(${idsFilter})&select=chat_id,title,owner_email,invite_code,updated_at&order=updated_at.desc`
            );
            const chats = await chatsResp.json();
            return res.status(200).json({ items: Array.isArray(chats) ? chats : [] });
        }

        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'متد پشتیبانی نمی‌شود.' });
        }

        const action = req.query?.action;

        // ===== POST action=create: ساخت چت مشترک جدید =====
        if (action === 'create') {
            const countResp = await supaFetch(
                `shared_chat_participants?email=eq.${encodeURIComponent(email)}&select=chat_id`,
                { headers: { 'Prefer': 'count=exact' } }
            );
            const countHeader = countResp.headers.get('content-range');
            const currentCount = countHeader ? parseInt(countHeader.split('/')[1], 10) || 0 : 0;
            if (currentCount >= MAX_SHARED_CHATS_PER_USER) {
                return res.status(403).json({ error: `حداکثر ${MAX_SHARED_CHATS_PER_USER} گفتگوی مشترک مجاز است.` });
            }

            const chatId = generateChatId();
            const inviteCode = generateInviteCode();
            const now = Date.now();
            const title = String(req.body?.title || 'گفتگوی مشترک').slice(0, 200);

            const createResp = await supaFetch('shared_chats', {
                method: 'POST',
                body: JSON.stringify([{
                    chat_id: chatId,
                    owner_email: email,
                    title,
                    invite_code: inviteCode,
                    created_at: now,
                    updated_at: now
                }])
            });
            if (!createResp.ok) {
                const errBody = await createResp.text().catch(() => '');
                console.error('[shared-chats] create failed:', errBody);
                return res.status(500).json({ error: 'ساخت گفتگوی مشترک ناموفق بود.' });
            }

            await supaFetch('shared_chat_participants', {
                method: 'POST',
                body: JSON.stringify([{ chat_id: chatId, email, joined_at: now }])
            });

            return res.status(200).json({ chatId, inviteCode, title });
        }

        // ===== POST action=join: پیوستن با کد دعوت =====
        if (action === 'join') {
            const inviteCode = String(req.body?.inviteCode || '').trim().toUpperCase();
            if (!inviteCode) {
                return res.status(400).json({ error: 'کد دعوت مشخص نشده.' });
            }

            const findResp = await supaFetch(
                `shared_chats?invite_code=eq.${encodeURIComponent(inviteCode)}&select=chat_id,title`
            );
            const findRows = await findResp.json();
            if (!Array.isArray(findRows) || !findRows.length) {
                return res.status(404).json({ error: 'کد دعوت معتبر نیست.' });
            }
            const chatId = findRows[0].chat_id;

            // اگر قبلاً عضو بوده، دوباره اضافه کردن خطا نمی‌دهد (merge-duplicates)
            // - یعنی می‌شود از کد دعوت هم برای join اول و هم به‌عنوان یک
            // لینک دعوت قابل استفاده‌ی مکرر برای همان عضو استفاده کرد.
            await supaFetch('shared_chat_participants', {
                method: 'POST',
                headers: { 'Prefer': 'resolution=merge-duplicates' },
                body: JSON.stringify([{ chat_id: chatId, email, joined_at: Date.now() }])
            });

            return res.status(200).json({ chatId, title: findRows[0].title });
        }

        // ===== POST action=send: ارسال پیام + پاسخ ربات =====
        if (action === 'send') {
            const chatId = String(req.body?.chatId || '').trim();
            const text = String(req.body?.text || '').trim();
            if (!chatId || !text) {
                return res.status(400).json({ error: 'chatId یا text مشخص نشده.' });
            }
            if (text.length > MAX_MESSAGE_CHARS) {
                return res.status(413).json({ error: `پیام نباید بیشتر از ${MAX_MESSAGE_CHARS} کاراکتر باشد.` });
            }
            if (!(await isParticipant(chatId, email))) {
                return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
            }

            // پیام کاربر همیشه فوری ذخیره می‌شود (حتی اگر بعداً قفل جواب
            // ربات را به تعویق بیندازد) - این‌طوری نفر دوم فوراً پیام
            // نفر اول را در نتیجه‌ی polling بعدی می‌بیند، بدون نیاز به
            // منتظر ماندن برای جواب ربات.
            const userMsg = await insertMessage(chatId, 'user', email, text);
            await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}`, {
                method: 'PATCH',
                body: JSON.stringify({ updated_at: Date.now() })
            });

            const gotLock = await acquireLock(chatId, email);
            if (!gotLock) {
                // یکی دیگر همین الان دارد پیام قبلی را پردازش می‌کند؛ پیام
                // این کاربر ذخیره شده و در نوبت polling بعدی هم دیده
                // می‌شود، ولی این درخواست خودش منتظر جواب ربات نمی‌ماند -
                // کلاینت با همان polling معمولی جواب را می‌بیند وقتی آماده شود.
                return res.status(200).json({ message: userMsg, botPending: true });
            }

            try {
                const historyResp = await supaFetch(
                    `shared_chat_messages?chat_id=eq.${encodeURIComponent(chatId)}&select=role,sender_email,text&order=id.asc&limit=100`
                );
                const historyRows = await historyResp.json();
                const historyForPrompt = (Array.isArray(historyRows) ? historyRows : []).map(row => ({
                    role: row.role === 'model' ? 'model' : 'user',
                    parts: [{
                        text: row.role === 'user' && row.sender_email
                            ? `[${row.sender_email}]: ${row.text}`
                            : row.text
                    }]
                }));

                const botText = await getBotReply(historyForPrompt);
                const botMsg = await insertMessage(chatId, 'model', null, botText);
                await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}`, {
                    method: 'PATCH',
                    body: JSON.stringify({ updated_at: Date.now() })
                });

                return res.status(200).json({ message: userMsg, botMessage: botMsg });
            } catch (err) {
                console.error('[shared-chats] bot reply failed:', err?.message || err);
                // پیام کاربر خودش با موفقیت ذخیره شده؛ فقط جواب ربات نرسید -
                // این را جدا اعلام می‌کنیم تا کلاینت پیام کاربر را از دست
                // ندهد، فقط بگوید «ربات جواب نداد، دوباره امتحان کن».
                return res.status(502).json({ message: userMsg, error: 'پاسخ ربات دریافت نشد.' });
            } finally {
                await releaseLock(chatId);
            }
        }

        return res.status(400).json({ error: 'action نامعتبر است.' });
    } catch (err) {
        console.error('[shared-chats] handler error:', err?.message || err);
        return res.status(500).json({ error: 'خطای داخلی سرور.', detail: err?.message || String(err) });
    }
};
