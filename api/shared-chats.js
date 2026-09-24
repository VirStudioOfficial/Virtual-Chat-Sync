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
// POST /api/shared-chats?action=create   body:{title?, model?} -> چت جدید + inviteCode (مدل فقط همین‌جا و توسط سازنده تعیین می‌شود)
// POST /api/shared-chats?action=join     body:{inviteCode} -> عضو شدن با کد دعوت
// POST /api/shared-chats?action=upload   body:{chatId,base64,contentType,name} -> آپلود یک عکس به Supabase Storage (فقط اعضا)
// POST /api/shared-chats?action=send     body:{chatId,text?,attachments?:[{path}]} -> ارسال پیام (+عکس) + پاسخ Gemini (غیر-استریم، سازگاری قدیمی)
// POST /api/shared-chats?action=stream   body:{chatId,text?,attachments?:[{path}]} -> FEATURE: مثل send ولی پاسخ ربات را با
//   Server-Sent Events تکه‌تکه پخش می‌کند (دقیقاً همون الگوی pages/api/chat.js: خط‌های
//   `data: {...}\n\n`؛ رویدادهای ممکن: {userMessage:{...}} یک‌بار در همون اول،
//   {text:"..."} به ازای هر تکه، و در پایان {done:true, botMessage:{...}}
//   یا {done:true, botPending:true} یا {error:"..."}). کلاینت‌های قدیمی‌تر که
//   هنوز از action=send استفاده می‌کنند دست‌نخورده کار می‌کنند.
// GET  /api/shared-chats?action=download&chatId=..&path=.. -> دانلود یک عکس (فقط اعضا؛ پاسخ: {base64,contentType})
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

// ===== عکس در چت مشترک (Supabase Storage) =====
// از همان باکت چت‌های شخصی استفاده می‌کنیم ولی زیر پیشوند جدا (shared/)
// تا هیچ تداخلی با مسیرهای <email>/<chatId>/... در api/chats.js نباشد.
const STORAGE_BUCKET = 'chat-attachments';
const MAX_UPLOAD_SIZE = 5 * 1024 * 1024;        // 5MB برای هر عکس (باکت رایگان 1GB است)
const MAX_ATTACHMENTS_PER_MESSAGE = 4;
const ALLOWED_IMAGE_TYPES = ['image/png', 'image/jpeg', 'image/webp', 'image/gif'];
// فقط عکس‌های این‌قدر پیام اخیر برای ربات فرستاده می‌شود؛ وگرنه با یک
// چت پر از عکس، حجم درخواست به Gemini (و زمان پاسخ) بی‌رویه بالا می‌رود.
const MAX_IMAGES_TO_BOT = 6;

// ===== Gemini engine (اقتباس‌شده از معماری pages/api/chat.js) =====
// Shared Chat به agent/toolهای چت شخصی نیاز ندارد، اما لایه‌ی ارتباط با Gemini
// همان اصول را دارد: classify خطا، health-aware key rotation، deadline مشترک،
// AbortController، retry هوشمند و model fallback.
const MAX_ATTEMPTS_PER_KEY = 2;
const BOT_PER_CALL_TIMEOUT_MS = 7000;
const BOT_TOTAL_BUDGET_MIN_MS = 12000;
const BOT_TOTAL_BUDGET_MAX_MS = 28000;
const BOT_RETRY_DELAY_MS = 250;

// ===== مدل ثابت هر چت مشترک =====
const ALLOWED_MODELS = [
    'gemini-3.8-flash',
    'gemini-3.5-flash-lite',
    'gemini-3.1-pro-preview',
    'gemini-3.6-flash'
];
const DEFAULT_MODEL = 'gemini-3.8-flash';

function getGeminiKeys() {
    return (process.env.GEMINI_API_KEYS || process.env.GEMINI_API_KEY || '')
        .split(',')
        .map(k => k.trim())
        .filter(Boolean);
}

function classifyGeminiError(error) {
    const status = Number(
        error?.status ??
        error?.error?.code ??
        error?.body?.status ??
        error?.body?.error?.code ??
        0
    ) || null;
    const providerCode =
        error?.error?.status ||
        error?.body?.error?.status ||
        error?.statusText ||
        null;
    const rawMessage = String(
        error?.message ||
        error?.error?.message ||
        error?.body?.message ||
        error?.body?.error?.message ||
        ''
    ).trim();
    const normalized = `${providerCode || ''} ${rawMessage}`.toLowerCase();

    if (error?.name === 'AbortError' || /timeout|timed out|deadline exceeded/.test(normalized)) {
        return { category: 'timeout', retryable: true, keySpecific: false, status, providerCode, rawMessage };
    }
    if (status === 429 || /resource_exhausted|quota|rate.?limit|too many requests/.test(normalized)) {
        const quota = /free.?tier|daily.?quota|quota.?exceeded|exceeded your current quota/.test(normalized);
        return {
            category: quota ? 'quota_exhausted' : 'rate_limit',
            retryable: true,
            keySpecific: true,
            status: status || 429,
            providerCode,
            rawMessage
        };
    }
    if (status === 401 || /api key|invalid.*key|unauthenticated|authentication/.test(normalized)) {
        return { category: 'invalid_api_key', retryable: true, keySpecific: true, status: status || 401, providerCode, rawMessage };
    }
    if (status === 403 || /permission|forbidden|access denied|not authorized/.test(normalized)) {
        return { category: 'permission_denied', retryable: true, keySpecific: true, status: status || 403, providerCode, rawMessage };
    }
    if (status === 404 || /model.*not found|not_found|unknown model/.test(normalized)) {
        return { category: 'model_not_found', retryable: true, keySpecific: false, status: status || 404, providerCode, rawMessage };
    }
    if (status === 400 || /invalid argument|invalid request|bad request|malformed/.test(normalized)) {
        return { category: 'invalid_request', retryable: false, keySpecific: false, status: status || 400, providerCode, rawMessage };
    }
    if (status === 413 || /too large|payload.*large|request.*size|token limit|context length/.test(normalized)) {
        return { category: 'request_too_large', retryable: false, keySpecific: false, status: status || 413, providerCode, rawMessage };
    }
    if ((status >= 500 && status <= 599) || /service unavailable|internal server error|bad gateway|temporarily unavailable/.test(normalized)) {
        return { category: 'provider_unavailable', retryable: true, keySpecific: false, status, providerCode, rawMessage };
    }
    if (error instanceof TypeError || /fetch failed|network|socket|econn|enotfound|connection/.test(normalized)) {
        return { category: 'network_error', retryable: true, keySpecific: false, status, providerCode, rawMessage };
    }
    return { category: 'unknown_error', retryable: true, keySpecific: false, status, providerCode, rawMessage };
}

// Per-process health state; intentionally stores the key itself only in memory.
// Nothing here is logged or persisted.
const __sharedKeyFailureCounts = new Map();

function rotateKeysByHealth(keys) {
    const shuffled = keys
        .map(k => ({ k, r: Math.random() }))
        .sort((a, b) => a.r - b.r)
        .map(x => x.k);
    return shuffled.sort((a, b) =>
        (__sharedKeyFailureCounts.get(a) || 0) - (__sharedKeyFailureCounts.get(b) || 0)
    );
}

function markKeyResult(key, ok) {
    if (ok) __sharedKeyFailureCounts.set(key, 0);
    else __sharedKeyFailureCounts.set(key, (__sharedKeyFailureCounts.get(key) || 0) + 1);
}

function keyLabel(keys, key) {
    return `key#${keys.indexOf(key) + 1}/${keys.length}`;
}

function getModelFallbacks(model) {
    const selected = ALLOWED_MODELS.includes(model) ? model : DEFAULT_MODEL;
    const fallbacks = [selected];
    if (selected === 'gemini-3.1-pro-preview') {
        fallbacks.push('gemini-3.8-flash', 'gemini-3.5-flash-lite');
    } else if (selected === 'gemini-3.8-flash' || selected === 'gemini-3.6-flash') {
        fallbacks.push('gemini-3.5-flash-lite');
    }
    return [...new Set(fallbacks)];
}

function getBotDeadlineMs(keyCount, modelCount = 1) {
    // Unlike chat.js, Shared Chat's HTTP client historically expects a short
    // response window. We therefore copy the architecture, but keep the
    // deadline below that client ceiling. More keys/models get more budget,
    // but never beyond BOT_TOTAL_BUDGET_MAX_MS.
    const attempts = Math.max(1, keyCount * modelCount);
    return Math.min(
        BOT_TOTAL_BUDGET_MAX_MS,
        Math.max(BOT_TOTAL_BUDGET_MIN_MS, 3500 + attempts * 1800)
    );
}

function makeGeminiRequestBody(historyForPrompt) {
    return {
        systemInstruction: { parts: [{ text: SHARED_CHAT_SYSTEM_TEXT }] },
        contents: historyForPrompt
    };
}

function parseGeminiText(data) {
    return data?.candidates?.[0]?.content?.parts?.map(p => p?.text || '').join('') || '';
}

function makeHttpGeminiError(status, bodyText) {
    let parsed = null;
    try { parsed = JSON.parse(bodyText); } catch (_) {}
    const err = new Error(parsed?.error?.message || `HTTP ${status}`);
    err.status = status;
    err.statusText = parsed?.error?.status || null;
    err.body = parsed;
    return err;
}

function isPermanentEmptyReason(reason) {
    return ['SAFETY', 'RECITATION', 'PROHIBITED_CONTENT'].some(x => String(reason || '').includes(x));
}

async function getBotReply(historyForPrompt, model) {
    const keys = getGeminiKeys();
    if (!keys.length) throw new Error('GEMINI_API_KEYS/GEMINI_API_KEY تنظیم نشده است.');

    const models = getModelFallbacks(model);
    const startedAt = Date.now();
    const overallDeadline = startedAt + getBotDeadlineMs(keys.length, models.length);
    const failures = [];
    let lastError = null;

    outer:
    for (const currentModel of models) {
        const orderedKeys = rotateKeysByHealth(keys);
        for (const currentKey of orderedKeys) {
            if (Date.now() >= overallDeadline) break outer;
            for (let attempt = 1; attempt <= MAX_ATTEMPTS_PER_KEY; attempt++) {
                const remaining = Math.max(0, overallDeadline - Date.now());
                if (remaining < 1000) break outer;

                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), Math.min(BOT_PER_CALL_TIMEOUT_MS, remaining));
                try {
                    const response = await fetch(
                        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(currentModel)}:generateContent`,
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'x-goog-api-key': currentKey
                            },
                            body: JSON.stringify(makeGeminiRequestBody(historyForPrompt)),
                            signal: controller.signal
                        }
                    );
                    if (!response.ok) {
                        const body = await response.text().catch(() => '');
                        const err = makeHttpGeminiError(response.status, body);
                        const classified = classifyGeminiError(err);
                        failures.push(`${keyLabel(keys, currentKey)} try${attempt}: HTTP ${response.status} ${body.slice(0, 240).replace(/\s+/g, ' ')}`);
                        lastError = err;
                        if (classified.keySpecific) markKeyResult(currentKey, false);
                        if (!classified.retryable) break;
                        continue;
                    }

                    const data = await response.json();
                    const text = parseGeminiText(data).trim();
                    if (text) {
                        markKeyResult(currentKey, true);
                        return text;
                    }

                    const cand = data?.candidates?.[0];
                    const reason = cand?.finishReason || (data?.promptFeedback?.blockReason ? `prompt blocked: ${data.promptFeedback.blockReason}` : 'no candidates');
                    failures.push(`${keyLabel(keys, currentKey)} try${attempt}: پاسخ بدون متن (${reason})`);
                    if (isPermanentEmptyReason(reason)) break;
                    markKeyResult(currentKey, false);
                } catch (err) {
                    const classified = classifyGeminiError(err);
                    failures.push(`${keyLabel(keys, currentKey)} try${attempt}: ${classified.category}${classified.rawMessage ? ` (${classified.rawMessage})` : ''}`);
                    lastError = err;
                    if (classified.keySpecific) markKeyResult(currentKey, false);
                    if (!classified.retryable) break;
                } finally {
                    clearTimeout(timeoutId);
                }

                if (attempt < MAX_ATTEMPTS_PER_KEY && Date.now() < overallDeadline) {
                    await new Promise(resolve => setTimeout(resolve, BOT_RETRY_DELAY_MS * attempt));
                }
            }
        }
    }

    const classified = classifyGeminiError(lastError || new Error('Gemini deadline exceeded'));
    console.error(
        `[shared-chats] Gemini failed (models=${models.join(',')}, ${Date.now() - startedAt}ms, category=${classified.category}): ${failures.join(' | ') || 'no attempts'}`
    );
    throw new Error('پاسخ از سرویس هوش مصنوعی دریافت نشد.');
}

async function streamBotReply(historyForPrompt, model, onChunk) {
    const keys = getGeminiKeys();
    if (!keys.length) throw new Error('GEMINI_API_KEYS/GEMINI_API_KEY تنظیم نشده است.');

    const models = getModelFallbacks(model);
    const startedAt = Date.now();
    const overallDeadline = startedAt + getBotDeadlineMs(keys.length, models.length);
    const failures = [];
    let lastError = null;

    outer:
    for (const currentModel of models) {
        const orderedKeys = rotateKeysByHealth(keys);
        for (const currentKey of orderedKeys) {
            if (Date.now() >= overallDeadline) break outer;
            for (let attempt = 1; attempt <= MAX_ATTEMPTS_PER_KEY; attempt++) {
                const remaining = Math.max(0, overallDeadline - Date.now());
                if (remaining < 1000) break outer;

                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), Math.min(BOT_PER_CALL_TIMEOUT_MS, remaining));
                let emittedAny = false;
                let fullText = '';
                let buffer = '';
                let lastEmptyReason = null;

                try {
                    const response = await fetch(
                        `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(currentModel)}:streamGenerateContent?alt=sse`,
                        {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'x-goog-api-key': currentKey
                            },
                            body: JSON.stringify(makeGeminiRequestBody(historyForPrompt)),
                            signal: controller.signal
                        }
                    );

                    if (!response.ok) {
                        const body = await response.text().catch(() => '');
                        const err = makeHttpGeminiError(response.status, body);
                        const classified = classifyGeminiError(err);
                        failures.push(`${keyLabel(keys, currentKey)} try${attempt}: HTTP ${response.status} ${body.slice(0, 240).replace(/\s+/g, ' ')}`);
                        lastError = err;
                        if (classified.keySpecific) markKeyResult(currentKey, false);
                        if (!classified.retryable) break;
                    } else {
                        const processEvent = (rawEvent) => {
                            const line = rawEvent.split(/\r?\n/).find(l => l.startsWith('data:'));
                            if (!line) return;
                            const jsonStr = line.slice(5).trim();
                            if (!jsonStr || jsonStr === '[DONE]') return;
                            let parsed;
                            try { parsed = JSON.parse(jsonStr); } catch (_) { return; }
                            const cand = parsed?.candidates?.[0];
                            const pieceText = cand?.content?.parts?.map(p => p?.text || '').join('') || '';
                            if (pieceText) {
                                fullText += pieceText;
                                emittedAny = true;
                                onChunk(pieceText);
                            } else if (cand?.finishReason || parsed?.promptFeedback?.blockReason) {
                                lastEmptyReason = cand?.finishReason || `prompt blocked: ${parsed.promptFeedback.blockReason}`;
                            }
                        };

                        for await (const rawChunk of response.body) {
                            buffer += Buffer.isBuffer(rawChunk) ? rawChunk.toString('utf8') : String(rawChunk);
                            let sep;
                            while ((sep = buffer.search(/\r?\n\r?\n/)) !== -1) {
                                const rawEvent = buffer.slice(0, sep);
                                const match = buffer.slice(sep).match(/^\r?\n\r?\n/);
                                buffer = buffer.slice(sep + (match ? match[0].length : 2));
                                processEvent(rawEvent);
                            }
                        }
                        if (buffer.trim()) processEvent(buffer);

                        if (fullText.trim()) {
                            markKeyResult(currentKey, true);
                            return fullText;
                        }

                        const reason = lastEmptyReason || 'دلیل نامشخص';
                        failures.push(`${keyLabel(keys, currentKey)} try${attempt}: پاسخ استریم بدون متن (${reason})`);
                        if (isPermanentEmptyReason(reason)) break;
                        markKeyResult(currentKey, false);
                    }
                } catch (err) {
                    const classified = classifyGeminiError(err);
                    failures.push(`${keyLabel(keys, currentKey)} try${attempt}: ${classified.category}${classified.rawMessage ? ` (${classified.rawMessage})` : ''}`);
                    lastError = err;
                    if (emittedAny) {
                        console.error(`[shared-chats] stream interrupted mid-way (model=${currentModel}, ${keyLabel(keys, currentKey)}): ${err?.message || err}`);
                        throw new Error('پاسخ ربات وسط راه قطع شد.');
                    }
                    if (classified.keySpecific) markKeyResult(currentKey, false);
                    if (!classified.retryable) break;
                } finally {
                    clearTimeout(timeoutId);
                }

                if (attempt < MAX_ATTEMPTS_PER_KEY && Date.now() < overallDeadline) {
                    await new Promise(resolve => setTimeout(resolve, BOT_RETRY_DELAY_MS * attempt));
                }
            }
        }
    }

    const classified = classifyGeminiError(lastError || new Error('Gemini deadline exceeded'));
    console.error(
        `[shared-chats] Gemini stream failed (models=${models.join(',')}, ${Date.now() - startedAt}ms, category=${classified.category}): ${failures.join(' | ') || 'no attempts'}`
    );
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

            // ----- دانلود یک عکس: فقط اعضا، و فقط مسیری که واقعاً به همین چت ثبت شده -----
            if (req.query?.action === 'download') {
                const path = String(req.query?.path || '');
                if (!chatId || !path) {
                    return res.status(400).json({ error: 'chatId یا path مشخص نشده.' });
                }
                if (!(await isParticipant(chatId, email))) {
                    return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
                }
                // فقط پیشوند مسیر کافی نیست: مسیر باید دقیقاً در جدول ضمیمه‌ها برای
                // همین chat_id ثبت شده باشد، وگرنه یک عضو می‌توانست مسیر یک چت دیگر را بخواهد.
                const regResp = await supaFetch(
                    `shared_chat_attachments?chat_id=eq.${encodeURIComponent(chatId)}&storage_path=eq.${encodeURIComponent(path)}&select=content_type`
                );
                const regRows = regResp.ok ? await regResp.json() : [];
                if (!Array.isArray(regRows) || !regRows.length) {
                    return res.status(404).json({ error: 'فایل پیدا نشد.' });
                }
                const storageResp = await downloadFromStorage(path);
                if (!storageResp.ok) return res.status(404).json({ error: 'فایل پیدا نشد.' });
                const base64 = Buffer.from(await storageResp.arrayBuffer()).toString('base64');
                return res.status(200).json({ base64, contentType: regRows[0].content_type });
            }

            if (chatId) {
                if (!(await isParticipant(chatId, email))) {
                    return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
                }
                const since = Number(req.query?.since) || 0;
                const msgsResp = await supaFetch(
                    `shared_chat_messages?chat_id=eq.${encodeURIComponent(chatId)}&id=gt.${since}&select=*&order=id.asc&limit=${MAX_MESSAGES_PER_POLL}`
                );
                const messages = await msgsResp.json();
                const list = Array.isArray(messages) ? messages : [];
                const attMap = await fetchAttachmentsForMessages(list.map(m => m.id));
                for (const m of list) m.attachments = attMap[m.id] || [];
                return res.status(200).json({ messages: list });
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
                `shared_chats?chat_id=in.(${idsFilter})&select=chat_id,title,owner_email,invite_code,model,updated_at&order=updated_at.desc`
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

            // مدل فقط اینجا (توسط سازنده) تعیین می‌شود و بعداً هیچ endpointی
            // اجازه‌ی تغییرش را ندارد؛ مدل نامعتبر رد می‌شود (نه اینکه بی‌صدا
            // جایگزین شود) تا کلاینت بفهمد چه اتفاقی افتاده.
            const requestedModel = req.body?.model;
            if (requestedModel !== undefined && requestedModel !== null && requestedModel !== '' &&
                !ALLOWED_MODELS.includes(requestedModel)) {
                return res.status(400).json({ error: 'مدل انتخاب‌شده معتبر نیست.', allowedModels: ALLOWED_MODELS });
            }
            const model = requestedModel || DEFAULT_MODEL;

            const createResp = await supaFetch('shared_chats', {
                method: 'POST',
                body: JSON.stringify([{
                    chat_id: chatId,
                    owner_email: email,
                    title,
                    invite_code: inviteCode,
                    model,
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

            return res.status(200).json({ chatId, inviteCode, title, model });
        }

        // ===== POST action=join: پیوستن با کد دعوت =====
        if (action === 'join') {
            const inviteCode = String(req.body?.inviteCode || '').trim().toUpperCase();
            if (!inviteCode) {
                return res.status(400).json({ error: 'کد دعوت مشخص نشده.' });
            }

            const findResp = await supaFetch(
                `shared_chats?invite_code=eq.${encodeURIComponent(inviteCode)}&select=chat_id,title,model`
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

            return res.status(200).json({ chatId, title: findRows[0].title, model: findRows[0].model || DEFAULT_MODEL });
        }

        // ===== POST action=upload: آپلود یک عکس به Supabase Storage (فقط اعضا) =====
        // آپلود جدا از send است تا بدنه‌ی send سبک بماند و اگر آپلود شکست خورد،
        // پیام متنی کاربر گیر نکند. فایل تا وقتی همراه یک پیام send نشود
        // در جدول ضمیمه‌ها ثبت نمی‌شود (پس برای بقیه‌ی اعضا نامرئی است).
        if (action === 'upload') {
            const chatId = String(req.body?.chatId || '').trim();
            if (!chatId) {
                return res.status(400).json({ error: 'chatId مشخص نشده.' });
            }
            if (!(await isParticipant(chatId, email))) {
                return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
            }
            const { base64, contentType, name } = req.body || {};
            if (!base64 || typeof base64 !== 'string') {
                return res.status(400).json({ error: 'محتوای عکس (base64) خالی است.' });
            }
            if (!ALLOWED_IMAGE_TYPES.includes(contentType)) {
                return res.status(415).json({ error: 'فقط عکس (PNG، JPEG، WebP، GIF) مجاز است.' });
            }
            // پیشوند data:...;base64, اگر بود حذف می‌شود
            const buffer = Buffer.from(base64.split(',').pop(), 'base64');
            if (!buffer.length) {
                return res.status(400).json({ error: 'محتوای عکس خالی یا نامعتبر است.' });
            }
            if (buffer.length > MAX_UPLOAD_SIZE) {
                return res.status(413).json({ error: `حجم عکس بیشتر از ${MAX_UPLOAD_SIZE / (1024 * 1024)} مگابایت مجاز است.` });
            }

            const objectPath = sharedStoragePath(chatId, name);
            const uploadResp = await uploadToStorage(objectPath, buffer, contentType);
            if (!uploadResp.ok) {
                const errText = await uploadResp.text().catch(() => '');
                console.error('[shared-chats] uploadToStorage failed:', errText);
                return res.status(502).json({ error: 'آپلود عکس روی Supabase Storage ناموفق بود.' });
            }
            return res.status(200).json({
                ok: true,
                path: objectPath,
                contentType,
                name: String(name || 'image').slice(0, 150),
                size: buffer.length
            });
        }

        // ===== POST action=send: ارسال پیام + پاسخ ربات =====
        if (action === 'send') {
            const chatId = String(req.body?.chatId || '').trim();
            const text = String(req.body?.text || '').trim();
            const rawAttachments = Array.isArray(req.body?.attachments) ? req.body.attachments : [];
            if (!chatId || (!text && !rawAttachments.length)) {
                return res.status(400).json({ error: 'chatId یا (text/attachments) مشخص نشده.' });
            }
            if (text.length > MAX_MESSAGE_CHARS) {
                return res.status(413).json({ error: `پیام نباید بیشتر از ${MAX_MESSAGE_CHARS} کاراکتر باشد.` });
            }
            if (rawAttachments.length > MAX_ATTACHMENTS_PER_MESSAGE) {
                return res.status(413).json({ error: `حداکثر ${MAX_ATTACHMENTS_PER_MESSAGE} عکس در هر پیام مجاز است.` });
            }
            if (!(await isParticipant(chatId, email))) {
                return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
            }

            // هر path باید همان چیزی باشد که خود همین چت در upload برگردانده:
            // پیشوند shared/<chatId>/ اجباری است تا کسی نتواند فایل یک چت دیگر
            // (یا فایل چت شخصی یک کاربر) را به پیام خودش وصل کند.
            const requiredPrefix = `shared/${encodeURIComponent(chatId)}/`;
            const validAttachments = [];
            for (const att of rawAttachments) {
                const path = String(att?.path || '');
                if (!path.startsWith(requiredPrefix) || path.includes('..')) {
                    return res.status(400).json({ error: 'مسیر عکس نامعتبر است.' });
                }
                // وجود فایل واقعی در Storage را تأیید کن (و نوع/حجم را از خود
                // Storage بخوان، نه از ادعای کلاینت)
                const headResp = await downloadFromStorage(path);
                if (!headResp.ok) {
                    return res.status(400).json({ error: 'یکی از عکس‌ها پیدا نشد؛ دوباره آپلودش کن.' });
                }
                const bytes = Buffer.from(await headResp.arrayBuffer());
                const realType = (headResp.headers.get('content-type') || '').split(';')[0].trim();
                if (!ALLOWED_IMAGE_TYPES.includes(realType) || bytes.length > MAX_UPLOAD_SIZE) {
                    return res.status(400).json({ error: 'یکی از عکس‌ها نامعتبر است.' });
                }
                validAttachments.push({
                    path,
                    contentType: realType,
                    name: String(att?.name || 'image').slice(0, 150),
                    size: bytes.length
                });
            }

            // پیام کاربر همیشه فوری ذخیره می‌شود (حتی اگر بعداً قفل جواب
            // ربات را به تعویق بیندازد) - این‌طوری نفر دوم فوراً پیام
            // نفر اول را در نتیجه‌ی polling بعدی می‌بیند، بدون نیاز به
            // منتظر ماندن برای جواب ربات.
            const userMsg = await insertMessage(chatId, 'user', email, text);
            if (!userMsg) {
                // فایل‌هایی که همین الان آپلود شده بودند دیگر به هیچ پیامی وصل نمی‌شوند؛ پاکشان کن.
                await Promise.all(validAttachments.map(a => deleteFromStorage(a.path)));
                return res.status(500).json({ error: 'ذخیره‌ی پیام ناموفق بود.' });
            }
            if (validAttachments.length) {
                const attResp = await supaFetch('shared_chat_attachments', {
                    method: 'POST',
                    body: JSON.stringify(validAttachments.map(a => ({
                        message_id: userMsg.id,
                        chat_id: chatId,
                        storage_path: a.path,
                        content_type: a.contentType,
                        file_name: a.name,
                        size_bytes: a.size,
                        created_at: Date.now()
                    })))
                });
                if (!attResp.ok) {
                    console.error('[shared-chats] attachments insert failed:', await attResp.text().catch(() => ''));
                    // به‌جای اینکه پیام بدون عکس بماند (و کاربر فکر کند عکس رفته)،
                    // پیام و فایل‌ها را برمی‌داریم و خطا برمی‌گردانیم تا کلاینت دوباره امتحان کند.
                    await supaFetch(`shared_chat_messages?id=eq.${userMsg.id}`, { method: 'DELETE' });
                    await Promise.all(validAttachments.map(a => deleteFromStorage(a.path)));
                    return res.status(500).json({ error: 'ذخیره‌ی عکس‌ها ناموفق بود؛ دوباره امتحان کن.' });
                }
                userMsg.attachments = validAttachments.map(a => ({
                    path: a.path, contentType: a.contentType, name: a.name, size: a.size
                }));
            } else {
                userMsg.attachments = [];
            }
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
                    `shared_chat_messages?chat_id=eq.${encodeURIComponent(chatId)}&select=id,role,sender_email,text&order=id.desc&limit=100`
                );
                const historyRowsDesc = await historyResp.json();
                // از جدید به قدیم گرفتیم (تا limit روی «آخرین ۱۰۰ پیام» اعمال شود)؛ برای prompt برعکس می‌کنیم.
                const historyRows = Array.isArray(historyRowsDesc) ? historyRowsDesc.reverse() : [];

                // عکس‌های این پیام‌ها را یک‌جا بگیر و فقط MAX_IMAGES_TO_BOT تای آخر را
                // واقعاً برای ربات بفرست (بقیه در متن با یک نشانه‌ی «[عکس]» می‌آیند).
                const attMap = await fetchAttachmentsForMessages(historyRows.map(r => r.id));
                const allImages = [];
                for (const row of historyRows) {
                    for (const att of (attMap[row.id] || [])) allImages.push({ msgId: row.id, att });
                }
                const sendableImages = new Set(allImages.slice(-MAX_IMAGES_TO_BOT).map(x => x.att.id));

                const historyForPrompt = [];
                for (const row of historyRows) {
                    const parts = [];
                    const label = row.role === 'user' && row.sender_email ? `[${row.sender_email}]: ` : '';
                    const rowAtts = attMap[row.id] || [];
                    const bodyText = (row.text || '') + (rowAtts.length && !row.text ? '(عکس فرستاده شد)' : '');
                    parts.push({ text: `${label}${bodyText}` });

                    for (const att of rowAtts) {
                        if (!sendableImages.has(att.id)) {
                            parts.push({ text: '[عکس قدیمی‌تر - برای صرفه‌جویی در حجم، دوباره فرستاده نشد]' });
                            continue;
                        }
                        const imgResp = await downloadFromStorage(att.path);
                        if (!imgResp.ok) continue;
                        const b64 = Buffer.from(await imgResp.arrayBuffer()).toString('base64');
                        parts.push({ inlineData: { mimeType: att.contentType, data: b64 } });
                    }
                    historyForPrompt.push({ role: row.role === 'model' ? 'model' : 'user', parts });
                }

                // مدل ثابت این چت (همان که سازنده موقع create انتخاب کرده)
                const chatModel = await getChatModel(chatId);
                const botText = await getBotReply(historyForPrompt, chatModel);
                const botMsg = await insertMessage(chatId, 'model', null, botText);
                await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}`, {
                    method: 'PATCH',
                    body: JSON.stringify({ updated_at: Date.now() })
                });

                if (botMsg) botMsg.attachments = [];
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

        // ===== POST action=stream: FEATURE (فیچر A) - مثل send، ولی پاسخ
        // ربات را با SSE تکه‌تکه پخش می‌کند به‌جای اینکه کاربر تا آخر جواب
        // کامل هیچ‌چی نبیند. اعتبارسنجی/ذخیره‌ی پیام کاربر و مدیریت قفل
        // دقیقاً عین action=send است (کپی عمدی، نه فراخوانی مشترک، تا
        // مسیر send برای کلاینت‌های قدیمی‌تر بدون هیچ ریسکی دست‌نخورده بماند).
        if (action === 'stream') {
            const chatId = String(req.body?.chatId || '').trim();
            const text = String(req.body?.text || '').trim();
            const rawAttachments = Array.isArray(req.body?.attachments) ? req.body.attachments : [];
            if (!chatId || (!text && !rawAttachments.length)) {
                return res.status(400).json({ error: 'chatId یا (text/attachments) مشخص نشده.' });
            }
            if (text.length > MAX_MESSAGE_CHARS) {
                return res.status(413).json({ error: `پیام نباید بیشتر از ${MAX_MESSAGE_CHARS} کاراکتر باشد.` });
            }
            if (rawAttachments.length > MAX_ATTACHMENTS_PER_MESSAGE) {
                return res.status(413).json({ error: `حداکثر ${MAX_ATTACHMENTS_PER_MESSAGE} عکس در هر پیام مجاز است.` });
            }
            if (!(await isParticipant(chatId, email))) {
                return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
            }

            const requiredPrefix = `shared/${encodeURIComponent(chatId)}/`;
            const validAttachments = [];
            for (const att of rawAttachments) {
                const path = String(att?.path || '');
                if (!path.startsWith(requiredPrefix) || path.includes('..')) {
                    return res.status(400).json({ error: 'مسیر عکس نامعتبر است.' });
                }
                const headResp = await downloadFromStorage(path);
                if (!headResp.ok) {
                    return res.status(400).json({ error: 'یکی از عکس‌ها پیدا نشد؛ دوباره آپلودش کن.' });
                }
                const bytes = Buffer.from(await headResp.arrayBuffer());
                const realType = (headResp.headers.get('content-type') || '').split(';')[0].trim();
                if (!ALLOWED_IMAGE_TYPES.includes(realType) || bytes.length > MAX_UPLOAD_SIZE) {
                    return res.status(400).json({ error: 'یکی از عکس‌ها نامعتبر است.' });
                }
                validAttachments.push({
                    path,
                    contentType: realType,
                    name: String(att?.name || 'image').slice(0, 150),
                    size: bytes.length
                });
            }

            const userMsg = await insertMessage(chatId, 'user', email, text);
            if (!userMsg) {
                await Promise.all(validAttachments.map(a => deleteFromStorage(a.path)));
                return res.status(500).json({ error: 'ذخیره‌ی پیام ناموفق بود.' });
            }
            if (validAttachments.length) {
                const attResp = await supaFetch('shared_chat_attachments', {
                    method: 'POST',
                    body: JSON.stringify(validAttachments.map(a => ({
                        message_id: userMsg.id,
                        chat_id: chatId,
                        storage_path: a.path,
                        content_type: a.contentType,
                        file_name: a.name,
                        size_bytes: a.size,
                        created_at: Date.now()
                    })))
                });
                if (!attResp.ok) {
                    console.error('[shared-chats] attachments insert failed:', await attResp.text().catch(() => ''));
                    await supaFetch(`shared_chat_messages?id=eq.${userMsg.id}`, { method: 'DELETE' });
                    await Promise.all(validAttachments.map(a => deleteFromStorage(a.path)));
                    return res.status(500).json({ error: 'ذخیره‌ی عکس‌ها ناموفق بود؛ دوباره امتحان کن.' });
                }
                userMsg.attachments = validAttachments.map(a => ({
                    path: a.path, contentType: a.contentType, name: a.name, size: a.size
                }));
            } else {
                userMsg.attachments = [];
            }
            await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}`, {
                method: 'PATCH',
                body: JSON.stringify({ updated_at: Date.now() })
            });

            // FIX: از همین‌جا به بعد پاسخ HTTP دیگر یک JSON عادی نیست -
            // هدرهای SSE را دستی می‌نویسیم (دقیقاً هم‌الگو با
            // pages/api/chat.js) چون بعد از این هر خطای await باید به‌جای
            // res.status(...).json(...) با یک event {error:...} به کلاینت
            // برسد؛ کلاینت دیگر منتظر status code جدید نمی‌ماند.
            res.writeHead(200, {
                'Content-Type': 'text/event-stream; charset=utf-8',
                'Cache-Control': 'no-cache, no-transform',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no'
            });
            if (typeof res.flushHeaders === 'function') res.flushHeaders();
            const sendEvent = (obj) => {
                res.write(`data: ${JSON.stringify(obj)}\n\n`);
                if (typeof res.flush === 'function') res.flush();
            };
            // اولین event: پیام کاربر (با id واقعی سرور) - کلاینت این را
            // فوری جایگزین نسخه‌ی optimistic خودش می‌کند، دقیقاً مثل چیزی
            // که قبلاً از فیلد "message" در پاسخ غیر-استریمی می‌خواند.
            sendEvent({ userMessage: userMsg });

            const gotLock = await acquireLock(chatId, email);
            if (!gotLock) {
                sendEvent({ done: true, botPending: true });
                return res.end();
            }

            try {
                const historyResp = await supaFetch(
                    `shared_chat_messages?chat_id=eq.${encodeURIComponent(chatId)}&select=id,role,sender_email,text&order=id.desc&limit=100`
                );
                const historyRowsDesc = await historyResp.json();
                const historyRows = Array.isArray(historyRowsDesc) ? historyRowsDesc.reverse() : [];

                const attMap = await fetchAttachmentsForMessages(historyRows.map(r => r.id));
                const allImages = [];
                for (const row of historyRows) {
                    for (const att of (attMap[row.id] || [])) allImages.push({ msgId: row.id, att });
                }
                const sendableImages = new Set(allImages.slice(-MAX_IMAGES_TO_BOT).map(x => x.att.id));

                const historyForPrompt = [];
                for (const row of historyRows) {
                    const parts = [];
                    const label = row.role === 'user' && row.sender_email ? `[${row.sender_email}]: ` : '';
                    const rowAtts = attMap[row.id] || [];
                    const bodyText = (row.text || '') + (rowAtts.length && !row.text ? '(عکس فرستاده شد)' : '');
                    parts.push({ text: `${label}${bodyText}` });

                    for (const att of rowAtts) {
                        if (!sendableImages.has(att.id)) {
                            parts.push({ text: '[عکس قدیمی‌تر - برای صرفه‌جویی در حجم، دوباره فرستاده نشد]' });
                            continue;
                        }
                        const imgResp = await downloadFromStorage(att.path);
                        if (!imgResp.ok) continue;
                        const b64 = Buffer.from(await imgResp.arrayBuffer()).toString('base64');
                        parts.push({ inlineData: { mimeType: att.contentType, data: b64 } });
                    }
                    historyForPrompt.push({ role: row.role === 'model' ? 'model' : 'user', parts });
                }

                const chatModel = await getChatModel(chatId);
                const botText = await streamBotReply(historyForPrompt, chatModel, (piece) => {
                    sendEvent({ text: piece });
                });
                const botMsg = await insertMessage(chatId, 'model', null, botText);
                await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}`, {
                    method: 'PATCH',
                    body: JSON.stringify({ updated_at: Date.now() })
                });
                if (botMsg) botMsg.attachments = [];
                sendEvent({ done: true, botMessage: botMsg });
                return res.end();
            } catch (err) {
                console.error('[shared-chats] stream bot reply failed:', err?.message || err);
                sendEvent({ error: 'پاسخ ربات دریافت نشد.' });
                return res.end();
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
