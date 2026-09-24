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

// ===== retry پاسخ ربات =====
// کل بودجه باید از تایم‌اوت خواندن کلاینت (۳۰ثانیه در SharedChatApiClient) و
// maxDuration تابع ورسل کمتر بماند، وگرنه کلاینت قطع می‌کند و پیام ربات بعداً
// (بی‌صدا) می‌رسد.
const MAX_ATTEMPTS_PER_KEY = 2;
const BOT_PER_CALL_TIMEOUT_MS = 20 * 1000;
const BOT_TOTAL_BUDGET_MS = 26 * 1000;
const BOT_RETRY_DELAY_MS = 800;

// ===== مدل ثابت هر چت مشترک =====
// فقط مدل‌های این لیست پذیرفته می‌شوند تا کاربر نتواند یک رشته‌ی دلخواه
// را داخل URL درخواست Gemini بنشاند. اگر مدل‌های موردنظرت فرق دارند،
// فقط همین لیست را عوض کن (اولین مورد پیش‌فرض است).
// همان سه مدلی که چیپ انتخاب مدل در اپ (MainActivity modelOptions) نشان
// می‌دهد. gemini-3.6-flash هم نگه داشته شده چون چت‌های مشترکی که قبل از
// این فیچر ساخته شده‌اند در دیتابیس همین مقدار را دارند (default ستون).
const ALLOWED_MODELS = [
    'gemini-3.8-flash',        // Virtual Bot 1.7 - پیش‌فرض اپ
    'gemini-3.5-flash-lite',   // Virtual Bot 1.1
    'gemini-3.1-pro-preview',  // Virtual Bot 1.3
    'gemini-3.6-flash'         // قدیمی (سازگاری با چت‌های قبلی)
];
const DEFAULT_MODEL = 'gemini-3.8-flash';

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


// ===== Storage helpers (Supabase Storage REST) =====
// مسیر: shared/<chatId>/<timestamp>_<نام امن>. برخلاف چت شخصی، ایمیل
// آپلودکننده در مسیر نیست چون دسترسی بر اساس عضویت در چت است، نه مالکیت.
function sharedStoragePath(chatId, fileName) {
    const safeName = String(fileName || 'image').replace(/[^\w.\-]+/g, '_').slice(0, 100);
    const rand = crypto.randomBytes(4).toString('hex');
    return `shared/${encodeURIComponent(chatId)}/${Date.now()}_${rand}_${safeName}`;
}

async function uploadToStorage(objectPath, buffer, contentType) {
    return fetch(`${SUPABASE_URL}/storage/v1/object/${STORAGE_BUCKET}/${objectPath}`, {
        method: 'POST',
        headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': contentType || 'application/octet-stream',
            'x-upsert': 'false'
        },
        body: buffer
    });
}

async function downloadFromStorage(objectPath) {
    return fetch(`${SUPABASE_URL}/storage/v1/object/${STORAGE_BUCKET}/${objectPath}`, {
        headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`
        }
    });
}

async function deleteFromStorage(objectPath) {
    // best-effort: اگر پاک نشد، مشکلی برای کاربر پیش نمی‌آید (فقط یک فایل یتیم می‌ماند)
    try {
        await fetch(`${SUPABASE_URL}/storage/v1/object/${STORAGE_BUCKET}/${objectPath}`, {
            method: 'DELETE',
            headers: {
                'apikey': SUPABASE_SERVICE_ROLE_KEY,
                'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`
            }
        });
    } catch (_) { /* ignore */ }
}

// مدل ثابت این چت را از دیتابیس می‌خواند. اگر ستون/ردیف مشکل داشت یا مدل
// دیگر در لیست مجاز نبود، به پیش‌فرض برمی‌گردیم تا چت از کار نیفتد.
async function getChatModel(chatId) {
    try {
        const resp = await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}&select=model`);
        if (!resp.ok) return DEFAULT_MODEL;
        const rows = await resp.json();
        const model = Array.isArray(rows) && rows[0] && rows[0].model;
        return ALLOWED_MODELS.includes(model) ? model : DEFAULT_MODEL;
    } catch (_) {
        return DEFAULT_MODEL;
    }
}

// عکس‌های چند پیام را یک‌جا می‌گیرد و به‌صورت map از message_id -> [attachment] برمی‌گرداند
// (یک query برای کل batch، نه یک query به ازای هر پیام).
async function fetchAttachmentsForMessages(messageIds) {
    const map = {};
    if (!messageIds.length) return map;
    const resp = await supaFetch(
        `shared_chat_attachments?message_id=in.(${messageIds.join(',')})&select=id,message_id,storage_path,content_type,file_name,size_bytes&order=id.asc`
    );
    if (!resp.ok) return map;
    const rows = await resp.json();
    if (!Array.isArray(rows)) return map;
    for (const row of rows) {
        (map[row.message_id] = map[row.message_id] || []).push({
            id: row.id,
            path: row.storage_path,
            contentType: row.content_type,
            name: row.file_name,
            size: row.size_bytes
        });
    }
    return map;
}

// FEATURE (فیچر B - پرامپت بهتر): قبلاً این‌جا فقط ۴ خط بود (نه لحن، نه
// قوانین قالب‌بندی/لیست، نه چیزی درباره‌ی شماره‌گذاری تودرتو) - برای همین
// چت مشترک هم لحن خشک‌تری داشت و هم مشکل «بخش ۱، ۱، ۱» (تکرار شماره در
// لیست‌های تودرتو) که مدل‌های کوچیک بدون راهنمایی صریح بهش دچار می‌شن.
// این‌جا معادل خلاصه‌شده‌ی بخش «لحن» + «قالب‌بندی» از systemText اصلی
// chat.js است (نگاه کن به pages/api/chat.js حدود خط ۵۳۶۴-۵۴۴۴)، نه کل
// آن پرامپت ۲۹هزار کاراکتری (که شامل حافظه/ترجیحات/ویجت/SVG است و چت
// مشترک فعلاً به هیچ‌کدام نیاز ندارد).
const SHARED_CHAT_SYSTEM_TEXT =
    'تو Virtual Bot هستی؛ دستیار هوش مصنوعی گرم، صمیمی و طبیعی به فارسی، مثل صحبت با یک دوست باهوش، نه متن خشک و رسمی.\n' +
    'در این گفتگو ممکن است بیش از یک نفر با تو صحبت کند - هر پیام کاربر با نام فرستنده مشخص شده؛ ' +
    'به هر نفر با توجه به کل زمینه‌ی گفتگو پاسخ بده، نه فقط آخرین پیام را جدا از بقیه در نظر بگیر. ' +
    'اگر دو نفر همزمان موضوع‌های متفاوتی مطرح کرده‌اند، مشخص کن به کدام پیام/کدام فرد پاسخ می‌دهی.\n' +
    'لحن: رسمی→محترمانه، دوستانه→صمیمی، شوخ→هم‌راستا. محاوره‌ای و روان باش؛ فقط عبارت‌های رایج و طبیعی فارسی. ' +
    'سؤال ساده کوتاه جواب بده؛ موضوع پیچیده کامل و مرحله‌ای. جمله‌ی اول را طوری نساز که با پاسخ واقعی بعدی تناقض داشته باشد.\n' +
    'ایموجی را مستقل از رفتار کاربر و طبیعی استفاده کن (لازم نیست کاربر اول ایموجی بزند)؛ در پاسخ‌های رسمی/فنی/جدی ایموجی کم یا اصلاً استفاده نکن؛ ' +
    'هرگز 🤖 استفاده نکن و از ردیف طولانی ایموجی پرهیز کن.\n' +
    'قالب‌بندی (فقط وقتی واقعاً لازم است): ایتالیک با *متن* یا _متن_؛ خط‌خورده با ~~متن~~؛ لینک واقعی با [متن](https://...)؛ ' +
    'جدول مارک‌داون فقط برای داده‌ی واقعاً جدولی.\n' +
    'قانون شماره‌گذاری لیست تودرتو (مهم): هر سطح فقط یک‌بار شماره/بولت بگیرد - هرگز ننویس «۱. بخش ۱» یا زیر آیتم شماره‌ی «۱» دوباره زیرشماره‌ی «۱.۱» را با پیشوند تکراری تکرار نکن؛ ' +
    'برای زیرسطح از حروف (الف، ب) یا خط تیره‌ی ساده استفاده کن، نه تکرار همان عدد پدر. لیست تودرتو با ۲ فاصله برای هر سطح تورفتگی داشته باشد.\n' +
    'ریاضی: درون‌خطی با $...$ و مستقل/بزرگ با $$...$$؛ علامت $ را escape نکن.\n' +
    'درباره‌ی چیزهایی که نمی‌دانی اطلاعات ساختگی نده و بگو مطمئن نیستی.';

// ===== پاسخ ربات: یک generateContent ساده (بدون استریم/ابزار) با
// چرخش بین چند کلید API، دقیقاً هم‌الگو با تابع تولید عنوان در
// chat.js. چت مشترک برای شروع نیازی به search/file-edit ندارد. =====
async function getBotReply(historyForPrompt, model) {
    const geminiKeys = (process.env.GEMINI_API_KEYS || process.env.GEMINI_API_KEY || '')
        .split(',').map(k => k.trim()).filter(Boolean);
    if (!geminiKeys.length) {
        throw new Error('GEMINI_API_KEYS/GEMINI_API_KEY تنظیم نشده است.');
    }

    const systemText = SHARED_CHAT_SYSTEM_TEXT;

    // هر کلید تا MAX_ATTEMPTS_PER_KEY بار امتحان می‌شود، ولی فقط برای خطاهای
    // «گذرا» (429 / 5xx / تایم‌اوت / خطای شبکه). خطاهای دائمی (400 = درخواست
    // خراب، 401/403 = کلید بد، 404 = مدل ناموجود) retry نمی‌شوند - تکرارشان
    // فقط وقت تلف می‌کند. بودجه‌ی کل زمان هم محدود است تا از maxDuration
    // ورسل (و تایم‌اوت ۳۰ثانیه‌ی کلاینت اندروید) رد نشویم.
    const startedAt = Date.now();
    const failures = []; // برای لاگ نهایی: چرا هر تلاش شکست خورد
    const modelName = model || DEFAULT_MODEL;

    for (let keyIdx = 0; keyIdx < geminiKeys.length; keyIdx++) {
        const key = geminiKeys[keyIdx];
        for (let attempt = 1; attempt <= MAX_ATTEMPTS_PER_KEY; attempt++) {
            const remaining = BOT_TOTAL_BUDGET_MS - (Date.now() - startedAt);
            if (remaining < 3000) {
                failures.push(`key#${keyIdx + 1}: بودجه‌ی زمانی تمام شد`);
                break;
            }
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), Math.min(BOT_PER_CALL_TIMEOUT_MS, remaining));
            let retryable = false;
            try {
                const response = await fetch(
                    `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(modelName)}:generateContent`,
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

                if (!response.ok) {
                    const errBody = await response.text().catch(() => '');
                    // فقط ۳۰۰ کاراکتر اول، بدون کلید (خود پاسخ گوگل کلید را نشان نمی‌دهد)
                    failures.push(`key#${keyIdx + 1} try${attempt}: HTTP ${response.status} ${errBody.slice(0, 300).replace(/\s+/g, ' ')}`);
                    retryable = response.status === 429 || response.status >= 500;
                } else {
                    const data = await response.json();
                    const cand = data?.candidates?.[0];
                    const text = cand?.content?.parts?.map(p => p?.text || '').join('').trim();
                    if (text) return text;

                    // جواب «موفق» ولی بدون متن: دلیلش را لاگ کن (SAFETY، MAX_TOKENS،
                    // RECITATION، یا promptFeedback.blockReason). این‌ها retry نمی‌شوند
                    // چون با همان ورودی همان نتیجه را می‌دهند.
                    const why = cand?.finishReason
                        || (data?.promptFeedback?.blockReason ? `prompt blocked: ${data.promptFeedback.blockReason}` : 'no candidates');
                    failures.push(`key#${keyIdx + 1} try${attempt}: پاسخ بدون متن (${why})`);
                    retryable = false;
                }
            } catch (err) {
                const aborted = err?.name === 'AbortError';
                failures.push(`key#${keyIdx + 1} try${attempt}: ${aborted ? 'timeout' : (err?.message || err)}`);
                retryable = true; // تایم‌اوت/شبکه گذراست
            } finally {
                clearTimeout(timeoutId);
            }

            if (!retryable) break; // خطای دائمی این کلید: برو سراغ کلید بعدی
            if (attempt < MAX_ATTEMPTS_PER_KEY) {
                await new Promise(r => setTimeout(r, BOT_RETRY_DELAY_MS * attempt)); // backoff ساده
            }
        }
    }

    // یک خط لاگ کامل: مدل + دلیل هر تلاش. این همان چیزی است که قبلاً نبود و
    // باعث می‌شد فقط «پاسخ دریافت نشد» ببینیم بدون اینکه بفهمیم چرا.
    console.error(`[shared-chats] Gemini failed (model=${modelName}, ${Date.now() - startedAt}ms): ${failures.join(' | ') || 'no attempts'}`);
    throw new Error('پاسخ از سرویس هوش مصنوعی دریافت نشد.');
}

// FEATURE (فیچر A - استریم): معادل استریمی getBotReply. با
// streamGenerateContent?alt=sse (همون endpoint چت عادی chat.js) تماس
// می‌گیرد و هر تکه‌ی متن رسیده را فوری با onChunk به بیرون می‌فرستد -
// این‌طوری کاربر اولین کلمه را طی ~۱ ثانیه می‌بیند، نه بعد از ۵-۱۰ ثانیه
// انتظار برای کل جواب (که تنها دلیل کندی محسوس چت مشترک نسبت به چت
// عادی همین بود، نه Supabase/polling).
// همان منطق retry/کلید چندگانه‌ی getBotReply این‌جا هم رعایت شده تا
// افت پایداری نداشته باشیم.
async function streamBotReply(historyForPrompt, model, onChunk) {
    const geminiKeys = (process.env.GEMINI_API_KEYS || process.env.GEMINI_API_KEY || '')
        .split(',').map(k => k.trim()).filter(Boolean);
    if (!geminiKeys.length) {
        throw new Error('GEMINI_API_KEYS/GEMINI_API_KEY تنظیم نشده است.');
    }

    const startedAt = Date.now();
    const failures = [];
    const modelName = model || DEFAULT_MODEL;

    for (let keyIdx = 0; keyIdx < geminiKeys.length; keyIdx++) {
        const key = geminiKeys[keyIdx];
        for (let attempt = 1; attempt <= MAX_ATTEMPTS_PER_KEY; attempt++) {
            const remaining = BOT_TOTAL_BUDGET_MS - (Date.now() - startedAt);
            if (remaining < 3000) {
                failures.push(`key#${keyIdx + 1}: بودجه‌ی زمانی تمام شد`);
                break;
            }
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), Math.min(BOT_PER_CALL_TIMEOUT_MS, remaining));
            let retryable = false;
            // FIX: اگر وسط استریم چند توکن واقعاً به کلاینت فرستاده شده
            // باشد ولی خودِ اتصال قطع/ارور شود، دیگر نباید یک بار دیگر
            // (روی کلید بعدی) از اول جواب بدهیم - کلاینت یک پاسخ نصفه با
            // متن تکراری می‌بیند. پس اگر تا اینجا چیزی emit شده، به‌جای
            // retryable=true، خطا را همون‌جا بالا می‌بریم.
            let emittedAny = false;
            try {
                const response = await fetch(
                    `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(modelName)}:streamGenerateContent?alt=sse`,
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'x-goog-api-key': key },
                        body: JSON.stringify({
                            systemInstruction: { parts: [{ text: SHARED_CHAT_SYSTEM_TEXT }] },
                            contents: historyForPrompt
                        }),
                        signal: controller.signal
                    }
                );

                if (!response.ok) {
                    const errBody = await response.text().catch(() => '');
                    failures.push(`key#${keyIdx + 1} try${attempt}: HTTP ${response.status} ${errBody.slice(0, 300).replace(/\s+/g, ' ')}`);
                    retryable = response.status === 429 || response.status >= 500;
                } else {
                    let fullText = '';
                    let buffer = '';
                    // FIX: بعضی وقت‌ها یک candidate بدون متن (finishReason
                    // مثل SAFETY/RECITATION/MAX_TOKENS یا promptFeedback
                    // بلاک‌شده) برمی‌گردد - قبلاً این حالت فقط پیام مبهم
                    // «پاسخ استریم بدون متن» می‌داد بدون اینکه معلوم شود
                    // چرا؛ حالا آخرین finishReason/blockReason دیده‌شده را
                    // نگه می‌داریم تا در لاگ خطا مشخص باشد.
                    let lastEmptyReason = null;
                    // Node/Vercel: response.body یک async iterable از Buffer/Uint8Array است
                    // (همون الگویی که خودِ chat.js برای پایپ‌کردن استریم Gemini استفاده می‌کند).
                    for await (const rawChunk of response.body) {
                        buffer += Buffer.isBuffer(rawChunk) ? rawChunk.toString('utf8') : String(rawChunk);
                        // SSE: خط‌های "data: {...}" جدا با یک خط خالی. تا وقتی
                        // یک بلوک کامل (پایان با \n\n) نداریم صبر می‌کنیم.
                        let sepIdx;
                        while ((sepIdx = buffer.indexOf('\n\n')) !== -1) {
                            const rawEvent = buffer.slice(0, sepIdx);
                            buffer = buffer.slice(sepIdx + 2);
                            const line = rawEvent.split('\n').find(l => l.startsWith('data:'));
                            if (!line) continue;
                            const jsonStr = line.slice(5).trim();
                            if (!jsonStr || jsonStr === '[DONE]') continue;
                            let parsed;
                            try { parsed = JSON.parse(jsonStr); } catch (_) { continue; }
                            const cand = parsed?.candidates?.[0];
                            const pieceText = cand?.content?.parts?.map(p => p?.text || '').join('') || '';
                            if (pieceText) {
                                fullText += pieceText;
                                emittedAny = true;
                                onChunk(pieceText);
                            } else {
                                lastEmptyReason = cand?.finishReason
                                    || (parsed?.promptFeedback?.blockReason ? `prompt blocked: ${parsed.promptFeedback.blockReason}` : lastEmptyReason);
                            }
                        }
                    }
                    if (fullText.trim()) return fullText;
                    // FIX: قبلاً همیشه retryable=false بود، یعنی یک 503/۴۲۹
                    // گذرا که تصادفاً یک استریم خالی برگردانده بود هم دیگر
                    // روی همان کلید امتحان نمی‌شد. فقط دلایل دائمی (SAFETY،
                    // RECITATION، بلاک شدن prompt) واقعاً retry نمی‌خواهند؛
                    // بقیه (finishReason ناشناس یا هیچ‌کدام) را گذرا فرض کن.
                    const permanentReasons = ['SAFETY', 'RECITATION', 'PROHIBITED_CONTENT'];
                    const isPermanent = lastEmptyReason && permanentReasons.some(r => lastEmptyReason.includes(r));
                    failures.push(`key#${keyIdx + 1} try${attempt}: پاسخ استریم بدون متن (${lastEmptyReason || 'دلیل نامشخص'})`);
                    retryable = !isPermanent;
                }
            } catch (err) {
                const aborted = err?.name === 'AbortError';
                failures.push(`key#${keyIdx + 1} try${attempt}: ${aborted ? 'timeout' : (err?.message || err)}`);
                if (emittedAny) {
                    clearTimeout(timeoutId);
                    // چیزی از قبل به کاربر رسیده - دیگر retry نکن، همون‌قدر که
                    // رسیده را به‌عنوان جواب نهایی قبول کن تا چیزی گم/تکرار نشود.
                    console.error(`[shared-chats] stream interrupted mid-way (model=${modelName}): ${err?.message || err}`);
                    throw new Error('پاسخ ربات وسط راه قطع شد.');
                }
                retryable = true;
            } finally {
                clearTimeout(timeoutId);
            }

            if (!retryable) break;
            if (attempt < MAX_ATTEMPTS_PER_KEY) {
                await new Promise(r => setTimeout(r, BOT_RETRY_DELAY_MS * attempt));
            }
        }
    }

    console.error(`[shared-chats] Gemini stream failed (model=${modelName}, ${Date.now() - startedAt}ms): ${failures.join(' | ') || 'no attempts'}`);
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
