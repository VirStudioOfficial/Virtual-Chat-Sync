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
const LOCK_STALE_MS = 90 * 1000; // اگر قفل قدیمی‌تر از این بود، یعنی درخواست قبلی هنگ/کرش کرده - نادیده‌اش می‌گیریم

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
    'درباره‌ی چیزهایی که نمی‌دانی اطلاعات ساختگی نده و بگو مطمئن نیستی.\n' +
    'قالب‌بندی پیشرفته (فقط وقتی واقعاً لازم است): ایتالیک با *متن* یا _متن_؛ خط‌خورده با ~~متن~~؛ لینک واقعی با [متن](https://...)؛ جدول فقط برای داده‌ی جدولی.\n' +
    'برای یک اسم یا مفهوم کوتاه و مهم، نه جمله، می‌توانی از برچسب تزئینی {{entity:نام}} استفاده کنی؛ فقط وقتی واقعاً به خوانایی کمک می‌کند و زیاده‌روی نکن.\n' +
    'برای اطلاعات به‌روز، قیمت، اخبار، رویدادها یا چیزی که ممکن است بعد از زمان آموزش مدل تغییر کرده باشد، از ابزار web_search استفاده کن. برای سؤال ثابت و عمومی سرچ نکن. قبل از ابزار هیچ مقدمه‌ای برای کاربر نساز. برای هر سؤال معمولاً یک سرچ کافی است و سرچ تکراری فقط وقتی مجاز است که نتیجه‌ی اول واقعاً ناکافی/نامرتبط باشد.\n' +
    'هنگام پاسخ به گفتگوی مشترک، نتیجه‌ی سرچ را در پاسخ نهایی با زبان طبیعی و دقیق به کار ببر و لینک/منبع واقعی را از نتیجه‌ی ابزار حفظ کن.';

// ===== هم‌راستاسازی پرامپت با pages/api/chat.js =====
// chat.js پرامپت را برای هر درخواست می‌سازد (نام مدل + هویت + تاریخ روز + قانون
// معرفی مدل)؛ چت مشترک قبلاً یک متن ثابت بود و مثلاً تاریخ امروز را نمی‌دانست،
// برای همین جواب سؤال‌های زمانی/وب‌سرچ ضعیف‌تر از چت عادی بود.
// نام‌ها همان چیزی‌اند که چیپ انتخاب مدل در اپ نشان می‌دهد (MainActivity modelOptions).
const SHARED_MODEL_DISPLAY_NAMES = {
    'gemini-3.5-flash-lite': 'Virtual Bot 1.1',
    'gemini-3.6-flash': 'Virtual Bot 1.6',
    'gemini-3.8-flash': 'Virtual Bot 1.7',
    'gemini-3.1-pro-preview': 'Virtual Bot 1.3'
};

function buildSharedSystemText(modelName) {
    const displayName = SHARED_MODEL_DISPLAY_NAMES[modelName] || 'Virtual Bot';
    const now = new Date();
    let jalaliDate = '';
    let gregorianDate = '';
    let tehranTime = '';
    try {
        jalaliDate = new Intl.DateTimeFormat('fa-IR-u-ca-persian', { timeZone: 'Asia/Tehran', weekday: 'long', year: 'numeric', month: 'long', day: 'numeric' }).format(now);
        gregorianDate = new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Tehran', year: 'numeric', month: '2-digit', day: '2-digit' }).format(now);
        tehranTime = new Intl.DateTimeFormat('fa-IR', { timeZone: 'Asia/Tehran', hour: '2-digit', minute: '2-digit' }).format(now);
    } catch (_) { /* Intl بدون داده‌ی locale: تاریخ را نادیده بگیر */ }

    const identity =
        `\nهویت: نام این مدل ${displayName} است. فقط اگر کاربر همین الان مستقیم درباره‌ی مدل پرسید بگو «من ${displayName} هستم.»؛ ` +
        'هرگز خودت را با نسخه‌ای دیگر یا Gemini معرفی نکن و نام سازنده/تیم نساز. ' +
        'بدون پرسش مستقیم کاربر، معرفی مدل را در هیچ پاسخی نیاور.\n';
    const dateContext = jalaliDate
        ? `\nاطلاعات زمان واقعی؛ همیشه همین را ملاک بگیر:\nامروز: ${jalaliDate} (میلادی: ${gregorianDate})\n` +
          `ساعت فعلی به وقت تهران: ${tehranTime}\n` +
          'مهم: وقت تهران فقط برای تاریخ/روز هفته است، نه لزوماً ساعت واقعی کاربران.\n'
        : '';
    return SHARED_CHAT_SYSTEM_TEXT + identity + dateContext;
}

// ===== Gemini engine (Shared Chat) =====
// این بخش موتور مشترک Gemini + وب‌سرچ را نگه می‌دارد؛ قابلیت‌های مربوط به
// فایل/ویدیو/Live عمداً وارد Shared Chat نمی‌شوند.
//
// هم‌راستاسازی با chat.js:
// 1) health-aware key rotation و deadline مستقل برای هر کلید.
// 2) streamGenerateContent با SSE واقعی و ارسال chunk به محض رسیدن.
// 3) web_search با function calling واقعی Gemini + Tavily.
// 4) بعد از یک web_search برای همان سؤال، سرچ دوباره انجام نمی‌شود.
// 5) قطع اتصال/Stop کلاینت با AbortSignal به Gemini و Tavily منتقل می‌شود.
// 6) اگر استریم وسط راه قطع شود، متن رسیده دوباره از اول تکرار نمی‌شود.

const GEMINI_KEY_FAILURE_COUNTS = new Map();
const TAVILY_KEY_FAILURE_COUNTS = new Map();

// Compatibility constants above remain unchanged; the effective cap below is
// intentionally short so a single bad key cannot consume the full request budget.
const SHARED_GEMINI_EFFECTIVE_ATTEMPT_TIMEOUT_MS = Math.min(
    BOT_PER_CALL_TIMEOUT_MS,
    4500
);
const SHARED_GEMINI_MIN_REMAINING_MS = 1200;
const SHARED_TAVILY_TIMEOUT_MS = 5000;

// ===== استریم: رفع قطع‌شدن وسط پاسخ =====
// قبلاً یک تایمر ۴.۵ثانیه‌ای روی «کل عمر» استریم بود (نه فقط انتظار اولین
// بایت)، پس هر پاسخی که بیشتر از چند ثانیه طول می‌کشید وسط جمله قطع و به‌عنوان
// پاسخ کامل ذخیره می‌شد. حالا (مثل pages/api/chat.js): تایمر فقط تا اولین
// chunk، بعد از آن نگهبان بی‌فعالیتی که با هر chunk ریست می‌شود، و اگر Gemini
// اتصال را بدون finishReason ببندد، از همان نقطه ادامه‌ی پاسخ خواسته می‌شود.
const SHARED_STREAM_FIRST_BYTE_TIMEOUT_MS = 12 * 1000;
const SHARED_STREAM_IDLE_MS = 30 * 1000;
const SHARED_MAX_STREAM_RECOVERIES = 2;
const SHARED_CONTINUE_PROMPT =
    '[ادامهٔ پاسخ پس از قطع ناقص استریم — داخلی] پاسخ قبلی در میانهٔ تولید متوقف شد؛ متنی که تا الان فرستاده شده ممکن است دقیقاً وسط یک کلمه (حتی وسط یک پسوند/ضمیر چسبیده مثل «ت»، «م»، «ش»، «ای»، «ها») بریده شده باشد. ' +
    'اگر آخرین کاراکترِ متن قبلی حرف است نه فاصله یا علامت نگارشی، بدون هیچ فاصله‌ی اضافه دقیقاً بچسب به همان آخرین حرف و کلمه را کامل کن؛ فقط اگر متن قبلی درست سر یک فاصله یا پایان جمله تمام شده، نوبت تازه را با فاصله/کلمه‌ی بعدی شروع کن. ' +
    'هیچ بخشی از متن قبلی را تکرار نکن و مقدمه، عذرخواهی یا اشاره به قطع شدن ننویس.';
const SHARED_SEARCH_PREAMBLE_HOLD_MS = 1500;

const SHARED_GEMINI_TOOLS = [
    {
        function_declarations: [
            {
                name: 'web_search',
                description:
                    'جستجوی واقعی و زنده در وب برای اطلاعات به‌روز، قیمت، اخبار، رویدادها یا هر چیزی که ممکن است بعد از زمان آموزش مدل تغییر کرده باشد یا مدل به آن مطمئن نیست. برای سؤال‌های ثابت و عمومی از این ابزار استفاده نکن. معمولاً یک بار سرچ کافی است؛ تکرار فقط وقتی مجاز است که نتیجه‌ی اول واقعاً ناقص یا نامرتبط باشد.',
                parameters: {
                    type: 'object',
                    properties: {
                        query: {
                            type: 'string',
                            description: 'عبارت جستجوی کوتاه و دقیق.'
                        },
                        reason: {
                            type: 'string',
                            description: 'یک دلیل کوتاه فارسی برای سرچ که در صورت نیاز به رابط کاربر نشان داده می‌شود.'
                        }
                    },
                    required: ['query', 'reason']
                }
            }
        ]
    }
];

function rotateGeminiKeysByHealth(keys) {
    const shuffled = keys
        .map(key => ({ key, order: Math.random() }))
        .sort((a, b) => a.order - b.order)
        .map(({ key }) => key);

    return shuffled.sort((a, b) => {
        const fa = GEMINI_KEY_FAILURE_COUNTS.get(a) || 0;
        const fb = GEMINI_KEY_FAILURE_COUNTS.get(b) || 0;
        return fa - fb;
    });
}

function rotateTavilyKeysByHealth(keys) {
    const shuffled = keys
        .map(key => ({ key, order: Math.random() }))
        .sort((a, b) => a.order - b.order)
        .map(({ key }) => key);

    return shuffled.sort((a, b) => {
        const fa = TAVILY_KEY_FAILURE_COUNTS.get(a) || 0;
        const fb = TAVILY_KEY_FAILURE_COUNTS.get(b) || 0;
        return fa - fb;
    });
}

function markGeminiKeyResult(key, ok) {
    GEMINI_KEY_FAILURE_COUNTS.set(key, ok ? 0 : (GEMINI_KEY_FAILURE_COUNTS.get(key) || 0) + 1);
}

function markTavilyKeyResult(key, ok) {
    TAVILY_KEY_FAILURE_COUNTS.set(key, ok ? 0 : (TAVILY_KEY_FAILURE_COUNTS.get(key) || 0) + 1);
}

function geminiKeyLabel(keys, key) {
    const index = keys.indexOf(key);
    return `key#${index + 1}/${keys.length}`;
}

function classifyGeminiError(error) {
    const status = Number(
        error?.status ??
        error?.error?.code ??
        error?.body?.status ??
        error?.body?.error?.code ??
        0
    ) || null;

    const providerCode = String(
        error?.error?.status ||
        error?.body?.error?.status ||
        error?.statusText ||
        ''
    ).trim() || null;

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
        return { category: 'rate_limit_or_quota', retryable: true, keySpecific: true, status: status || 429, providerCode, rawMessage };
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

async function readGeminiErrorBody(response) {
    const raw = await response.text().catch(() => '');
    let parsed = null;
    if (raw) {
        try { parsed = JSON.parse(raw); } catch (_) {}
    }
    return { raw, parsed };
}

function getGeminiKeys() {
    return rotateGeminiKeysByHealth(
        (process.env.GEMINI_API_KEYS || process.env.GEMINI_API_KEY || '')
            .split(',')
            .map(k => k.trim())
            .filter(Boolean)
    );
}

function getTavilyKeys() {
    return rotateTavilyKeysByHealth(
        (process.env.TAVILY_API_KEYS || process.env.TAVILY_API_KEY || '')
            .split(',')
            .map(k => k.trim())
            .filter(Boolean)
    );
}

// نسخه‌ی دقیق‌تر از chat.js: «وب/ارز/now» فقط به‌صورت کلمه‌ی کامل (نه داخل «خوبی/ارزش/know») تا
// پیام‌های معمولی الکی وارد حالت نگه‌داشتن متن (preamble hold) نشوند.
function looksLikeWebSearchIntent(text) {
    const s = String(text || '').toLowerCase();
    if (!s.trim()) return false;
    return /(?:سرچ|جستجو|گوگل|(?<![آ-ی])وب(?![آ-ی])|اینترنت|قیمت(?:\s|‌)*(?:الان|امروز|فعلی|جدید|لحظه)|الان چنده|قیمتش|هزینه|آخرین|امروز|امشب|اخبار|خبرهای|آب[\u200c ]?وهوا|هوا(?:ی|\s)|نرخ|(?<![آ-ی])ارز(?![آ-ی])|دلار|یورو|طلا|سهام|موجودی|current|latest|today|right now|\\bnow\\b|search|google|look up|news|weather|price|stock|exchange rate|availability)/i.test(s);
}

function extractGeminiParts(data) {
    const parts = data?.candidates?.[0]?.content?.parts;
    return Array.isArray(parts) ? parts : [];
}

function extractGeminiText(data) {
    return extractGeminiParts(data)
        .map(p => p?.text || '')
        .filter(Boolean)
        .join('');
}

function extractFunctionCall(data) {
    const part = extractGeminiParts(data).find(p => p?.functionCall?.name);
    if (!part?.functionCall) return null;
    return {
        name: part.functionCall.name,
        args: part.functionCall.args || {},
        thoughtSignature: part.thoughtSignature || null
    };
}

function isPermanentNoTextReason(reason) {
    const value = String(reason || '').toUpperCase();
    return ['SAFETY', 'RECITATION', 'PROHIBITED_CONTENT'].some(r => value.includes(r));
}

// Gemini گاهی با نوبت‌های پشت‌سرهم از یک role (مثلاً دو نفر پشت هم پیام بدهند)
// یا شروع با role=model خطای 400 می‌دهد؛ این تابع قبل از ارسال، contents را
// تمیز می‌کند: نوبت‌های هم‌role مجاور ادغام و نوبت‌های model ابتدایی حذف می‌شوند.
function normalizeGeminiContents(contents) {
    const out = [];
    for (let item of Array.isArray(contents) ? contents : []) {
        if (!item || !Array.isArray(item.parts)) continue;
        // part متنیِ خالی ({text:''}) را Gemini با 400 رد می‌کند
        const cleanParts = item.parts.filter(p => p && !(Object.keys(p).length === 1 && typeof p.text === 'string' && !p.text.trim()));
        if (!cleanParts.length) continue;
        item = { ...item, parts: cleanParts };
        const role = item.role === 'model' ? 'model' : 'user';
        const last = out[out.length - 1];
        const hasFn = item.parts.some(p => p && (p.functionCall || p.functionResponse));
        const lastHasFn = last && last.parts.some(p => p && (p.functionCall || p.functionResponse));
        if (last && last.role === role && !hasFn && !lastHasFn) {
            last.parts = [...last.parts, ...item.parts];
        } else {
            out.push({ ...item, role, parts: [...item.parts] });
        }
    }
    while (out.length && out[0].role === 'model') out.shift();
    return out;
}

// مثل pages/api/chat.js: thinking روی low (به‌جز flash-lite که thinkingConfig
// را نمی‌پذیرد) + safetySettings. بدون این‌ها Gemini 3 پیش‌فرض thinking سنگین
// دارد و با سقف ۴.۵ ثانیه‌ی هر تلاش، تایم‌اوت می‌شود.
function buildGeminiRequestBody(contents, options = {}) {
    const includeTools = options.includeTools !== false && !options.searchUsed;
    const modelName = options.modelName || '';
    return {
        system_instruction: { parts: [{ text: buildSharedSystemText(modelName) }] },
        contents: normalizeGeminiContents(contents),
        safetySettings: [
            { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'BLOCK_ONLY_HIGH' },
            { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'BLOCK_ONLY_HIGH' },
            { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'BLOCK_ONLY_HIGH' },
            { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'BLOCK_ONLY_HIGH' }
        ],
        generationConfig: modelName === 'gemini-3.5-flash-lite'
            ? {}
            : { thinkingConfig: { thinkingLevel: 'low' } },
        ...(includeTools ? { tools: SHARED_GEMINI_TOOLS } : {})
    };
}

function makeAbortController(externalSignal, timeoutMs) {
    const controller = new AbortController();
    const onExternalAbort = () => {
        try { controller.abort(externalSignal?.reason); } catch (_) { controller.abort(); }
    };
    if (externalSignal) {
        if (externalSignal.aborted) onExternalAbort();
        else externalSignal.addEventListener('abort', onExternalAbort, { once: true });
    }
    const timeoutId = setTimeout(() => controller.abort(), Math.max(250, timeoutMs));
    return {
        controller,
        timeoutId,
        cleanup() {
            clearTimeout(timeoutId);
            if (externalSignal) externalSignal.removeEventListener('abort', onExternalAbort);
        }
    };
}

function getSearchResultText(result) {
    if (!result) return 'جستجوی وب نتیجه‌ای برنگرداند.';
    if (result.ok) return result.result;
    return `[جستجوی وب ناموفق بود | ${result.code || 'search_error'}] ${result.message || 'نتیجه‌ای دریافت نشد.'}`;
}

async function fetchTavilyResults(query, searchCache, externalSignal) {
    const keys = getTavilyKeys();
    if (!keys.length) {
        return {
            ok: false,
            code: 'search_not_configured',
            status: null,
            retryable: false,
            message: 'سرویس جستجو پیکربندی نشده است.'
        };
    }

    const cacheKey = String(query || '').trim().toLowerCase();
    if (!cacheKey) {
        return { ok: false, code: 'search_empty_query', status: 400, retryable: false, message: 'عبارت جستجو خالی بود.' };
    }
    if (searchCache?.has(cacheKey)) return searchCache.get(cacheKey);

    const currentKey = keys[0];
    const keyIndex = keys.indexOf(currentKey) + 1;
    const fail = (code, message, status = null, retryable = false) => {
        const result = { ok: false, code, status, retryable, message };
        if (searchCache) searchCache.set(cacheKey, result);
        return result;
    };

    const abort = makeAbortController(externalSignal, SHARED_TAVILY_TIMEOUT_MS);
    try {
        const response = await fetch('https://api.tavily.com/search', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                api_key: currentKey,
                query: cacheKey,
                search_depth: 'basic',
                max_results: 2
            }),
            signal: abort.controller.signal
        });

        if (!response.ok) {
            let body = null;
            try { body = await response.json(); } catch (_) {}
            markTavilyKeyResult(currentKey, false);
            const status = response.status;
            if (status === 401 || status === 403) return fail('search_invalid_key', 'کلید سرویس جستجو معتبر نیست یا دسترسی آن رد شده است.', status, false);
            if (status === 429) return fail('search_rate_limit', 'سرویس جستجو به محدودیت درخواست رسید.', status, true);
            if (status >= 500) return fail('search_provider_error', 'خود سرویس جستجو موقتاً با خطای سرور مواجه شد.', status, true);
            return fail('search_http_error', `سرویس جستجو درخواست را رد کرد (${status}).`, status, false);
        }

        const data = await response.json();
        if (!Array.isArray(data?.results) || data.results.length === 0) {
            markTavilyKeyResult(currentKey, true);
            return fail('search_no_results', 'جستجو انجام شد اما نتیجه‌ای برای این عبارت پیدا نشد.', 200, false);
        }

        markTavilyKeyResult(currentKey, true);
        const formatted = data.results.map(r =>
            `عنوان: ${r.title || 'بدون عنوان'}\n` +
            `منبع: ${r.url || 'نامشخص'}\n` +
            `محتوا: ${String(r.content || '').slice(0, 1800)}`
        ).join('\n\n---\n\n');

        const success = { ok: true, code: 'search_success', status: 200, result: formatted };
        if (searchCache) searchCache.set(cacheKey, success);
        return success;
    } catch (err) {
        markTavilyKeyResult(currentKey, false);
        if (externalSignal?.aborted) throw err;
        if (err?.name === 'AbortError') return fail('search_timeout', 'جستجوی وب در زمان تعیین‌شده پاسخ نداد.', 408, true);
        return fail('search_network_error', 'ارتباط با سرویس جستجو برقرار نشد.', null, true);
    } finally {
        abort.cleanup();
    }
}

async function runGeminiJsonRequest(contents, modelName, key, externalSignal, timeoutMs, includeTools = true) {
    const abort = makeAbortController(externalSignal, timeoutMs);
    try {
        return await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(modelName)}:generateContent`,
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-goog-api-key': key
                },
                body: JSON.stringify(buildGeminiRequestBody(contents, { includeTools, modelName })),
                signal: abort.controller.signal
            }
        );
    } finally {
        abort.cleanup();
    }
}

async function getBotReply(historyForPrompt, model, externalSignal) {
    const geminiKeys = getGeminiKeys();
    if (!geminiKeys.length) throw new Error('GEMINI_API_KEYS/GEMINI_API_KEY تنظیم نشده است.');

    const startedAt = Date.now();
    const failures = [];
    const modelName = model || DEFAULT_MODEL;
    const searchCache = new Map();
    const searchState = { used: false, result: null };
    let workingContents = [...historyForPrompt];

    for (let keyIdx = 0; keyIdx < geminiKeys.length; keyIdx++) {
        if (externalSignal?.aborted) throw new Error('client_disconnected');
        const key = geminiKeys[keyIdx];
        let remaining = BOT_TOTAL_BUDGET_MS - (Date.now() - startedAt);
        if (remaining < SHARED_GEMINI_MIN_REMAINING_MS) break;
        const timeoutMs = Math.min(SHARED_GEMINI_EFFECTIVE_ATTEMPT_TIMEOUT_MS, Math.max(1000, remaining - 250));

        try {
            const firstResponse = await runGeminiJsonRequest(
                workingContents,
                modelName,
                key,
                externalSignal,
                timeoutMs,
                !searchState.used
            );

            if (!firstResponse.ok) {
                const errorBody = await readGeminiErrorBody(firstResponse);
                const classified = classifyGeminiError({
                    status: firstResponse.status,
                    statusText: firstResponse.statusText,
                    body: errorBody.parsed,
                    message: errorBody.parsed?.error?.message || errorBody.raw
                });
                failures.push(`${geminiKeyLabel(geminiKeys, key)}: HTTP ${firstResponse.status} ${errorBody.raw.slice(0, 280).replace(/\s+/g, ' ')}`);
                markGeminiKeyResult(key, false);
                if (!classified.retryable) break;
                continue;
            }

            const firstData = await firstResponse.json();
            const functionCall = extractFunctionCall(firstData);
            const firstText = extractGeminiText(firstData).trim();

            if (functionCall?.name === 'web_search' && !searchState.used) {
                searchState.used = true;
                const query = String(functionCall.args?.query || '').trim();
                const searchResult = await fetchTavilyResults(query, searchCache, externalSignal);
                searchState.result = searchResult;

                remaining = BOT_TOTAL_BUDGET_MS - (Date.now() - startedAt);
                if (remaining < SHARED_GEMINI_MIN_REMAINING_MS) {
                    if (searchResult?.result) {
                        workingContents = [
                            ...historyForPrompt,
                            { role: 'user', parts: [{ text: `[نتیجه جستجوی وب برای سؤال فعلی]\n${getSearchResultText(searchResult)}` }] }
                        ];
                    }
                    break;
                }

                const followupTimeout = Math.min(SHARED_GEMINI_EFFECTIVE_ATTEMPT_TIMEOUT_MS, Math.max(1000, remaining - 250));
                const followupContents = [
                    ...historyForPrompt,
                    {
                        role: 'model',
                        parts: [{
                            functionCall: functionCall.args
                                ? { name: functionCall.name, args: functionCall.args }
                                : { name: functionCall.name, args: {} },
                            ...(functionCall.thoughtSignature ? { thoughtSignature: functionCall.thoughtSignature } : {})
                        }]
                    },
                    {
                        role: 'user',
                        parts: [{
                            functionResponse: {
                                name: 'web_search',
                                response: {
                                    result: getSearchResultText(searchResult),
                                    searchError: searchResult?.ok ? null : {
                                        code: searchResult?.code || 'search_error',
                                        status: searchResult?.status ?? null
                                    }
                                }
                            }
                        }]
                    }
                ];

                const secondResponse = await runGeminiJsonRequest(
                    followupContents,
                    modelName,
                    key,
                    externalSignal,
                    followupTimeout,
                    false
                );

                if (!secondResponse.ok) {
                    const errorBody = await readGeminiErrorBody(secondResponse);
                    const classified = classifyGeminiError({ status: secondResponse.status, statusText: secondResponse.statusText, body: errorBody.parsed, message: errorBody.parsed?.error?.message || errorBody.raw });
                    failures.push(`${geminiKeyLabel(geminiKeys, key)}: follow-up HTTP ${secondResponse.status} ${errorBody.raw.slice(0, 260).replace(/\s+/g, ' ')}`);
                    markGeminiKeyResult(key, false);
                    if (classified.retryable && searchResult?.result) {
                        // Never search again. Let the next key answer using the saved web result as ordinary context.
                        workingContents = [
                            ...historyForPrompt,
                            { role: 'user', parts: [{ text: `[نتیجه جستجوی وب که همین سؤال قبلاً دریافت کرده است]\n${getSearchResultText(searchResult)}\n\nبا استفاده از همین نتیجه، پاسخ نهایی را بده.` }] }
                        ];
                        continue;
                    }
                    if (!classified.retryable) break;
                    continue;
                }

                const secondData = await secondResponse.json();
                const text = extractGeminiText(secondData).trim();
                if (text) {
                    markGeminiKeyResult(key, true);
                    return text;
                }
                const why = secondData?.candidates?.[0]?.finishReason || secondData?.promptFeedback?.blockReason || 'no candidates';
                failures.push(`${geminiKeyLabel(geminiKeys, key)}: follow-up بدون متن (${String(why).slice(0, 160)})`);
                if (isPermanentNoTextReason(why)) break;
                markGeminiKeyResult(key, false);
                continue;
            }

            if (firstText) {
                markGeminiKeyResult(key, true);
                return firstText;
            }

            const why = firstData?.candidates?.[0]?.finishReason || firstData?.promptFeedback?.blockReason || 'no candidates';
            failures.push(`${geminiKeyLabel(geminiKeys, key)}: پاسخ بدون متن (${String(why).slice(0, 180)})`);
            markGeminiKeyResult(key, !isPermanentNoTextReason(why));
            if (isPermanentNoTextReason(why)) break;
        } catch (err) {
            if (externalSignal?.aborted) throw err;
            const classified = classifyGeminiError(err);
            failures.push(`${geminiKeyLabel(geminiKeys, key)}: ${classified.category === 'timeout' ? 'timeout' : (String(err?.message || err).slice(0, 80) + (err?.status ? ` HTTP ${err.status}` : '') + (err?.rawBody ? ` ${String(err.rawBody).slice(0, 280).replace(/\s+/g, ' ')}` : ''))}`);
            // خطای سطح‌درخواست (400/413) برای همه‌ی کلیدها یکسان است؛ چرخاندن ۱۲ کلید فقط وقت تلف می‌کند.
            if (classified.category === 'invalid_request' || classified.category === 'request_too_large') break;
            markGeminiKeyResult(key, false);
            // If a search already happened, the next key must use its result and may not call web_search again.
            if (searchState.used && searchState.result?.result) {
                workingContents = [
                    ...historyForPrompt,
                    { role: 'user', parts: [{ text: `[نتیجه جستجوی وب قبلی برای همین سؤال]\n${getSearchResultText(searchState.result)}\n\nبا همین نتیجه پاسخ نهایی را بده و دوباره سرچ نکن.` }] }
                ];
            }
        }
    }

    console.error(
        `[shared-chats] Gemini failed (model=${modelName}, ${Date.now() - startedAt}ms): ` +
        `${failures.join(' | ') || 'no attempts'}`
    );
    throw new Error('پاسخ از سرویس هوش مصنوعی دریافت نشد.');
}

async function streamOneGeminiRound({ contents, modelName, key, externalSignal, timeoutMs, includeTools, onText, searchIntent }) {
    const abort = makeAbortController(externalSignal, timeoutMs);
    let idleTimer = null;
    const armIdle = () => {
        if (idleTimer) clearTimeout(idleTimer);
        idleTimer = setTimeout(() => { try { abort.controller.abort(); } catch (_) {} }, SHARED_STREAM_IDLE_MS);
    };
    try {
        const response = await fetch(
            `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(modelName)}:streamGenerateContent?alt=sse`,
            {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'x-goog-api-key': key
                },
                body: JSON.stringify(buildGeminiRequestBody(contents, { includeTools, modelName })),
                signal: abort.controller.signal
            }
        );

        if (!response.ok) {
            const errorBody = await readGeminiErrorBody(response);
            const err = new Error('gemini_upstream_failed');
            err.status = response.status;
            err.statusText = response.statusText;
            err.body = errorBody.parsed;
            err.rawBody = errorBody.raw;
            throw err;
        }
        if (!response.body) throw new Error('Gemini response.body خالی بود');

        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        let buffer = '';
        let accumulatedParts = [];
        let finishReason = null;
        let pendingPreamble = '';
        let preambleTimer = null;
        let preambleFlushed = false;
        let functionCall = null;
        let streamDone = false;

        const emitText = (piece) => {
            if (!piece) return;
            try { onText(piece); } catch (e) { throw e; }
        };

        const flushPending = () => {
            if (!pendingPreamble) return;
            emitText(pendingPreamble);
            pendingPreamble = '';
            preambleFlushed = true;
        };

        if (searchIntent && includeTools) {
            preambleTimer = setTimeout(() => {
                preambleFlushed = true;
                flushPending();
            }, SHARED_SEARCH_PREAMBLE_HOLD_MS);
        }

        const handleEvent = (eventText) => {
            const lines = eventText.split(/\r?\n/);
            const dataLine = lines.find(line => line.startsWith('data:'));
            if (!dataLine) return;
            const jsonStr = dataLine.slice(5).trim();
            if (!jsonStr || jsonStr === '[DONE]') return;

            let parsed;
            try { parsed = JSON.parse(jsonStr); } catch (_) { return; }
            const candidate = parsed?.candidates?.[0];
            if (!candidate) return;
            if (candidate.finishReason) finishReason = candidate.finishReason;

            const parts = Array.isArray(candidate?.content?.parts) ? candidate.content.parts : [];
            const eventHasFunctionCall = parts.some(part => !!part?.functionCall);

            for (const part of parts) {
                if (typeof part?.text === 'string') {
                    const textPart = { text: part.text };
                    if (part.thoughtSignature) textPart.thoughtSignature = part.thoughtSignature;
                    accumulatedParts.push(textPart);
                    if (eventHasFunctionCall) {
                        // Tool preamble: retain but do not show; if a prior timer already flushed it, it remains visible.
                        if (!preambleFlushed) pendingPreamble += part.text;
                    } else if (searchIntent && includeTools && !preambleFlushed && !functionCall) {
                        pendingPreamble += part.text;
                    } else {
                        emitText(part.text);
                    }
                } else if (part?.functionCall?.name) {
                    const fc = {
                        name: part.functionCall.name,
                        args: part.functionCall.args || {},
                        thoughtSignature: part.thoughtSignature || null
                    };
                    accumulatedParts.push({
                        functionCall: { name: fc.name, args: fc.args },
                        ...(fc.thoughtSignature ? { thoughtSignature: fc.thoughtSignature } : {})
                    });
                    if (!functionCall) functionCall = fc;
                    if (preambleTimer) {
                        clearTimeout(preambleTimer);
                        preambleTimer = null;
                    }
                    pendingPreamble = '';
                }
            }
        };

        let interrupted = false;
        let gotFirstChunk = false;
        try {
            while (!streamDone) {
                const { value, done } = await reader.read();
                if (done) break;
                if (!gotFirstChunk) {
                    // اولین داده رسید: تایمر «انتظار اولین بایت» دیگر لازم نیست.
                    gotFirstChunk = true;
                    clearTimeout(abort.timeoutId);
                }
                armIdle();
                buffer += decoder.decode(value, { stream: true });
                let match;
                while ((match = buffer.search(/\r?\n\r?\n/)) !== -1) {
                    const separator = buffer.match(/\r?\n\r?\n/)[0];
                    const eventText = buffer.slice(0, match);
                    buffer = buffer.slice(match + separator.length);
                    handleEvent(eventText);
                }
            }
            buffer += decoder.decode();
            if (buffer.trim()) handleEvent(buffer);
        } catch (readErr) {
            if (preambleTimer) { clearTimeout(preambleTimer); preambleTimer = null; }
            if (externalSignal?.aborted) throw readErr;
            const hadText = accumulatedParts.some(p => typeof p.text === 'string' && p.text);
            if (!hadText && !functionCall) throw readErr; // چیزی نرسیده: بگذار کلید بعدی امتحان شود
            interrupted = true; // متن ناقص رسیده: به‌جای دور ریختن، برگردان تا ادامه خواسته شود
        }

        if (preambleTimer) clearTimeout(preambleTimer);
        if (!functionCall && pendingPreamble) flushPending();
        return {
            text: accumulatedParts.filter(p => typeof p.text === 'string').map(p => p.text).join(''),
            parts: accumulatedParts.filter(p => typeof p.text === 'string' && p.text),
            functionCall,
            finishReason,
            interrupted
        };
    } finally {
        if (idleTimer) clearTimeout(idleTimer);
        abort.cleanup();
    }
}

// یک round استریم + ادامه‌ی خودکار اگر Gemini وسط پاسخ اتصال را بست.
// «ناقص» یعنی متن رسیده ولی هیچ finishReason (STOP/MAX_TOKENS/...) نیامده.
// در این حالت متن قبلی را به‌عنوان نوبت model و یک دستور «ادامه بده» به‌عنوان
// نوبت user اضافه می‌کنیم؛ متن جدید مستقیم به ادامه‌ی همان پیام پخش می‌شود.
// FIX: کمک‌تابع برای رفع کلمه‌ی نصفه هنگام «ادامه‌ی استریم». آخرین بخش متنیِ
// آرایه‌ی parts (نوبت model) را می‌گیرد، اگر به فاصله/newline/علامت نگارشی
// ختم نمی‌شود (یعنی وسط یک کلمه قطع شده)، همان کلمه‌ی ناقصِ انتهایی را حذف
// می‌کند تا وقتی به مدل به‌عنوان تاریخچه پس داده می‌شود، از مرز یک کلمه‌ی
// کامل ادامه بگیرد، نه وسط آن. اگر متن با فاصله/نگارش تمام شده (یعنی قطعی
// دقیقاً بین دو کلمه بوده)، دست‌نخورده برمی‌گردد.
function trimTrailingPartialWord(parts) {
    if (!Array.isArray(parts) || !parts.length) return parts;
    const result = parts.map(p => ({ ...p }));
    for (let i = result.length - 1; i >= 0; i--) {
        if (typeof result[i].text !== 'string' || !result[i].text) continue;
        const text = result[i].text;
        // اگر آخرین کاراکتر فاصله/نیم‌فاصله/newline/علامت نگارشی است، کلمه کامل بوده - دست نزن.
        if (/[\s\u200c.,!?؛،:؟\-)\]}»"'`]$/.test(text)) return result;
        // آخرین «کلمه»ی ناقص را (تا اولین فاصله/نیم‌فاصله قبل از انتهای متن) پیدا و حذف کن.
        const cut = text.search(/[\s\u200c]+\S*$/);
        result[i].text = cut === -1 ? '' : text.slice(0, cut + 1);
        return result;
    }
    return result;
}

async function streamRoundWithRecovery({ contents, modelName, key, externalSignal, firstByteTimeoutMs, includeTools, onText, searchIntent }) {
    let workingContents = contents;
    let combinedText = '';
    for (let attempt = 0; ; attempt++) {
        const round = await streamOneGeminiRound({
            contents: workingContents,
            modelName,
            key,
            externalSignal,
            timeoutMs: firstByteTimeoutMs,
            includeTools: attempt === 0 ? includeTools : false,
            onText,
            searchIntent: attempt === 0 ? searchIntent : false
        });
        combinedText += round.text;
        if (round.functionCall) return { ...round, text: combinedText };

        const incomplete = !round.finishReason && round.text.trim().length > 0;
        if (!incomplete) return { ...round, text: combinedText };

        if (attempt >= SHARED_MAX_STREAM_RECOVERIES || externalSignal?.aborted) {
            console.error(`[shared-chats] stream incomplete after ${attempt} recoveries (model=${modelName}, chars=${combinedText.length}, interrupted=${round.interrupted})`);
            const marker = '\n\n⚠️ پاسخ کامل نشد';
            try { onText(marker); } catch (_) {}
            return { ...round, text: combinedText + marker, finishReason: 'INCOMPLETE_STREAM' };
        }
        console.warn(`[shared-chats] stream cut without finishReason; continuing (attempt ${attempt + 1}/${SHARED_MAX_STREAM_RECOVERIES}, chars=${combinedText.length}, interrupted=${round.interrupted})`);
        // FIX (کلمه‌ی نصفه‌شده هنگام ادامه‌ی استریم، مثل «چطورتری»/«می‌آاد»):
        // وقتی استریم دقیقاً وسط یک کلمه قطع می‌شود، مدل در ادامه، بخشی از همان
        // کلمه‌ی ناقص را از نو (کمی متفاوت) می‌نویسد و به باقی‌مانده‌ی قبلی می‌چسبد.
        // راه‌حل: آخرین کلمه‌ی ناقص را از انتهای متنی که به‌عنوان تاریخچه به مدل
        // پس داده می‌شود (parts مربوط به نقش model) قطع می‌کنیم تا مدل دقیقاً از
        // یک مرز کلمه/فاصله ادامه بدهد، نه وسط یک کلمه. کاراکترهای بریده‌شده را
        // به‌عنوان چانک منفی به onText نمی‌فرستیم چون قبلاً استریم شده‌اند؛ فقط
        // در تاریخچه‌ی ارسالی به مدل حذف می‌شوند - combinedText خودش دست‌نخورده
        // می‌ماند چون هرچه کاربر تا این لحظه دیده را نباید از UI پاک کنیم.
        const trimmedParts = trimTrailingPartialWord(round.parts);
        workingContents = [
            ...workingContents,
            { role: 'model', parts: trimmedParts },
            { role: 'user', parts: [{ text: SHARED_CONTINUE_PROMPT }] }
        ];
    }
}

async function streamBotReplyWithModel(historyForPrompt, model, onChunk, externalSignal, onStep) {
    const geminiKeys = getGeminiKeys();
    if (!geminiKeys.length) throw new Error('GEMINI_API_KEYS/GEMINI_API_KEY تنظیم نشده است.');

    const startedAt = Date.now();
    const failures = [];
    const modelName = model || DEFAULT_MODEL;
    const searchCache = new Map();
    const searchState = { used: false, result: null };
    // FIX (متن ربات بعضی وقت‌ها یک‌جا می‌آمد): قبلاً قصد جستجو از «کل تاریخچه‌ی ۱۰۰ پیامی»
    // (شامل جواب‌های خود ربات) حساب می‌شد؛ کافی بود هر پیامی در آن پنجره کلمه‌ای مثل «امروز/قیمت/جدید»
    // داشته باشد تا برای همه‌ی پاسخ‌های بعدی، متن تا ۱.۵ ثانیه نگه داشته شود (preamble hold) و
    // یک‌جا بیاید. چت عادی (pages/api/chat.js) فقط پیام آخر کاربر را می‌سنجد؛ اینجا هم همان.
    const lastUserTurn = [...historyForPrompt].reverse().find(x => x?.role === 'user');
    const lastUserText = (lastUserTurn?.parts || [])
        .map(p => p?.text || '')
        .join(' ')
        .replace(/^\[[^\]]*\]:\s*/, ''); // برچسب «[نام]:» جزو پیام نیست
    const searchIntent = looksLikeWebSearchIntent(lastUserText);
    let accumulatedAnswer = '';
    let workingContents = [...historyForPrompt];
    let consecutiveUnavailable = 0;

    for (let keyIdx = 0; keyIdx < geminiKeys.length; keyIdx++) {
        if (externalSignal?.aborted) return accumulatedAnswer.trim();
        const key = geminiKeys[keyIdx];
        const remainingBeforeAttempt = BOT_TOTAL_BUDGET_MS - (Date.now() - startedAt);
        if (remainingBeforeAttempt < SHARED_GEMINI_MIN_REMAINING_MS) break;
        const timeoutMs = Math.min(SHARED_STREAM_FIRST_BYTE_TIMEOUT_MS, Math.max(1000, remainingBeforeAttempt - 250));

        try {
            const round = await streamRoundWithRecovery({
                contents: workingContents,
                modelName,
                key,
                externalSignal,
                firstByteTimeoutMs: timeoutMs,
                includeTools: !searchState.used,
                onText: piece => {
                    accumulatedAnswer += piece;
                    try { onChunk(piece); } catch (_) {}
                },
                searchIntent
            });

            if (round.functionCall?.name === 'web_search' && !searchState.used) {
                searchState.used = true;
                const query = String(round.functionCall.args?.query || '').trim();
                const reason = String(round.functionCall.args?.reason || '').trim();
                if (onStep) {
                    try { onStep(reason || `دارم درباره‌ی «${query}» توی وب سرچ می‌کنم...`, 'web_search'); } catch (_) {}
                }
                const searchResult = await fetchTavilyResults(query, searchCache, externalSignal);
                searchState.result = searchResult;

                const remainingAfterSearch = BOT_TOTAL_BUDGET_MS - (Date.now() - startedAt);
                if (remainingAfterSearch < SHARED_GEMINI_MIN_REMAINING_MS) {
                    return accumulatedAnswer.trim();
                }

                // The original tool call is valid Gemini context for the immediate follow-up.
                const followupContents = [
                    ...historyForPrompt,
                    {
                        role: 'model',
                        parts: [{
                            functionCall: { name: 'web_search', args: round.functionCall.args || {} },
                            ...(round.functionCall.thoughtSignature ? { thoughtSignature: round.functionCall.thoughtSignature } : {})
                        }]
                    },
                    {
                        role: 'user',
                        parts: [{
                            functionResponse: {
                                name: 'web_search',
                                response: {
                                    result: getSearchResultText(searchResult),
                                    searchError: searchResult?.ok ? null : {
                                        code: searchResult?.code || 'search_error',
                                        status: searchResult?.status ?? null
                                    }
                                }
                            }
                        }]
                    }
                ];

                const secondTimeout = Math.min(
                    SHARED_STREAM_FIRST_BYTE_TIMEOUT_MS,
                    Math.max(1000, remainingAfterSearch - 250)
                );
                const secondRound = await streamRoundWithRecovery({
                    contents: followupContents,
                    modelName,
                    key,
                    externalSignal,
                    firstByteTimeoutMs: secondTimeout,
                    includeTools: false,
                    onText: piece => {
                        accumulatedAnswer += piece;
                        try { onChunk(piece); } catch (_) {}
                    },
                    searchIntent: false
                });

                if (secondRound.text.trim()) {
                    markGeminiKeyResult(key, true);
                    return accumulatedAnswer.trim();
                }

                const why = secondRound.finishReason || 'follow-up بدون متن';
                failures.push(`${geminiKeyLabel(geminiKeys, key)}: follow-up بدون متن (${String(why).slice(0, 160)})`);
                if (isPermanentNoTextReason(why)) break;
                markGeminiKeyResult(key, false);
                // Keep search result for next key, but don't call web_search again.
                workingContents = [
                    ...historyForPrompt,
                    { role: 'user', parts: [{ text: `[نتیجه جستجوی وب قبلی برای همین سؤال]\n${getSearchResultText(searchResult)}\n\nبا همین نتیجه پاسخ نهایی را بده و دوباره سرچ نکن.` }] }
                ];
                continue;
            }

            if (round.text.trim()) {
                markGeminiKeyResult(key, true);
                return accumulatedAnswer.trim();
            }

            const why = round.finishReason || 'no candidates';
            failures.push(`${geminiKeyLabel(geminiKeys, key)}: پاسخ استریم بدون متن (${String(why).slice(0, 160)})`);
            markGeminiKeyResult(key, !isPermanentNoTextReason(why));
            if (isPermanentNoTextReason(why)) break;
        } catch (err) {
            if (externalSignal?.aborted) return accumulatedAnswer.trim();
            const classified = classifyGeminiError(err);
            failures.push(`${geminiKeyLabel(geminiKeys, key)}: ${classified.category === 'timeout' ? 'timeout' : (String(err?.message || err).slice(0, 80) + (err?.status ? ` HTTP ${err.status}` : '') + (err?.rawBody ? ` ${String(err.rawBody).slice(0, 280).replace(/\s+/g, ' ')}` : ''))}`);

            // Never duplicate already-visible streamed text on a different key.
            if (accumulatedAnswer.trim()) {
                console.error(`[shared-chats] stream interrupted mid-way (model=${modelName}, ${geminiKeyLabel(geminiKeys, key)}): ${err?.message || err}`);
                return accumulatedAnswer.trim();
            }

            // خطای سطح‌درخواست (400/413) برای همه‌ی کلیدها یکسان است؛ چرخاندن ۱۲ کلید فقط وقت تلف می‌کند.
            if (classified.category === 'invalid_request' || classified.category === 'request_too_large') break;
            // 503 «high demand» مشکل کل مدل است نه یک کلید؛ بعد از ۳ کلید پشت‌سرهم بیهوده ۱۲ کلید را نچرخان، برو سراغ مدل جایگزین.
            if (classified.category === 'provider_unavailable') {
                consecutiveUnavailable++;
                if (consecutiveUnavailable >= 3) break;
            } else {
                consecutiveUnavailable = 0;
            }
            markGeminiKeyResult(key, false);
            if (searchState.used && searchState.result?.result) {
                workingContents = [
                    ...historyForPrompt,
                    { role: 'user', parts: [{ text: `[نتیجه جستجوی وب قبلی برای همین سؤال]\n${getSearchResultText(searchState.result)}\n\nبا همین نتیجه پاسخ نهایی را بده و دوباره سرچ نکن.` }] }
                ];
            }
        }
    }

    console.error(
        `[shared-chats] Gemini stream failed (model=${modelName}, ${Date.now() - startedAt}ms): ` +
        `${failures.join(' | ') || 'no attempts'}`
    );
    throw new Error('پاسخ از سرویس هوش مصنوعی دریافت نشد.');
}

// ===== Model Fallback (همان منطق pages/api/chat.js) =====
// اگر مدل انتخاب‌شده‌ی چت شلوغ (503) یا ناموجود بود، مدل‌های جایگزین به ترتیب
// امتحان می‌شوند. چت عادی همین کار را می‌کرد ولی چت مشترک نه، برای همین با
// «This model is currently experiencing high demand» کاملاً از کار می‌افتاد.
const SHARED_MODEL_FALLBACKS = {
    'gemini-3.8-flash': ['gemini-3.6-flash', 'gemini-3.5-flash-lite'],
    'gemini-3.6-flash': ['gemini-3.5-flash-lite'],
    'gemini-3.1-pro-preview': ['gemini-3.6-flash', 'gemini-3.5-flash-lite']
};

async function streamBotReply(historyForPrompt, model, onChunk, externalSignal, onStep) {
    const primary = model || DEFAULT_MODEL;
    const modelsToTry = [primary, ...(SHARED_MODEL_FALLBACKS[primary] || [])];
    let emittedAny = false;
    const trackedOnChunk = (piece) => { if (piece) emittedAny = true; onChunk(piece); };
    let lastErr = null;
    for (let i = 0; i < modelsToTry.length; i++) {
        if (externalSignal?.aborted) return '';
        try {
            if (i > 0) console.warn(`[shared-chats] falling back to model ${modelsToTry[i]} (primary=${primary})`);
            return await streamBotReplyWithModel(historyForPrompt, modelsToTry[i], trackedOnChunk, externalSignal, onStep);
        } catch (err) {
            lastErr = err;
            // اگر چیزی از پاسخ قبلاً به کاربر رسیده، مدل عوض نکن تا متن تکراری/ناجور نشود.
            if (emittedAny) throw err;
        }
    }
    throw lastErr || new Error('پاسخ از سرویس هوش مصنوعی دریافت نشد.');
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

// ===== نام نمایشی به‌جای ایمیل =====
// امنیت/حریم خصوصی: قبلاً سرور sender_email هر پیام را (ایمیل کامل!) برای همه‌ی
// اعضا می‌فرستاد و ایمیل کامل سازنده هم در لیست چت‌ها بود؛ اپ فقط بخش قبل از @
// را نشان می‌داد ولی کل ایمیل در پاسخ شبکه‌ بود. حالا هیچ ایمیلی به کلاینت
// نمی‌رسد: هر پیام sender_name (نام نمایشی حساب) و is_mine دارد.
// نام نمایشی همان چیزی است که اپ در حافظه‌ی حساب (user_memory) با کلید زیر ذخیره
// می‌کند (نگاه کن به UserMemoryClient.ACCOUNT_NAME_KEY).
const ACCOUNT_DISPLAY_NAME_KEY = '__vc_account_display_name';

function toPersianDigits(n) {
    return String(n).replace(/[0-9]/g, d => '۰۱۲۳۴۵۶۷۸۹'[d]);
}

function sanitizeDisplayName(raw) {
    const name = String(raw || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 40);
    // اگر کاربر ایمیلش را به‌عنوان اسم گذاشته، همان را افشا نکن.
    if (!name || name.includes('@')) return '';
    return name;
}

// ایمیل‌ها -> {email: name}. اگر کسی اسم نداشت «کاربر N» (N = ترتیب عضویت در همین
// چت)، و اگر دو نفر اسم یکسان داشتند، برای تفکیک شماره‌ی ترتیبی اضافه می‌شود.
async function resolveDisplayNames(chatId, emails) {
    const unique = [...new Set((emails || []).filter(Boolean).map(e => String(e).toLowerCase()))];
    const names = {};
    if (!unique.length) return names;

    try {
        const inList = unique.map(e => `"${e.replace(/"/g, '')}"`).join(',');
        const resp = await supaFetch(
            `user_memory?owner_email=in.(${encodeURIComponent(inList)})&key=eq.${ACCOUNT_DISPLAY_NAME_KEY}&select=owner_email,value`
        );
        const rows = resp.ok ? await resp.json() : [];
        if (Array.isArray(rows)) {
            for (const r of rows) {
                const clean = sanitizeDisplayName(r.value);
                if (clean) names[String(r.owner_email).toLowerCase()] = clean;
            }
        }
    } catch (_) { /* بدون اسم: fallback پایین */ }

    const needsFallback = unique.some(e => !names[e]);
    const dupes = new Set();
    {
        const seen = new Set();
        for (const e of unique) {
            const n = names[e];
            if (!n) continue;
            if (seen.has(n)) dupes.add(n);
            seen.add(n);
        }
    }
    if (needsFallback || dupes.size) {
        let order = [];
        try {
            const pResp = await supaFetch(
                `shared_chat_participants?chat_id=eq.${encodeURIComponent(chatId)}&select=email&order=joined_at.asc`
            );
            const pRows = pResp.ok ? await pResp.json() : [];
            order = Array.isArray(pRows) ? pRows.map(r => String(r.email).toLowerCase()) : [];
        } catch (_) { /* ترتیب نامشخص */ }
        for (const e of unique) {
            const idx = order.indexOf(e);
            const ordinal = toPersianDigits(idx >= 0 ? idx + 1 : unique.indexOf(e) + 1);
            if (!names[e]) names[e] = `کاربر ${ordinal}`;
            else if (dupes.has(names[e])) names[e] = `${names[e]} (${ordinal})`;
        }
    }
    return names;
}

// پیام‌ها را برای ارسال به کلاینت آماده می‌کند: بدون sender_email.
async function presentMessages(chatId, messages, requesterEmail) {
    const list = Array.isArray(messages) ? messages : [];
    const senderEmails = list.filter(m => m && m.role === 'user' && m.sender_email).map(m => m.sender_email);
    const names = await resolveDisplayNames(chatId, senderEmails);
    const me = String(requesterEmail || '').toLowerCase();
    return list.map(m => {
        if (!m) return m;
        const { sender_email, ...rest } = m;
        const senderLower = sender_email ? String(sender_email).toLowerCase() : '';
        return {
            ...rest,
            sender_name: m.role === 'user' && senderLower ? (names[senderLower] || 'کاربر') : null,
            is_mine: m.role === 'user' && !!senderLower && senderLower === me
        };
    });
}

async function presentMessage(chatId, message, requesterEmail) {
    return (await presentMessages(chatId, [message], requesterEmail))[0];
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
                return res.status(200).json({ messages: await presentMessages(chatId, list, email) });
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
            // ایمیل سازنده را برای اعضا نفرست؛ فقط بگو «این کاربر سازنده است یا نه».
            const items = (Array.isArray(chats) ? chats : []).map(({ owner_email, ...c }) => ({
                ...c,
                is_owner: String(owner_email || '').toLowerCase() === email
            }));
            return res.status(200).json({ items });
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

        // ===== POST action=setmodel: تغییر مدل یک چت مشترک =====
        // مطابق طراحی اولیه، فقط سازنده‌ی چت می‌تواند مدل را عوض کند (بقیه فقط
        // مدل فعلی را می‌بینند). مدل فقط از لیست ALLOWED_MODELS پذیرفته می‌شود.
        if (action === 'setmodel') {
            const chatId = String(req.body?.chatId || '').trim();
            const newModel = String(req.body?.model || '').trim();
            if (!chatId || !newModel) {
                return res.status(400).json({ error: 'chatId یا model مشخص نشده.' });
            }
            if (!ALLOWED_MODELS.includes(newModel)) {
                return res.status(400).json({ error: 'مدل انتخاب‌شده معتبر نیست.', allowedModels: ALLOWED_MODELS });
            }
            if (!(await isParticipant(chatId, email))) {
                return res.status(403).json({ error: 'عضو این گفتگوی مشترک نیستی.' });
            }
            const ownerResp = await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}&select=owner_email`);
            const ownerRows = ownerResp.ok ? await ownerResp.json() : [];
            const ownerEmail = String(ownerRows?.[0]?.owner_email || '').toLowerCase();
            if (!ownerEmail || ownerEmail !== email) {
                return res.status(403).json({ error: 'فقط سازنده‌ی چت می‌تواند مدل را عوض کند.' });
            }
            const patchResp = await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}`, {
                method: 'PATCH',
                body: JSON.stringify({ model: newModel, updated_at: Date.now() })
            });
            if (!patchResp.ok) {
                console.error('[shared-chats] setmodel failed:', await patchResp.text().catch(() => ''));
                return res.status(500).json({ error: 'تغییر مدل ناموفق بود.' });
            }
            return res.status(200).json({ ok: true, model: newModel });
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
                return res.status(200).json({ message: await presentMessage(chatId, userMsg, email), botPending: true });
            }

            try {
                const historyResp = await supaFetch(
                    `shared_chat_messages?chat_id=eq.${encodeURIComponent(chatId)}&select=id,role,sender_email,text&order=id.desc&limit=100`
                );
                const historyRowsDesc = await historyResp.json();
                // از جدید به قدیم گرفتیم (تا limit روی «آخرین ۱۰۰ پیام» اعمال شود)؛ برای prompt برعکس می‌کنیم.
                const historyRows = Array.isArray(historyRowsDesc) ? historyRowsDesc.reverse() : [];
                // به‌جای ایمیل، نام نمایشی در پرامپت (تا مدل ایمیل کسی را در جواب لو ندهد).
                const promptNames = await resolveDisplayNames(chatId, historyRows.map(r => r.sender_email));

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
                    const label = row.role === 'user' && row.sender_email ? `[${promptNames[String(row.sender_email).toLowerCase()] || 'کاربر'}]: ` : '';
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
                return res.status(200).json({ message: await presentMessage(chatId, userMsg, email), botMessage: await presentMessage(chatId, botMsg, email) });
            } catch (err) {
                console.error('[shared-chats] bot reply failed:', err?.message || err);
                // پیام کاربر خودش با موفقیت ذخیره شده؛ فقط جواب ربات نرسید -
                // این را جدا اعلام می‌کنیم تا کلاینت پیام کاربر را از دست
                // ندهد، فقط بگوید «ربات جواب نداد، دوباره امتحان کن».
                return res.status(502).json({ message: await presentMessage(chatId, userMsg, email), error: 'پاسخ ربات دریافت نشد.' });
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
                if (res.writableEnded || res.destroyed) return;
                try {
                    res.write(`data: ${JSON.stringify(obj)}\n\n`);
                    if (typeof res.flush === 'function') res.flush();
                } catch (_) {}
            };

            // Stop/cancel propagation: when the Shared Chat client aborts its
            // fetch (e.g. the Android-style red ■ Stop button), abort the same
            // request signal used by Gemini/Tavily so upstream generation really
            // stops instead of continuing invisibly for the rest of the budget.
            const clientAbortController = new AbortController();
            let responseFinished = false;
            const abortForDisconnect = () => {
                if (!responseFinished) {
                    try { clientAbortController.abort(new Error('client_disconnected')); } catch (_) { clientAbortController.abort(); }
                }
            };
            req.once('aborted', abortForDisconnect);
            res.once('close', abortForDisconnect);
            res.once('finish', () => { responseFinished = true; });
            // اولین event: پیام کاربر (با id واقعی سرور) - کلاینت این را
            // فوری جایگزین نسخه‌ی optimistic خودش می‌کند، دقیقاً مثل چیزی
            // که قبلاً از فیلد "message" در پاسخ غیر-استریمی می‌خواند.
            sendEvent({ userMessage: await presentMessage(chatId, userMsg, email) });

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
                // به‌جای ایمیل، نام نمایشی در پرامپت (تا مدل ایمیل کسی را در جواب لو ندهد).
                const promptNames = await resolveDisplayNames(chatId, historyRows.map(r => r.sender_email));

                const attMap = await fetchAttachmentsForMessages(historyRows.map(r => r.id));
                const allImages = [];
                for (const row of historyRows) {
                    for (const att of (attMap[row.id] || [])) allImages.push({ msgId: row.id, att });
                }
                const sendableImages = new Set(allImages.slice(-MAX_IMAGES_TO_BOT).map(x => x.att.id));

                const historyForPrompt = [];
                for (const row of historyRows) {
                    const parts = [];
                    const label = row.role === 'user' && row.sender_email ? `[${promptNames[String(row.sender_email).toLowerCase()] || 'کاربر'}]: ` : '';
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
                const botText = await streamBotReply(
                    historyForPrompt,
                    chatModel,
                    (piece) => { sendEvent({ text: piece }); },
                    clientAbortController.signal,
                    (label, toolName) => sendEvent({ status: 'tool', tool: toolName, text: label })
                );
                const interruptedByClient = clientAbortController.signal.aborted;
                const savedBotText = interruptedByClient
                    ? (botText ? `${botText}\n\n⏹ پاسخ متوقف شد` : '')
                    : botText;
                const botMsg = savedBotText
                    ? await insertMessage(chatId, 'model', null, savedBotText)
                    : null;
                await supaFetch(`shared_chats?chat_id=eq.${encodeURIComponent(chatId)}`, {
                    method: 'PATCH',
                    body: JSON.stringify({ updated_at: Date.now() })
                });
                if (botMsg) botMsg.attachments = [];
                if (!clientAbortController.signal.aborted) {
                    sendEvent({ done: true, botMessage: await presentMessage(chatId, botMsg, email) });
                    return res.end();
                }
                try { res.end(); } catch (_) {}
                return;
            } catch (err) {
                if (!clientAbortController.signal.aborted) {
                    console.error('[shared-chats] stream bot reply failed:', err?.message || err);
                    sendEvent({ error: 'پاسخ ربات دریافت نشد.' });
                }
                try { res.end(); } catch (_) {}
                return;
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
