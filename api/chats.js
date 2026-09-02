// api/chats.js
//
// Sync گفتگوها/فایل‌ها بین دستگاه‌ها برای Virtual Chat.
// از Supabase (Postgres) استفاده می‌کند - جدول‌های chats/chat_history/
// chat_files/sessions که با schema.sql ساخته شده‌اند.
//
// امنیت: کلاینت بعد از لاگین موفق (api/auth.js) یک session token تصادفی
// می‌گیرد و آن را در هر درخواست در هدر Authorization می‌فرستد. این‌جا آن
// توکن را در جدول sessions جستجو می‌کنیم؛ اگر پیدا شد و منقضی نشده، ایمیل
// متناظرش را owner_email قابل‌اعتماد می‌دانیم. هیچ ارتباطی با گوگل یا هر
// سرویس بیرونی دیگر لازم نیست.
//
// GET    /api/chats                    -> لیست همه‌ی چت‌های همین کاربر (بدون html/history سنگین - فقط متادیتا برای رندر سایدبار)
// GET    /api/chats?chatId=...         -> یک چت کامل (html + history + files)
// PUT    /api/chats?chatId=...         -> ذخیره/آپدیت یک چت (title/html/pinned/updatedAt + history + files)
// DELETE /api/chats?chatId=...         -> حذف یک چت
// DELETE /api/chats?ids=id1,id2,id3    -> حذف دسته‌جمعی (برای انتخاب چندتایی توی سایدبار)
// POST   /api/chats?action=upload&chatId=...&name=...  -> آپلود یک فایل ضمیمه (body: { base64 })
// GET    /api/chats?action=download&chatId=...&name=... -> دانلود یک فایل ضمیمه (پاسخ: { base64 })
//
// نیازمندی‌های محیطی (روی Vercel، Settings -> Environment Variables):
//   SUPABASE_URL               - Project URL از داشبورد Supabase
//   SUPABASE_SERVICE_ROLE_KEY  - کلید secret/service_role (فقط همین‌جا، هرگز سمت کلاینت)

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
const MAX_CHATS_PER_USER = 500; // سقف امنیتی؛ خیلی بالاتر از چیزی‌ست که یک کاربر واقعی به آن می‌رسد
const MAX_HTML_SIZE = 2 * 1024 * 1024; // 2MB برای HTML رندرشده‌ی هر چت
const MAX_HISTORY_SIZE = 4 * 1024 * 1024; // 4MB برای JSON تاریخچه (فایل‌های متنی بزرگ هم این‌جا می‌روند)
const STORAGE_BUCKET = 'chat-attachments';
const MAX_UPLOAD_SIZE = 10 * 1024 * 1024; // 10MB per file - بیشتر از این توی باکت رایگان 1GB سریع پر می‌شود

function setCors(res) {
    res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
    res.setHeader('Access-Control-Allow-Methods', 'GET, PUT, POST, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// ===== تأیید هویت با session token داخلی (بدون گوگل) =====
async function verifySessionToken(token) {
    if (!token) return null;
    try {
        const resp = await supaFetch(`sessions?token=eq.${encodeURIComponent(token)}&select=email,expires_at`);
        if (!resp.ok) return null;
        const rows = await resp.json();
        if (!Array.isArray(rows) || !rows.length) return null;
        const session = rows[0];
        if (Number(session.expires_at) < Date.now()) return null; // منقضی شده
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

// ===== کمکی‌های Supabase REST (بدون نیاز به @supabase/supabase-js) =====
// چون فقط چند query ساده لازم داریم، مستقیم از PostgREST (همان چیزی که
// SUPABASE_URL/rest/v1/ می‌دهد) استفاده می‌کنیم تا یک وابستگی کامل npm
// اضافه نشود.
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

// مسیر فایل توی باکت: <ایمیل کاربر>/<chatId>/<نام فایل> - این‌طوری هر
// کاربر فقط توی پوشه‌ی خودش می‌نویسد/می‌خواند و تصادم نام فایل بین دو
// کاربر مختلف یا دو چت مختلف پیش نمی‌آید.
function storageObjectPath(ownerEmail, chatId, fileName) {
    const safeName = String(fileName || 'file').replace(/[^\w.\-]+/g, '_').slice(0, 150);
    return `${encodeURIComponent(ownerEmail)}/${encodeURIComponent(chatId)}/${Date.now()}_${safeName}`;
}

async function uploadToStorage(objectPath, buffer, contentType) {
    const resp = await fetch(`${SUPABASE_URL}/storage/v1/object/${STORAGE_BUCKET}/${objectPath}`, {
        method: 'POST',
        headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
            'Content-Type': contentType || 'application/octet-stream',
            'x-upsert': 'true'
        },
        body: buffer
    });
    return resp;
}

async function downloadFromStorage(objectPath) {
    const resp = await fetch(`${SUPABASE_URL}/storage/v1/object/${STORAGE_BUCKET}/${objectPath}`, {
        headers: {
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`
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
        return res.status(500).json({ error: 'سرور برای sync گفتگوها تنظیم نشده (SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY موجود نیست).' });
    }

    const ownerEmail = await verifySessionToken(getBearerToken(req));
    if (!ownerEmail) {
        return res.status(401).json({ error: 'ورود تأیید نشد. دوباره وارد شو.' });
    }

    try {
        // ===== GET: لیست همه‌ی چت‌ها (متادیتا)، یک چت کامل، یا دانلود فایل =====
        if (req.method === 'GET') {
            const chatId = req.query?.chatId;
            const action = req.query?.action;

            if (action === 'download') {
                const fileName = req.query?.path; // مسیر کامل ذخیره‌شده (نه فقط اسم اصلی)، از همان چیزی که موقع آپلود برگردانده شده
                if (!fileName) return res.status(400).json({ error: 'path مشخص نشده.' });
                // چک امنیتی: مسیر باید زیرمجموعه‌ی پوشه‌ی همین کاربر باشد،
                // وگرنه یک کاربر می‌تواند با حدس زدن مسیر، فایل کاربر دیگر
                // را بخواهد (چون این endpoint خودش هویت را از توکن گوگل
                // گرفته، نه از پارامتر - پس این چک واقعاً معنا دارد).
                if (!String(fileName).startsWith(`${encodeURIComponent(ownerEmail)}/`)) {
                    return res.status(403).json({ error: 'دسترسی مجاز نیست.' });
                }
                const storageResp = await downloadFromStorage(fileName);
                if (!storageResp.ok) return res.status(404).json({ error: 'فایل پیدا نشد.' });
                const arrayBuf = await storageResp.arrayBuffer();
                const base64 = Buffer.from(arrayBuf).toString('base64');
                const contentType = storageResp.headers.get('content-type') || 'application/octet-stream';
                return res.status(200).json({ base64, contentType });
            }

            if (chatId) {
                const [chatRes, historyRes, filesRes] = await Promise.all([
                    supaFetch(`chats?owner_email=eq.${encodeURIComponent(ownerEmail)}&chat_id=eq.${encodeURIComponent(chatId)}&select=*`),
                    supaFetch(`chat_history?owner_email=eq.${encodeURIComponent(ownerEmail)}&chat_id=eq.${encodeURIComponent(chatId)}&select=history`),
                    supaFetch(`chat_files?owner_email=eq.${encodeURIComponent(ownerEmail)}&chat_id=eq.${encodeURIComponent(chatId)}&select=files`)
                ]);
                const chatRows = await chatRes.json();
                const historyRows = await historyRes.json();
                const filesRows = await filesRes.json();
                if (!Array.isArray(chatRows) || !chatRows.length) {
                    return res.status(404).json({ error: 'گفتگو پیدا نشد.' });
                }
                return res.status(200).json({
                    chat: chatRows[0],
                    history: (historyRows[0] && historyRows[0].history) || [],
                    files: (filesRows[0] && filesRows[0].files) || []
                });
            }

            // فقط متادیتای سبک (بدون html/history سنگین) برای رندر اولیه‌ی
            // سایدبار - محتوای کامل هر چت با درخواست جدا (بالا) وقتی کاربر
            // واقعاً همان چت را باز می‌کند گرفته می‌شود.
            const listRes = await supaFetch(
                `chats?owner_email=eq.${encodeURIComponent(ownerEmail)}&select=chat_id,title,pinned,updated_at&order=updated_at.desc&limit=${MAX_CHATS_PER_USER}`
            );
            const rows = await listRes.json();
            return res.status(200).json({ items: Array.isArray(rows) ? rows : [] });
        }

        // ===== PUT: ذخیره/آپدیت یک چت =====
        if (req.method === 'PUT') {
            const chatId = req.query?.chatId;
            if (!chatId) return res.status(400).json({ error: 'chatId مشخص نشده.' });

            const { title, html, pinned, updatedAt, history, files } = req.body || {};

            if (typeof html === 'string' && html.length > MAX_HTML_SIZE) {
                return res.status(413).json({ error: 'محتوای این گفتگو خیلی بزرگ است.' });
            }
            const historyStr = history !== undefined ? JSON.stringify(history) : null;
            if (historyStr && historyStr.length > MAX_HISTORY_SIZE) {
                return res.status(413).json({ error: 'تاریخچه‌ی این گفتگو خیلی بزرگ است.' });
            }

            // سقف تعداد چت هر کاربر - جلوگیری از رشد بی‌نهایت دیتابیس رایگان
            const countRes = await supaFetch(
                `chats?owner_email=eq.${encodeURIComponent(ownerEmail)}&select=chat_id&chat_id=neq.${encodeURIComponent(chatId)}`,
                { headers: { 'Prefer': 'count=exact' } }
            );
            const countHeader = countRes.headers.get('content-range');
            const currentCount = countHeader ? parseInt(countHeader.split('/')[1], 10) || 0 : 0;
            if (currentCount >= MAX_CHATS_PER_USER) {
                return res.status(403).json({ error: `حداکثر ${MAX_CHATS_PER_USER} گفتگو در حساب هر کاربر مجاز است.` });
            }

            await supaFetch('chats', {
                method: 'POST',
                headers: { 'Prefer': 'resolution=merge-duplicates' },
                body: JSON.stringify([{
                    owner_email: ownerEmail,
                    chat_id: String(chatId),
                    title: String(title || 'گفتگوی جدید').slice(0, 200),
                    html: typeof html === 'string' ? html : '',
                    pinned: !!pinned,
                    updated_at: Number(updatedAt) || Date.now()
                }])
            });

            if (history !== undefined) {
                await supaFetch('chat_history', {
                    method: 'POST',
                    headers: { 'Prefer': 'resolution=merge-duplicates' },
                    body: JSON.stringify([{
                        owner_email: ownerEmail,
                        chat_id: String(chatId),
                        history: history
                    }])
                });
            }

            if (files !== undefined) {
                await supaFetch('chat_files', {
                    method: 'POST',
                    headers: { 'Prefer': 'resolution=merge-duplicates' },
                    body: JSON.stringify([{
                        owner_email: ownerEmail,
                        chat_id: String(chatId),
                        files: files
                    }])
                });
            }

            return res.status(200).json({ ok: true });
        }

        // ===== DELETE: حذف یک چت یا چند چت باهم =====
        if (req.method === 'DELETE') {
            const chatId = req.query?.chatId;
            const idsParam = req.query?.ids;
            const idsToDelete = idsParam
                ? String(idsParam).split(',').map(s => s.trim()).filter(Boolean)
                : (chatId ? [String(chatId)] : []);

            if (!idsToDelete.length) {
                return res.status(400).json({ error: 'chatId یا ids مشخص نشده.' });
            }

            const idsFilter = idsToDelete.map(id => encodeURIComponent(id)).join(',');
            await Promise.all([
                supaFetch(`chats?owner_email=eq.${encodeURIComponent(ownerEmail)}&chat_id=in.(${idsFilter})`, { method: 'DELETE' }),
                supaFetch(`chat_history?owner_email=eq.${encodeURIComponent(ownerEmail)}&chat_id=in.(${idsFilter})`, { method: 'DELETE' }),
                supaFetch(`chat_files?owner_email=eq.${encodeURIComponent(ownerEmail)}&chat_id=in.(${idsFilter})`, { method: 'DELETE' })
            ]);

            return res.status(200).json({ ok: true, deleted: idsToDelete.length });
        }

        // ===== POST: آپلود یک فایل ضمیمه به Supabase Storage =====
        if (req.method === 'POST') {
            const action = req.query?.action;
            if (action !== 'upload') {
                return res.status(400).json({ error: 'اکشن نامعتبر است.' });
            }
            const chatId = req.query?.chatId;
            const fileName = req.query?.name;
            if (!chatId || !fileName) {
                return res.status(400).json({ error: 'chatId یا name مشخص نشده.' });
            }
            const { base64, contentType } = req.body || {};
            if (!base64 || typeof base64 !== 'string') {
                return res.status(400).json({ error: 'محتوای فایل (base64) خالی است.' });
            }
            const buffer = Buffer.from(base64.split(',').pop(), 'base64');
            if (buffer.length > MAX_UPLOAD_SIZE) {
                return res.status(413).json({ error: `حجم فایل بیشتر از ${MAX_UPLOAD_SIZE / (1024 * 1024)} مگابایت مجاز است.` });
            }
            const objectPath = storageObjectPath(ownerEmail, chatId, fileName);
            const uploadResp = await uploadToStorage(objectPath, buffer, contentType);
            if (!uploadResp.ok) {
                const errText = await uploadResp.text().catch(() => '');
                console.error('uploadToStorage failed:', errText);
                return res.status(502).json({ error: 'آپلود فایل روی Supabase Storage ناموفق بود.' });
            }
            return res.status(200).json({ ok: true, path: objectPath });
        }

        return res.status(405).json({ error: 'متد پشتیبانی نمی‌شود.' });
    } catch (err) {
        console.error('chats handler error:', err?.message || err);
        return res.status(500).json({ error: 'خطای داخلی سرور.', detail: err?.message || String(err) });
    }
};
