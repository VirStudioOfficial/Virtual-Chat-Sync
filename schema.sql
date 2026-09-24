-- schema.sql برای پروژه‌ی virtual-chat-sync
-- این پروژه جایگزین کامل سیستم قبلی (که به Google OAuth وابسته بود) است:
-- کاربر با ایمیل/پسورد خودش (نه گوگل) ثبت‌نام و لاگین می‌کند.

-- جدول کاربران: پسورد هرگز خام ذخیره نمی‌شود، فقط هش bcrypt آن.
create table if not exists users (
    email text primary key,
    password_hash text not null,
    created_at bigint not null
);

-- جدول session: بعد از لاگین موفق، یک توکن تصادفی امن اینجا ذخیره می‌شود.
-- کلاینت این توکن را (نه ایمیل/پسورد را) در هر درخواست بعدی می‌فرستد.
create table if not exists sessions (
    token text primary key,
    email text not null references users(email) on delete cascade,
    created_at bigint not null,
    expires_at bigint not null
);

-- جدول‌های چت - دقیقاً همان ساختاری که در نسخه‌ی قبلی (وابسته به گوگل)
-- استفاده می‌شد، فقط owner_email حالا به ایمیل داخلی (جدول users) اشاره
-- می‌کند نه به ایمیل گوگل.
create table if not exists chats (
    owner_email text not null,
    chat_id text not null,
    title text not null default 'گفتگوی جدید',
    html text not null default '',
    pinned boolean not null default false,
    updated_at bigint not null,
    primary key (owner_email, chat_id)
);

create table if not exists chat_history (
    owner_email text not null,
    chat_id text not null,
    history jsonb,
    primary key (owner_email, chat_id)
);

create table if not exists chat_files (
    owner_email text not null,
    chat_id text not null,
    files jsonb,
    primary key (owner_email, chat_id)
);

-- کدهای تایید ایمیل (هم برای ثبت‌نام هم برای ورود از دستگاه جدید).
-- کد به‌صورت هش‌شده ذخیره می‌شود، نه خام. برای register، پسورد هم به‌صورت
-- هش‌شده موقتاً اینجا نگه داشته می‌شود تا کاربر واقعی فقط بعد از تایید
-- کد در جدول users ساخته شود.
create table if not exists pending_verifications (
    id bigserial primary key,
    email text not null,
    code_hash text not null,
    purpose text not null check (purpose in ('register', 'login')),
    password_hash text,
    device_id text,
    attempts int not null default 0,
    created_at bigint not null,
    expires_at bigint not null
);

create index if not exists idx_pending_verifications_email
    on pending_verifications(email, purpose);

-- دستگاه‌هایی که قبلاً برای یک ایمیل تایید شده‌اند، تا هر بار لاگین از همان
-- دستگاه دوباره کد نخواهیم.
create table if not exists known_devices (
    email text not null references users(email) on delete cascade,
    device_id text not null,
    created_at bigint not null,
    primary key (email, device_id)
);

-- FEATURE (dual-response A/B learning - مرحله ۴): هر بار کاربر بین دو
-- پاسخ (الف/ب) یکی را انتخاب می‌کند، همین‌جا ذخیره می‌شود. بعداً
-- api/preferences.js?action=analyze این جدول را می‌خواند و یک خلاصه‌ی
-- کوتاه از الگوی ترجیح کاربر می‌سازد که به system prompt تزریق می‌شود
-- (مرحله ۵) تا سبک کلی پاسخ‌ها با گذر زمان با سلیقه‌ی کاربر همسو شود.
create table if not exists response_preferences (
    id bigserial primary key,
    owner_email text not null,
    chat_id text not null,
    user_message text not null,
    response_a text not null,
    response_b text not null,
    chosen text not null check (chosen in ('a', 'b')),
    response_a_meta jsonb,
    response_b_meta jsonb,
    created_at bigint not null
);

create index if not exists idx_response_preferences_owner
    on response_preferences(owner_email, created_at desc);

-- FEATURE (حافظه‌ی بلندمدت کاربر): هر بار کاربر یک اطلاعات شخصی/دائمی
-- (اسم، مدل کارت گرافیک، رنگ مورد علاقه، یا هر چیز دیگری که مدل مهم
-- تشخیص دهد) می‌گوید، خودِ مدل در همان پاسخ یک بلاک widget-memory-save
-- می‌سازد و کلاینت همان لحظه اینجا upsert می‌کند (نه با تأخیر/دوره‌ای).
-- key آزاد است (نه از پیش تعریف‌شده) تا مدل بتواند هر برچسبی که مناسب
-- می‌داند بسازد؛ (owner_email, key) کلید اصلی است تا مقدار جدید همیشه
-- جایگزین مقدار قدیمی همان کلید شود، نه اینکه ردیف‌های تکراری بسازد.
create table if not exists user_memory (
    owner_email text not null,
    key text not null,
    value text not null,
    updated_at bigint not null,
    primary key (owner_email, key)
);

create index if not exists idx_user_memory_owner
    on user_memory(owner_email);

-- ============================================================
-- FEATURE: چت مشترک (دو یا چند نفر همزمان با هم و با ربات صحبت
-- می‌کنند). این جدول‌ها را به schema.sql موجود اضافه کن (کپی/پیست
-- در انتهای فایل کافی است، به هیچ جدول قبلی دست نمی‌زند).
-- ============================================================

-- هر چت مشترک یک رکورد این‌جا دارد. owner_email همان کسی است که چت را
-- ساخته (سازنده)؛ invite_code کد کوتاهی است که برای دعوت نفر(های) بعدی
-- به اشتراک گذاشته می‌شود.
create table if not exists shared_chats (
    chat_id text primary key,
    owner_email text not null references users(email) on delete cascade,
    title text not null default 'گفتگوی مشترک',
    invite_code text not null unique,
    created_at bigint not null,
    updated_at bigint not null
);

create index if not exists idx_shared_chats_invite_code
    on shared_chats(invite_code);

-- عضوهای هر چت مشترک. سازنده هم موقع ساخت این‌جا اضافه می‌شود (پس این
-- جدول همیشه منبع کامل و قابل‌اعتماد "چه کسانی عضو این چت‌اند" است).
create table if not exists shared_chat_participants (
    chat_id text not null references shared_chats(chat_id) on delete cascade,
    email text not null references users(email) on delete cascade,
    joined_at bigint not null,
    primary key (chat_id, email)
);

create index if not exists idx_shared_chat_participants_email
    on shared_chat_participants(email);

-- خودِ پیام‌ها. برخلاف چت‌های شخصی (که history یک‌جا به‌صورت JSON در
-- chat_history ذخیره می‌شود)، این‌جا هر پیام یک ردیف جداست چون چند نفر
-- همزمان می‌نویسند و باید بشود «فقط پیام‌های جدیدتر از فلان id» را
-- گرفت (برای polling کلاینت).
create table if not exists shared_chat_messages (
    id bigserial primary key,
    chat_id text not null references shared_chats(chat_id) on delete cascade,
    role text not null check (role in ('user', 'model')),
    sender_email text, -- برای role='model' خالی می‌ماند
    text text not null,
    created_at bigint not null
);

create index if not exists idx_shared_chat_messages_chat_id
    on shared_chat_messages(chat_id, id);

-- قفل ساده برای جلوگیری از قاطی‌شدن دو پاسخ ربات هم‌زمان. وقتی پیامی
-- در حال پردازش (در انتظار جواب Gemini) است، یک ردیف این‌جا می‌سازیم؛
-- چون chat_id کلید اصلی است، تلاش دوم برای ساخت همزمان با خطای
-- unique violation شکست می‌خورد و می‌فهمیم یکی دیگر مشغول است.
create table if not exists shared_chat_locks (
    chat_id text primary key references shared_chats(chat_id) on delete cascade,
    locked_by text not null,
    locked_at bigint not null
);

-- ============================================================
-- FEATURE: عکس در چت مشترک + مدل ثابت برای هر چت مشترک
-- (این بخش را هم در انتهای schema.sql / SQL Editor اجرا کن؛ همه‌ی
-- دستورها idempotent هستند و به داده‌ی قبلی دست نمی‌زنند.)
-- ============================================================

-- مدل ثابت هر چت مشترک: سازنده موقع create انتخاب می‌کند و بعد از آن
-- برای همه‌ی پاسخ‌های ربات در همان چت استفاده می‌شود. چت‌های قدیمی
-- که قبل از این فیچر ساخته شده‌اند مقدار پیش‌فرض را می‌گیرند.
alter table shared_chats
    add column if not exists model text not null default 'gemini-3.6-flash';
-- (default ستون عمداً همان مقدار قدیمی می‌ماند: چت‌های موجود همان مدلی را نگه می‌دارند که تا حالا عملاً استفاده می‌شد. چت‌های جدید مدل را از اپ می‌گیرند.)

-- عکس‌های پیوست‌شده به پیام‌ها. عکس خودش در Supabase Storage
-- (باکت chat-attachments) ذخیره می‌شود و اینجا فقط مسیرش + نوع فایل
-- می‌ماند. به‌جای ستون جدا روی shared_chat_messages، یک جدول مستقل
-- می‌سازیم تا یک پیام بتواند چند عکس داشته باشد و polling سبک بماند.
create table if not exists shared_chat_attachments (
    id bigserial primary key,
    message_id bigint not null references shared_chat_messages(id) on delete cascade,
    chat_id text not null references shared_chats(chat_id) on delete cascade,
    storage_path text not null,
    content_type text not null,
    file_name text,
    size_bytes bigint not null,
    created_at bigint not null
);

create index if not exists idx_shared_chat_attachments_message
    on shared_chat_attachments(message_id);
create index if not exists idx_shared_chat_attachments_chat
    on shared_chat_attachments(chat_id);

-- پیام بدون متن (فقط عکس) هم مجاز است؛ قبلاً text باید not null غیرخالی
-- می‌بود ولی ستون خودش فقط not null است (رشته‌ی خالی مجاز است)، پس
-- نیازی به تغییر ندارد.
