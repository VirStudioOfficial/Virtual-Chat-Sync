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
