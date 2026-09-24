package com.virtualchat.app

import android.net.Uri
import android.widget.Toast
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.outlined.ContentCopy
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.draw.drawBehind
import androidx.compose.ui.draw.shadow
import androidx.compose.ui.layout.ContentScale
import coil.compose.AsyncImage
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalClipboardManager
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.AnnotatedString
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

/**
 * FEATURE: چت مشترک - صفحه‌ی گفتگوی زنده که دو (یا بیشتر) کاربر توی یک
 * چت مشترک با ربات صحبت می‌کنند. چون سرور (api/shared-chats.js) بر مبنای
 * polling طراحی شده (نه WebSocket)، این صفحه هر ۲ ثانیه یک‌بار پیام‌های
 * جدید را از SharedChatApiClient.pollMessages می‌گیرد.
 *
 * پیام‌های "user" با ایمیل فرستنده نمایش داده می‌شوند (سرور فقط ایمیل
 * دارد، نه اسم نمایشی - نگاه کن به SharedChatMessage.senderEmail) تا
 * مشخص باشد کدام پیام مال کدام نفر است - نکته‌ای که این چت را از
 * ChatScreen معمولی متمایز می‌کند.
 *
 * FIX (دو باگ گزارش‌شده):
 * ۱) پیام کاربر دیر می‌آمد: قبلاً sendCurrentInput فقط sendMessage را
 *    صدا می‌زد و نتیجه‌اش را کلاً دور می‌ریخت - پیام تا poll بعدی (حداکثر
 *    ۲ ثانیه) اصلاً روی صفحه نبود. حالا فوری (optimistic) با یک id موقت
 *    منفی به لیست اضافه می‌شود، و وقتی sendMessage جواب واقعی (با id
 *    واقعی سرور) برگرداند، جایگزینش می‌شود.
 * ۲) جواب ربات نمی‌آمد: SharedChatApiClient.sendMessage قبلاً پاسخ سرور
 *    را با کلیدهای نادرست parse می‌کرد (نگاه کن به توضیح کامل در همان
 *    فایل) - جواب ربات هیچ‌وقت پیدا نمی‌شد، نه در همین درخواست نه بعداً
 *    (چون polling هم چیزی متفاوت دریافت نمی‌کرد؛ خودِ ذخیره‌سازی سمت سرور
 *    درست بود، فقط parse سمت اپ اشتباه بود). حالا botMessage از
 *    SendMessageResult مستقیم استفاده می‌شود، و اگه سرور بگوید
 *    botPending=true (یکی دیگر مشغول است)، isWaitingForReply روشن
 *    می‌ماند تا polling جواب را برساند.
 */
private const val POLL_INTERVAL_MS = 2000L

// FIX: اگه جواب ربات بعد از این مدت نیومد، «در حال تایپ» خودکار خاموش
// می‌شه (قبلاً روی خطای سرور تا ابد می‌ماند).
private const val TYPING_TIMEOUT_MS = 45_000L

/**
 * FIX (کرش «Key "10" was already used»): پیام ربات هم از پاسخ خودِ
 * sendMessage می‌رسید و هم از polling (با فاصله‌ی چند میلی‌ثانیه) و هر دو
 * به لیست اضافه‌اش می‌کردند؛ دو آیتم با key یکسان توی LazyColumn = کرش.
 * این تابع پیام‌های جدید را ادغام می‌کند و هر id واقعی (مثبت) را فقط یک‌بار
 * نگه می‌دارد. پیام‌های optimistic (id منفی) که نسخه‌ی واقعی‌شان رسیده هم
 * حذف می‌شوند.
 */
private fun mergeMessages(
    current: List<SharedChatMessage>,
    incoming: List<SharedChatMessage>
): List<SharedChatMessage> {
    if (incoming.isEmpty()) return current
    val existingIds = current.filter { it.id > 0 }.map { it.id }.toHashSet()
    val fresh = incoming.filter { it.id > 0 && it.id !in existingIds }.distinctBy { it.id }
    if (fresh.isEmpty()) return current
    // پیام‌های optimistic با همان متن (یا فقط-عکس: متن خالی + داشتن عکس) که
    // نسخه‌ی واقعی‌شان رسیده حذف می‌شوند.
    val realUserKeys = fresh.filter { it.role == "user" }
        .map { it.text to it.attachments.isNotEmpty() }.toSet()
    return current.filter {
        !(it.id < 0 && (it.text to it.localImageUris.isNotEmpty()) in realUserKeys)
    } + fresh
}

/**
 * FEATURE (هم‌ظاهر‌سازی با چت عادی - طبق درخواست کاربر): پس‌زمینه، هدر،
 * نوار ورودی، «در حال تایپ» و حباب‌ها حالا از همان کامپوننت‌های
 * ChatScreen استفاده می‌کنند (ChatHeaderBar / ChatComposerBar /
 * TypingIndicatorDots / MessageBlockView) و فقط منطق مخصوص چت مشترک
 * (polling، ایمیل فرستنده، کد دعوت، ادغام پیام‌ها) اینجا مانده.
 *
 * تصمیم‌ها (طبق پاسخ کاربر): دکمه‌ی تفکر/ویدیو/فایل/ویس نیست؛ فقط
 * «افزودن عکس» و چیپ مدل.
 *
 * مرحله‌ی ۲ (فعال): عکس و مدل حالا واقعاً به سرور می‌روند.
 * - عکس: هر عکس با همان فشرده‌ساز چت عادی (uriToAttachedImage) فشرده و
 *   جدا آپلود می‌شود (SharedChatApiClient.uploadImage)، سپس مسیرها
 *   همراه sendMessage می‌روند. عکس‌ها فقط بعد از دانلود (downloadImage)
 *   برای همه‌ی اعضا نمایش داده می‌شوند.
 * - مدل: برای کل چت ثابت است و سازنده موقع ساخت انتخاب می‌کند
 *   (SharedChatListDialog). این‌جا چیپ مدل فقط نمایشی/قفل است - همه‌ی
 *   اعضا همان مدل چت را می‌بینند و کسی نمی‌تواند وسط چت عوضش کند.
 *
 تصمیم چیدمان: چون چت چندنفره است، پیام «خودم» سمت راست (مثل چت
 * عادی) و پیام دیگران سمت چپ با برچسب اسم؛ ربات بدون حباب (مثل چت
 * عادی) ولی هم‌عرض با محتوای خودش.
 */
@OptIn(ExperimentalFoundationApi::class)
@Composable
fun SharedChatScreen(
    chatId: String,
    title: String,
    inviteCode: String?,
    // مدل ثابت این چت (از سرور). null = سرور قدیمی/ناشناخته → پیش‌فرض اپ.
    chatModel: String? = null,
    onBack: () -> Unit
) {
    val context = LocalContext.current
    val clipboard = LocalClipboardManager.current
    val coroutineScope = rememberCoroutineScope()
    val listState = rememberLazyListState()
    val myEmail = remember { SessionPrefs.getEmail(context) }

    var messages by remember { mutableStateOf(listOf<SharedChatMessage>()) }
    var lastId by remember { mutableStateOf(0L) }
    var inputText by remember { mutableStateOf("") }
    var isSending by remember { mutableStateOf(false) }
    var isWaitingForReply by remember { mutableStateOf(false) }
    // شمارنده‌ی id موقت برای پیام‌های optimistic (منفی، تا هیچ‌وقت با id
    // واقعی سرور - که همیشه مثبت است - تداخل نکند).
    var nextTempId by remember { mutableStateOf(-1L) }

    // --- وضعیت UI مخصوص نوار ورودی مشترک با چت عادی ---
    val pendingImageUris = remember { mutableStateListOf<Uri>() }
    // مدل چت ثابت است (تعیین‌شده توسط سازنده)؛ فقط برای نمایش برچسب.
    val fixedModelId = remember(chatModel) { chatModel ?: DEFAULT_SHARED_MODEL_ID }
    val fixedModelLabel = remember(fixedModelId) { sharedModelLabel(fixedModelId) }

    val imagePickerLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.GetMultipleContents()
    ) { uris: List<Uri> ->
        if (uris.isEmpty()) return@rememberLauncherForActivityResult
        val remaining = MAX_PENDING_IMAGES - pendingImageUris.size
        if (remaining <= 0) {
            Toast.makeText(context, "حداکثر $MAX_PENDING_IMAGES عکس در هر پیام", Toast.LENGTH_SHORT).show()
            return@rememberLauncherForActivityResult
        }
        for (uri in uris.take(remaining)) {
            coroutineScope.launch {
                val local = MediaPersist.persist(context, uri, "jpg")
                if (local == null) {
                    Toast.makeText(context, "خواندن این عکس ممکن نشد.", Toast.LENGTH_SHORT).show()
                } else if (pendingImageUris.size < MAX_PENDING_IMAGES) {
                    pendingImageUris.add(local)
                } else {
                    MediaPersist.discard(local)
                }
            }
        }
    }

    fun scrollToBottom() {
        coroutineScope.launch {
            // +1 چون ممکن است آیتم «در حال تایپ» هم آخر لیست باشد.
            val count = messages.size + if (isWaitingForReply) 1 else 0
            if (count > 0) listState.animateScrollToItem(count - 1)
        }
    }

    // Polling loop: هر ۲ ثانیه پیام‌های جدید را می‌گیرد. وقتی پیام جدیدی
    // با role="model" برسد، isWaitingForReply خودکار خاموش می‌شود.
    LaunchedEffect(chatId) {
        while (true) {
            val newOnes = SharedChatApiClient.pollMessages(context, chatId, lastId)
            if (newOnes.isNotEmpty()) {
                messages = mergeMessages(messages, newOnes)
                lastId = maxOf(lastId, newOnes.maxOf { it.id })
                if (newOnes.any { it.role == "model" }) {
                    isWaitingForReply = false
                }
                scrollToBottom()
            }
            delay(POLL_INTERVAL_MS)
        }
    }

    fun sendCurrentInput() {
        val text = inputText.trim()
        val imageUris = pendingImageUris.toList()
        // پیام فقط-عکس (بدون متن) هم مجاز است (سرور هم همین را می‌پذیرد).
        if ((text.isEmpty() && imageUris.isEmpty()) || isSending) return

        inputText = ""
        pendingImageUris.clear()
        isSending = true
        isWaitingForReply = true

        // فوری و optimistic پیام کاربر را نشان می‌دهیم (با عکس‌های محلی).
        val tempId = nextTempId
        nextTempId -= 1
        messages = messages + SharedChatMessage(
            id = tempId, role = "user", text = text, senderEmail = myEmail,
            localImageUris = imageUris.map { it.toString() }
        )
        scrollToBottom()

        coroutineScope.launch {
            // ۱) آپلود عکس‌ها (به‌ترتیب). اگر یکی شکست خورد، کل ارسال لغو می‌شود
            // و متن/عکس‌ها به کاربر برمی‌گردند - نه اینکه پیام بدون عکس برود
            // و کاربر فکر کند عکس هم رفته.
            val uploaded = mutableListOf<UploadedImage>()
            for (uri in imageUris) {
                val attached = uriToAttachedImage(context, uri)
                val up = attached?.let {
                    SharedChatApiClient.uploadImage(context, chatId, it.base64Data, it.mimeType, it.fileName)
                }
                if (up == null) {
                    val why = if (attached == null) "خواندن عکس ممکن نشد." else (SharedChatApiClient.lastError ?: "آپلود عکس ناموفق بود.")
                    Toast.makeText(context, why, Toast.LENGTH_LONG).show()
                    // برگرداندن وضعیت: پیام optimistic حذف، ورودی و عکس‌ها بازیابی
                    messages = messages.filter { it.id != tempId }
                    inputText = text
                    imageUris.forEach { u -> if (u !in pendingImageUris) pendingImageUris.add(u) }
                    isSending = false
                    isWaitingForReply = false
                    return@launch
                }
                uploaded.add(up)
            }

            // ۲) ارسال پیام (+مسیر عکس‌ها)
            val result = SharedChatApiClient.sendMessage(context, chatId, text, uploaded)
            isSending = false

            if (result.userMessage != null) {
                messages = messages.filter { it.id != tempId }
                messages = mergeMessages(messages, listOf(result.userMessage))
                lastId = maxOf(lastId, result.userMessage.id)
                // فایل‌های محلی دیگر لازم نیستند (عکس واقعی حالا روی سرور است)
                imageUris.forEach { MediaPersist.discard(it) }
            } else {
                // سرور پیام را نگرفت (مثلاً خطای ذخیره‌ی ضمیمه): optimistic را
                // برمی‌داریم و محتوا را به کاربر برمی‌گردانیم تا دوباره بزند.
                messages = messages.filter { it.id != tempId }
                inputText = text
                imageUris.forEach { u -> if (u !in pendingImageUris) pendingImageUris.add(u) }
            }

            if (result.botMessage != null) {
                messages = mergeMessages(messages, listOf(result.botMessage))
                lastId = maxOf(lastId, result.botMessage.id)
                isWaitingForReply = false
                scrollToBottom()
            } else if (!result.botPending) {
                isWaitingForReply = false
                val msg = result.errorMessage ?: "ربات پاسخ نداد. دوباره تلاش کن."
                Toast.makeText(context, msg, Toast.LENGTH_LONG).show()
            }
        }
    }

    // تایم‌اوت ایمنی برای «در حال تایپ».
    LaunchedEffect(isWaitingForReply) {
        if (isWaitingForReply) {
            delay(TYPING_TIMEOUT_MS)
            isWaitingForReply = false
        }
    }

    // همان پس‌زمینه‌ی ChatScreen (گرادینت تیره / رنگ ساده در تم روشن).
    Column(
        modifier = Modifier
            .fillMaxSize()
            .drawBehind {
                if (!ThemeState.isLight) {
                    drawRect(
                        brush = Brush.verticalGradient(
                            colors = listOf(
                                Color(0xFF333333),
                                Color(0xFF262626),
                                Color(0xFF1E1E1E),
                                Color(0xFF151515)
                            )
                        )
                    )
                } else {
                    drawRect(color = BgMain)
                }
            }
            .statusBarsPadding()
    ) {
        ChatHeaderBar(
            onOpenMenu = onBack,
            onOpenSettings = {},
            showChatSearch = false,
            onToggleChatSearch = {},
            showNotifPanel = false,
            notifLoading = false,
            notifItems = emptyList(),
            onToggleNotifPanel = {},
            onDismissNotifPanel = {},
            menuIcon = Icons.AutoMirrored.Outlined.ArrowBack,
            centerTitle = title,
            trailingContent = if (!inviteCode.isNullOrBlank()) {
                {
                    Row(
                        verticalAlignment = Alignment.CenterVertically,
                        modifier = Modifier.clickable {
                            clipboard.setText(AnnotatedString(inviteCode))
                            Toast.makeText(context, "کد دعوت کپی شد", Toast.LENGTH_SHORT).show()
                        }
                    ) {
                        Text("کد: $inviteCode", color = TextMain, fontSize = 13.sp, fontWeight = FontWeight.Medium)
                        Spacer(modifier = Modifier.width(6.dp))
                        Icon(Icons.Outlined.ContentCopy, contentDescription = "کپی کد دعوت", tint = TextMuted, modifier = Modifier.size(15.dp))
                    }
                }
            } else {
                { Spacer(modifier = Modifier.size(width = 1.dp, height = 26.dp)) }
            }
        )

        LazyColumn(
            state = listState,
            modifier = Modifier
                .weight(1f)
                .fillMaxWidth()
                .padding(horizontal = 16.dp),
            contentPadding = PaddingValues(vertical = 8.dp)
        ) {
            items(messages, key = { it.id }) { msg ->
                SharedChatBubble(msg, myEmail = myEmail, chatId = chatId)
            }
            if (isWaitingForReply) {
                item(key = "typing-indicator") {
                    // همان نقطه‌های پرش‌کننده‌ی چت عادی (سمت چپ چون
                    // ربات در چیدمان مشترک سمت چپ است).
                    Row(
                        modifier = Modifier.fillMaxWidth().padding(vertical = 10.dp, horizontal = 4.dp),
                        horizontalArrangement = Arrangement.End
                    ) {
                        TypingIndicatorDots(color = TextMain)
                    }
                }
            }
        }

        ChatComposerBar(
            isHomeState = false,
            inputText = inputText,
            onInputTextChange = { inputText = it },
            pendingImageUris = pendingImageUris,
            onRemoveImage = { uri -> pendingImageUris.remove(uri); MediaPersist.discard(uri) },
            pendingVideoUri = null,
            pendingVideoThumbnail = null,
            onRemoveVideo = {},
            pendingTextFiles = emptyList(),
            onRemoveTextFile = {},
            onPickImage = { imagePickerLauncher.launch("image/*") },
            onPickVideo = {},
            onPickFile = {},
            onAttachMenuOpened = {},
            recentDeviceImages = emptyList(),
            onPickRecentImage = {},
            selectedThinkLevel = "off",
            onThinkLevelChange = {},
            // مدل قفل است: کسی وسط چت نمی‌تواند عوضش کند؛ onModelChange
            // عمداً خالی است (تغییر در چت مشترک فقط موقع ساخت ممکن است).
            selectedModelLabel = fixedModelLabel,
            selectedModelId = fixedModelId,
            onModelChange = { _, _ ->
                Toast.makeText(context, "مدل این چت ثابت است و سازنده‌ی چت انتخابش کرده.", Toast.LENGTH_SHORT).show()
            },
            isGenerating = false,
            isPreparingImage = false,
            isPreparingVideo = false,
            isPreparingFile = false,
            mediaProgress = null,
            onPrimaryAction = { sendCurrentInput() },
            placeholderText = "پیام بنویس...",
            showThinkChip = false,
            showVideoOption = false,
            showFileOption = false,
            allowVoiceWhenEmpty = false
        )
    }
}

@OptIn(ExperimentalFoundationApi::class)
@Composable
private fun SharedChatBubble(msg: SharedChatMessage, myEmail: String?, chatId: String) {
    val isModel = msg.role == "model"
    val isMine = !isModel && myEmail != null && msg.senderEmail.equals(myEmail, ignoreCase = true)
    // نام فرستنده فقط برای پیام «دیگران» (نه خودم، نه ربات) - همین است که
    // چت مشترک را از چت تک‌نفره متمایز می‌کند.
    val otherName = if (!isModel && !isMine) msg.senderEmail?.substringBefore("@") else null

    val clipboard = LocalClipboardManager.current
    val context = LocalContext.current
    var showActions by remember { mutableStateOf(false) }

    // چیدمان: خودم و ربات هم‌جهت با چت عادی (اپ RTL: Start=راست)؛
    // در چت عادی کاربر Start و بات End است. اینجا «خودم» = Start
    // (همان راست) و دیگران/ربات = End (چپ) تا فرق بین آدم‌ها روشن باشد.
    val userShape = RoundedCornerShape(topStart = 20.dp, topEnd = 20.dp, bottomEnd = 20.dp, bottomStart = 6.dp)
    Row(
        modifier = Modifier.fillMaxWidth().padding(vertical = 6.dp),
        horizontalArrangement = if (isMine) Arrangement.Start else Arrangement.End
    ) {
        if (isModel) {
            Column(modifier = Modifier.fillMaxWidth(0.85f).widthIn(max = 480.dp).padding(horizontal = 4.dp, vertical = 2.dp)) {
                val blocks = remember(msg.text) { MessageFormatter.parse(msg.text) }
                Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    blocks.forEach { block ->
                        MessageBlockView(block = block, onDownloadCode = { _, _ -> }, onFeatureChipClick = { })
                    }
                }
                // دکمه‌ی کپی مثل چت عادی (فقط کپی؛ دوباره‌تولید در چت
                // مشترک معنی ندارد چون پیام روی سرور ذخیره می‌شود).
                Row(modifier = Modifier.padding(top = 4.dp)) {
                    IconButton(
                        onClick = {
                            clipboard.setText(AnnotatedString(msg.text))
                            Toast.makeText(context, "کپی شد", Toast.LENGTH_SHORT).show()
                        },
                        modifier = Modifier.size(30.dp)
                    ) {
                        Icon(Icons.Outlined.ContentCopy, contentDescription = "کپی", tint = TextMuted, modifier = Modifier.size(17.dp))
                    }
                }
            }
        } else {
            Column(horizontalAlignment = if (isMine) Alignment.Start else Alignment.End) {
                if (!otherName.isNullOrBlank()) {
                    Text(
                        otherName,
                        color = TextMuted,
                        fontSize = 11.sp,
                        modifier = Modifier.padding(bottom = 3.dp, start = 6.dp, end = 6.dp)
                    )
                }
                Box(
                    modifier = Modifier
                        .widthIn(max = 320.dp)
                        .shadow(elevation = 4.dp, shape = userShape, clip = false, ambientColor = Color.Black, spotColor = Color.Black)
                        .background(Brush.verticalGradient(listOf(BgInputFocused, BgCard)), shape = userShape)
                        .combinedClickable(onClick = {}, onLongClick = { showActions = !showActions })
                        .padding(horizontal = 16.dp, vertical = 12.dp)
                ) {
                    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        if (msg.attachments.isNotEmpty() || msg.localImageUris.isNotEmpty()) {
                            SharedChatImages(msg, chatId)
                        }
                        if (msg.text.isNotBlank()) {
                            Text(msg.text, color = TextMain, fontSize = 16.sp, fontWeight = FontWeight.Medium)
                        }
                    }
                }
                if (showActions && msg.text.isNotBlank()) {
                    IconButton(
                        onClick = {
                            clipboard.setText(AnnotatedString(msg.text))
                            Toast.makeText(context, "کپی شد", Toast.LENGTH_SHORT).show()
                            showActions = false
                        },
                        modifier = Modifier.size(30.dp)
                    ) {
                        Icon(Icons.Outlined.ContentCopy, contentDescription = "کپی", tint = TextMuted, modifier = Modifier.size(17.dp))
                    }
                }
            }
        }
    }
}


/** مدل پیش‌فرض اپ (همان چیپ MainActivity) وقتی سرور مدل چت را نفرستاده. */
internal const val DEFAULT_SHARED_MODEL_ID = "gemini-3.8-flash"

/**
 * مدل‌هایی که سازنده موقع ساخت چت مشترک می‌تواند انتخاب کند. عمداً همان سه
 * مدل چیپ MainActivity (modelOptions) - اگر آنجا مدلی عوض شد، این‌جا هم عوض کن.
 */
internal val SHARED_CHAT_MODEL_OPTIONS = listOf(
    Triple("Virtual Bot 1.1", "gemini-3.5-flash-lite", "سریع‌ترین پاسخ‌ها"),
    Triple("Virtual Bot 1.7", "gemini-3.8-flash", "جدیدترین مدل"),
    Triple("Virtual Bot 1.3", "gemini-3.1-pro-preview", "مناسب کدنویسی")
)

/** برچسب خوانا برای id مدل؛ مدل ناشناخته (مثلاً چت قدیمی) خودِ id را نشان می‌دهد. */
internal fun sharedModelLabel(modelId: String): String =
    SHARED_CHAT_MODEL_OPTIONS.firstOrNull { it.second == modelId }?.first ?: modelId

/** حالت بارگذاری یک عکس دانلودی. */
private sealed interface ImageLoadState {
    object Loading : ImageLoadState
    object Failed : ImageLoadState
    class Ready(val bytes: ByteArray) : ImageLoadState
}

/**
 * عکس‌های یک پیام. برای پیام‌های optimistic از فایل محلی نشان می‌دهد (فوری،
 * بدون دانلود)؛ برای پیام‌های واقعی (خودم یا دیگران) از سرور می‌گیرد.
 */
@Composable
private fun SharedChatImages(msg: SharedChatMessage, chatId: String) {
    val thumbShape = RoundedCornerShape(12.dp)

    // شبکه‌ی ساده‌ی ۲ستونه (بدون FlowRow که هنوز @ExperimentalLayoutApi است).
    // به‌جای لیستی از lambdaهای @Composable (که inference آن شکننده است)، هر
    // عکس را با یک «مدل داده‌ی ساده» توصیف می‌کنیم و ردیف‌ها را مستقیم می‌سازیم.
    // چون سقف ۴ عکس در هر پیام است، حداکثر ۲ ردیف می‌شود.
    val remote = msg.attachments
    val local = if (remote.isEmpty()) msg.localImageUris else emptyList()
    val total = remote.size + local.size
    Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
        var index = 0
        while (index < total) {
            Row(horizontalArrangement = Arrangement.spacedBy(6.dp)) {
                for (slot in index until minOf(index + 2, total)) {
                    if (slot < remote.size) {
                        SharedChatRemoteImage(remote[slot], chatId, thumbShape)
                    } else {
                        AsyncImage(
                            model = Uri.parse(local[slot - remote.size]),
                            contentDescription = "عکس ارسالی",
                            contentScale = ContentScale.Crop,
                            modifier = Modifier.size(140.dp).clip(thumbShape).background(BgCard)
                        )
                    }
                }
            }
            index += 2
        }
    }
}

/** یک عکس دانلودی: loading → عکس / خطا. با cache تا با هر recomposition دوباره دانلود نشود. */
@Composable
private fun SharedChatRemoteImage(att: SharedChatAttachment, chatId: String, shape: RoundedCornerShape) {
    val context = LocalContext.current
    var state by remember(att.path) { mutableStateOf<ImageLoadState>(ImageLoadState.Loading) }
    LaunchedEffect(att.path) {
        val cached = SharedImageCache.get(att.path)
        if (cached != null) {
            state = ImageLoadState.Ready(cached)
            return@LaunchedEffect
        }
        val img = SharedChatApiClient.downloadImage(context, chatId, att.path)
        if (img != null) {
            SharedImageCache.put(att.path, img.bytes)
            state = ImageLoadState.Ready(img.bytes)
        } else {
            state = ImageLoadState.Failed
        }
    }
    Box(
        modifier = Modifier.size(140.dp).clip(shape).background(BgCard),
        contentAlignment = Alignment.Center
    ) {
        when (val st = state) {
            is ImageLoadState.Ready -> AsyncImage(
                model = st.bytes,
                contentDescription = att.name ?: "عکس",
                contentScale = ContentScale.Crop,
                modifier = Modifier.fillMaxSize()
            )
            ImageLoadState.Loading -> CircularProgressIndicator(
                modifier = Modifier.size(22.dp), strokeWidth = 2.dp, color = TextMuted
            )
            ImageLoadState.Failed -> Text("⚠ عکس بارگذاری نشد", color = TextMuted, fontSize = 11.sp)
        }
    }
}

/**
 * cache حافظه‌ای ساده و سقف‌دار برای عکس‌های دانلودشده (LRU بر اساس مجموع
 * بایت). بدون آن، هر بار که LazyColumn آیتم را دوباره compose می‌کند عکس
 * دوباره از سرور می‌آمد.
 */
private object SharedImageCache {
    private const val MAX_BYTES = 24 * 1024 * 1024 // ۲۴MB
    private val map = object : LinkedHashMap<String, ByteArray>(16, 0.75f, true) {}
    private var total = 0

    @Synchronized fun get(key: String): ByteArray? = map[key]

    @Synchronized fun put(key: String, value: ByteArray) {
        map.remove(key)?.let { total -= it.size }
        map[key] = value
        total += value.size
        val it = map.entries.iterator()
        while (total > MAX_BYTES && it.hasNext()) {
            val e = it.next()
            if (e.key == key) continue
            total -= e.value.size
            it.remove()
        }
    }
}
