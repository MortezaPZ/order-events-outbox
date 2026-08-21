# Order Events — سرویس سفارش رویدادمحور

*An event-driven order service that solves the three failure modes that break naive message-based systems: lost events, duplicate processing, and blocked queues — via the transactional outbox pattern, idempotent consumers, and bounded retry + dead-letter. See below for the Persian write-up.*

یک سرویس سفارش رویدادمحور که حول سه حالت خرابی ساخته شده که سیستم‌های
پیام‌محور ساده‌لوح را از کار می‌اندازد:

| خرابی | پاسخ این پروژه |
|---|---|
| commit دیتابیس موفق می‌شود ولی publish شکست می‌خورد → رویداد گم می‌شود | **الگوی Transactional Outbox** |
| پیام دوبار تحویل داده می‌شود → اثر جانبی دوبار اجرا می‌شود | **مصرف‌کننده‌های ایدمپوتنت** |
| پیام همیشه شکست می‌خورد → صف برای همیشه مسدود می‌شود | **تلاش مجدد محدود + dead-letter** |

**FastAPI · SQLite · بروکر قابل‌تعویض · Docker · GitHub Actions.** بدون
هیچ زیرساختی اجرا می‌شود: `python demo.py` و کار می‌کند.

---

## جریان کار

```
POST /orders ──┬──▶ orders table     ┐
               └──▶ outbox table     ┘  یک تراکنش واحد
                        │
                  relay (poller)  ──── بروکر خاموش؟ سطر در حالت pending می‌ماند
                        │
                        ▼
                  broker (memory | redis)
                        │
                  worker — claim(event_id)
                   │            │
              بار اول       تکراری → رد شود
                   │
              اجرای handler
                   │
            ┌──────┴──────┐
          موفق          خطا
                           │
                  attempt < max؟ ──بله──▶ صف مجدد با backoff
                           │
                          خیر ──▶ dead_letters (قابل بازپخش)
```

---

## پنج تضمین، به‌صورت عملی

`python demo.py` هرکدام را اجرا می‌کند و نتیجه را چاپ می‌کند:

```
1. Normal flow
  placed          order=1  outbox_pending=1
  relay published queue=1  outbox_pending=0
  worker done     status=reserved
  after payment   status=confirmed

2. Broker outage — the outbox holds the event until delivery works
  pass 1: BROKER DOWN  outbox_pending=1
  pass 2: BROKER DOWN  outbox_pending=1
  pass 3: delivered    outbox_pending=0
  nothing lost    status=reserved

3. Duplicate delivery — at-least-once, but the effect runs once
  delivery 1: processed=1 duplicates_skipped=0
  delivery 2: processed=1 duplicates_skipped=1
  delivery 3: processed=1 duplicates_skipped=2

4. Poison message — bounded retries, then dead-letter, then replay
  attempt 1: retried=1 dead_lettered=0
  attempt 2: retried=2 dead_lettered=0
  attempt 3: retried=2 dead_lettered=1
  after replay:   status=reserved  dead_letters=0

5. Bulk flow — 500 orders, nothing lost
  orders placed:  500
  events relayed: 500
  reserved:       500
  outbox left:    0
  dead letters:   0
```

سناریوی ۲ آن یکی است که ارزش دوبار خواندن دارد: بروکر برای دو دور خاموش
بود و رویداد بازهم تحویل داده شد، چون هیچ‌وقت فقط در صف نبود.

---

## شروع سریع

```bash
python -m venv .venv
.venv/Scripts/activate            # source .venv/bin/activate در Linux/macOS
pip install -r requirements.txt

python demo.py                              # پنج سناریوی بالا
pytest tests -q                             # ۵۳ آزمون
uvicorn orderflow.api:app --reload          # API روی http://localhost:8000
```

با Docker:

```bash
docker compose up --build           # API + Redis
BROKER=redis docker compose up      # استفاده از صف واقعی
```

---

## API

| متد | مسیر | هدف |
|---|---|---|
| `GET` | `/health` | تعداد سفارش‌ها، عمق outbox، عمق صف، dead letterها |
| `POST` | `/orders` | ثبت سفارش — سفارش و رویداد را اتمیک می‌نویسد |
| `GET` | `/orders` · `/orders/{id}` | خواندن سفارش‌ها |
| `POST` | `/orders/{id}/pay` | صدور رویداد `order.paid` |
| `POST` | `/orders/{id}/cancel` | صدور رویداد `order.cancelled` |
| `GET` | `/dead-letters` | بررسی این‌که چه چیزی و چرا شکست خورده |
| `POST` | `/dead-letters/{id}/replay` | صف مجدد بعد از رفع علت |

```bash
curl -X POST http://localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customer":"Acme","amount":249.99}'

curl http://localhost:8000/health
```

`/health` عمداً `outbox_pending` و `queue_depth` را جدا نشان می‌دهد: رشد
outbox یعنی relay گیر کرده، رشد صف یعنی workerها عقب افتاده‌اند — و این‌ها
دو حادثه‌ی متفاوت‌اند.

---

## تصمیم‌های طراحی که ارزش توضیح دارند

**چرا outbox به‌جای publish داخل خود درخواست؟** چون «بنویس در دیتابیس، بعد
publish کن» یک پنجره‌ی خطر دارد: اگر پردازش بین این دو مرحله بمیرد، سفارش
وجود دارد ولی هیچ‌کس از آن باخبر نمی‌شود. نوشتن هر دو در یک تراکنش این
پنجره را می‌بندد. هزینه‌اش تحویل at-least-once به‌جای exactly-once است —
برای همین مصرف‌کننده‌ها ایدمپوتنت‌اند.

**claim یک عبارت اتمیک است.** `INSERT INTO processed_events` یا موفق
می‌شود یا به کلید اصلی برخورد می‌کند. یک check-then-act (`SELECT` بعد
`INSERT`) اجازه می‌داد دو worker هر دو در یک تحویل مجدد از چک عبور کنند.
یک آزمون هشت thread را روی یک event id مسابقه می‌دهد و تضمین می‌کند دقیقاً
یکی برنده می‌شود.

**یک handler ناموفق claim خودش را آزاد می‌کند.** این نکته‌ی ظریف است: اگر
claim نشتی می‌کرد، تلاش مجدد به‌عنوان تکراری رد می‌شد و رویداد بی‌سروصدا
ناپدید می‌شد — بدتر از خطای اصلی. یک آزمون دقیقاً همین را چک می‌کند.

**relay در اولین شکست publish متوقف می‌شود.** به رویداد بعدی نمی‌پرد، چون
این باعث تحویل خارج از ترتیب می‌شد. هر چیزی پشت آن شکست، در حالت pending
می‌ماند و در دور بعد دوباره تلاش می‌شود.

**خطاهای دائمی و موقت متفاوت‌اند.** یک handler برای payload‌ای که هرگز
موفق نمی‌شود (فیلد گم‌شده) `PermanentError` می‌دهد و بلافاصله به
dead-letter می‌رود، به‌جای سوزاندن سه تلاش رویش.

**نوع رویداد ناشناخته تأیید می‌شود، نه صف‌مجدد.** در یک ناوگان واقعی ممکن
است سرویس دیگری مصرف‌کننده‌ی مورد نظر باشد. صف‌مجدد برای همیشه چرخ می‌زد.

**رویدادها نسخه‌ی schema حمل می‌کنند.** مصرف‌کننده‌ای که payload جدیدتر از
درک خودش می‌گیرد، به‌جای بدخوانی آن را رد می‌کند — این در یک rolling deploy
که مصرف‌کننده‌های قدیم و جدید کنار هم اجرا می‌شوند، اهمیت دارد.

**dead letterها قابل بازپخش‌اند.** ازدست‌دادن یک پیام به‌خاطر باگی که از
قبل رفعش کرده‌ای قابل‌قبول نیست؛ `POST /dead-letters/{id}/replay` آن را با
شمارنده‌ی تلاش صفرشده برمی‌گرداند.

---

## آزمون‌ها — ۵۳ مورد

| بخش | پوشش می‌دهد |
|---|---|
| پاکت رویداد | رفت‌وبرگشت، اعتبارسنجی، JSON خراب، schema آینده |
| اتمیک‌بودن outbox | سفارش+رویداد باهم، بدون یتیم‌ماندن هنگام رد، ترتیب |
| Relay | تحویل، قطعی بروکر، حفظ ترتیب سفارش‌ها هنگام خطا |
| ایدمپوتنسی | claim یک‌باره، مسابقه‌ی ۸ thread، آزادسازی/بازگیری، تحویل مجدد |
| تلاش مجدد و DLQ | ریاضیات backoff، صف‌مجدد با شماره‌ی تلاش، dead-letter، بازپخش |
| آزادسازی claim | یک handler ناموفق نباید مانع تلاش مجدد خودش شود |
| سرتاسری | چرخه‌ی کامل، ۴۰ سفارش بدون هیچ گمشدگی، مدیریت پیام مسموم |
| API | ثبت، انتقال وضعیت، اعتبارسنجی، عملیات dead-letter |

---

## CI

`.github/workflows/ci.yml` روی Python 3.11/3.12/3.13 لینت و آزمون اجرا
می‌کند، بعد ایمیج Docker را می‌سازد و **کانتینر در حال اجرا را smoke-test
می‌کند** — منتظر `/health` می‌ماند، یک سفارش واقعی ثبت می‌کند و تضمین می‌کند
به وضعیت `reserved` می‌رسد. ایمیجی که ساخته می‌شود ولی سرویس نمی‌دهد، build
موفق حساب نمی‌شود.

---

## ساختار

```
order-events/
├── orderflow/
│   ├── events.py      # پاکت، نسخه‌بندی، سریالایز
│   ├── store.py       # سفارش‌ها، outbox، claimها، dead letterها (SQLite)
│   ├── broker.py       # پروتکل Broker: memory | flaky | redis
│   ├── relay.py       # outbox → broker
│   ├── worker.py      # ایدمپوتنسی، نردبان تلاش مجدد، dead-lettering
│   ├── handlers.py    # منطق کسب‌وکار
│   └── api.py         # FastAPI
├── tests/test_orderflow.py
├── .github/workflows/ci.yml
├── Dockerfile · docker-compose.yml
└── demo.py
```

## مجوز

MIT
