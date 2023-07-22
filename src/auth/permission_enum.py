from enum import Enum


class Service(Enum):
    MarketerAdmin = "ادمین مارکتر"


class Modules(Enum):
    All = "همه"
    Marketer = "مارکتر"
    Factor = "فاکتور"
    User = "افراد"
    Database = "دیتابیس"
    TBSSync = "همگام‌سازی با TBS"


class Actions(Enum):
    Read = "خواندن"
    Write = "نوشتن"
    Delete = "حذف"
    Create = "ایجاد کردن"
    Update = "به روز رسانی"
    All = "همه"
