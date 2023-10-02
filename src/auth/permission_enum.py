from enum import Enum


class Service(Enum):
    MarketerAdmin = "ادمین مارکتر"


class Modules(Enum):
    All = "همه"
    Marketer = "مارکتر"
    Factor = "فاکتور"
    Accounting = "حسابداری"
    User = "افراد"
    Database = "دیتابیس"
    TBSSync = "همگام‌سازی با TBS"
    Client = "مارکتر کلاینت"
    MultiFactorCalculation = "محاسبه اولیه فاکتورها"


class Actions(Enum):
    Read = "خواندن"
    Delete = "حذف"
    Create = "ایجاد کردن"
    Update = "به روز رسانی"
    All = "همه"
