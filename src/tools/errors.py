errors_mapping = {
    "30001": "موردی در دیتابیس یافت نشد.",
    "30002": "موردی برای ثابتهای فاکتورها یافت نشد.",
    "30003": "شناسه مارکتر را وارد کنید.",
    "30034": "شناسه قرارداد را وارد کنید.",
    "30004": "موردی با شناسه داده شده یافت نشد.",
    "30006": "مارکتر در دیتابیس وجود دارد.",
    "30026": "مارکتر در دیتابیس وجود ندارد.",
    "30007": "شناسه وارد شده در دیتابیس موجود است.",
    "30008": "موردی با متغیرهای داده شده یافت نشد.",
    "30009": "شناسه مارکترها را وارد کنید.",
    "30010": "کمیسیون را وارد کنید.",
    "30011": "مارکترها نباید یکسان باشند.",
    "30012": "این مارکتر زیرمجموعه نفر دیگری است.",
    "30013": "مغایرتی در تاریخ های داده شده  مشاهده نشد.",
    "30015": "کمیسیون را به درستی وارد کنید.",
    "30016": "کمیسیون را به درستی بین صفر و یک وارد کنید.",
    "30017": "تاریخ انتها را درست وارد کنید.",
    "30018": "تاریخ ابتدا را درست وارد کنید.",
    "30019": "این رابطه وجود ندارد.",
    "30025": "PAMCode  را وارد کنید.",
    "30027": "این کاربر در تاریخهای موردنظر معامله ای نداشته است.",
    "30028": "خروجی برای متغیرهای داده شده نداریم.",
    "30030": "شناسه مارکتر و دوره را وارد کنید.",
    "30031": "این مارکتر قرارداد فعالی ندارد لطفا ابتدا قرارداد را وارد کنید.",
    "30032": "این قرارداد دارای اطلاعات است لطفا در تغییرات وارد کنید.",
    "30033": "شماره فاکتور را وارد کنید.",
    "30050": "ورودی نام غیرقابل قبول است.",
    "30051": "ورودی ها را دوباره چک کنید.",
    "30052": "این مارکتر زیرمجموعه کسی نیست.",
    "30066": "کد ملی را درست وارد کنید.",
    "30071": "تاریخ پایان قبل از شروع است.",
    "30072": "این ارتباط وجود دارد.",
    "30090": "تاریخ اشتباه وارد شده است.",
}


def get_error(type: str, code: str):
    if type == "TypeError":
        return {"code": code, "message": errors_mapping[code]}
    if type == "json_invalid":
        return {"code": 400, "message": code}
    if type == "missing":
        return {"code": 412, "message": code}
    if type == "enum":
        return {"code": 412, "message": code}
