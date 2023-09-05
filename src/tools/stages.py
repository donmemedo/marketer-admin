import math

plan_one = {
    "1": {"start": 0, "end": 200000000000, "marketer_share": 0.35},
    "2": {"start": 200000000000, "end": 400000000000, "marketer_share": 0.4},
    "3": {"start": 400000000000, "end": 600000000000, "marketer_share": 0.45},
    "4": {"start": 600000000000, "end": math.inf, "marketer_share": 0.5}
}
plan_two = {
    "0": {"start": 0, "end": 30000000000, "marketer_share": 0},
    "1": {"start": 30000000000, "end": 50000000000, "marketer_share": 0.1},
    "2": {"start": 50000000000, "end": 100000000000, "marketer_share": 0.15},
    "3": {"start": 100000000000, "end": 150000000000, "marketer_share": 0.2},
    "4": {"start": 150000000000, "end": 200000000000, "marketer_share": 0.25},
    "5": {"start": 200000000000, "end": 300000000000, "marketer_share": 0.3},
    "6": {"start": 300000000000, "end": 400000000000, "marketer_share": 0.35},
    "7": {"start": 400000000000, "end": math.inf, "marketer_share": 0.4}
}
plan_three = {
    "0": {"start": 0, "end": 30000000000, "marketer_share": 0},
    "1": {"start": 10000000000, "end": 500000000000, "marketer_share": 0.4},
    "2": {"start": 500000000000, "end": 700000000000, "marketer_share": 0.45},
    "3": {"start": 700000000000, "end": math.inf, "marketer_share": 0.5}
}
plan_four = {
    "1": {"start": 0, "end": math.inf, "marketer_share": 0.42632},
    "2": {"start": 0, "end": math.inf, "marketer_share": 0.35526},
    "3": {"start": 0, "end": math.inf, "marketer_share": 0.4},
    "4": {"start": 0, "end": math.inf, "marketer_share": 0.35},
}

plans = {
    "جدول شماره ۱": plan_one,
    "جدول شماره ۲": plan_two,
    "جدول شماره ۳": plan_three,
    "": plan_four,
}