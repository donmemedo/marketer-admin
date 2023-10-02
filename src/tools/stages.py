import math

plan_one = {
    "1": {"start": 0, "end": 200000000000, "marketer_share": 0.35},
    "2": {"start": 200000000000, "end": 400000000000, "marketer_share": 0.4},
    "3": {"start": 400000000000, "end": 600000000000, "marketer_share": 0.45},
    "4": {"start": 600000000000, "end": math.inf, "marketer_share": 0.5},
}
plan_two = {
    "payeh": {"start": 0, "end": 30000000000, "marketer_share": 0},
    "morvarid": {"start": 30000000000, "end": 50000000000, "marketer_share": 0.1},
    "firouzeh": {"start": 50000000000, "end": 100000000000, "marketer_share": 0.15},
    "aghigh": {"start": 100000000000, "end": 150000000000, "marketer_share": 0.2},
    "yaghout": {"start": 150000000000, "end": 200000000000, "marketer_share": 0.25},
    "zomorod": {"start": 200000000000, "end": 300000000000, "marketer_share": 0.3},
    "tala": {"start": 300000000000, "end": 400000000000, "marketer_share": 0.35},
    "almas": {"start": 400000000000, "end": math.inf, "marketer_share": 0.4},
}
plan_three = {
    "0": {"start": 0, "end": 30000000000, "marketer_share": 0},
    "1": {"start": 10000000000, "end": 500000000000, "marketer_share": 0.4},
    "2": {"start": 500000000000, "end": 700000000000, "marketer_share": 0.45},
    "3": {"start": 700000000000, "end": math.inf, "marketer_share": 0.5},
}
no_plan = {
    "NoPlan": {"start": 0, "end": math.inf, "marketer_share": 0.1},
}

const_one = {"1": {"start": 0, "end": math.inf, "marketer_share": 0.42632}}
const_two = {"1": {"start": 0, "end": math.inf, "marketer_share": 0.35526}}
const_three = {"1": {"start": 0, "end": math.inf, "marketer_share": 0.4}}
const_four = {"1": {"start": 0, "end": math.inf, "marketer_share": 0.35}}

plans = {
    "جدول شماره ۱": plan_one,
    "جدول شماره ۲": plan_two,
    "جدول شماره ۳": plan_three,
    "نامشخص": no_plan,
    "ثابت شماره ۱": const_one,
    "ثابت شماره ۲": const_two,
    "ثابت شماره ۳": const_three,
    "ثابت شماره ۴": const_four,
}
ContractType = {"Agency": "نمایندگی", "Independent": "مستقل"}

CalculationBaseType = {
    "PlanOne": "جدول شماره ۱",
    "PlanTwo": "جدول شماره ۲",
    "PlanThree": "جدول شماره ۳",
    "NoPlan": "نامشخص",
    "ConstOne": "ثابت شماره ۱",
    "ConstTwo": "ثابت شماره ۲",
    "ConstThree": "ثابت شماره ۳",
    "ConstFour": "ثابت شماره ۴",
}

CoefficientBaseType = {"Plan": "پلکان", "Const": "ثابت"}
