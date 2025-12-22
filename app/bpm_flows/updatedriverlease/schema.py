## app/bpm_flows/updatedriverlease/schema.py

from typing import Literal, Optional

from pydantic import BaseModel


# DOV Lease
class DOVFinancialInfo(BaseModel):
    management_recommendation: Optional[float]
    med_lease: Optional[float]
    med_tlc_maximum_amount: Optional[float]
    veh_lease: Optional[float]
    veh_tlc_maximum_amount: Optional[float]
    lease_amount: Optional[float]


class DOVLease(BaseModel):
    leaseType: str
    financialInformation: DOVFinancialInfo


# Long-Term Lease
class LongTermFinancialInfo(BaseModel):
    management_recommendation: Optional[float]
    day_shift: Optional[float]
    day_tlc_maximum_amount: Optional[float]
    night_shift: Optional[float]
    night_tlc_maximum_amount: Optional[float]
    lease_amount: Optional[float]


class LongTermLease(BaseModel):
    leaseType: str
    financialInformation: LongTermFinancialInfo


# Short-Term Lease
class ShortTermDayNight(BaseModel):
    day_shift: Optional[float] = None
    night_shift: Optional[float] = None


class ShortTermLease(BaseModel):
    leaseType: Literal["short-term"]
    financialInformation: Optional[dict] = {
        "1_week_or_longer": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "1_week_or_longer_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "sun": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "sun_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "mon": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "mon_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "tus": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "tus_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "wen": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "wen_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "thu": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "thu_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "fri": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "fri_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "sat": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        },
        "sat_tlc_maximum_amount": {
            "day_shift": Optional[float],
            "night_shift": Optional[float],
        }
    }


# class ShortTermLease(BaseModel):
#     leaseType: Literal["short-term"]
#     financialInformation: Dict[
#         Literal["1_week_or_longer", "1_week_or_longer_tlc_maximum_amount",
#                 "sun", "sun_tlc_maximum_amount", "mon", "mon_tlc_maximum_amount",
#                 "tus", "tus_tlc_maximum_amount", "wen", "wen_tlc_maximum_amount",
#                 "thu", "thu_tlc_maximum_amount", "fri", "fri_tlc_maximum_amount",
#                 "sat", "sat_tlc_maximum_amount"],
#         Optional[ShortTermDayNight]
#     ] = {
#         "1_week_or_longer": None,
#         "1_week_or_longer_tlc_maximum_amount": None,
#         "sun": None,
#         "sun_tlc_maximum_amount": None,
#         "mon": None,
#         "mon_tlc_maximum_amount": None,
#         "tus": None,
#         "tus_tlc_maximum_amount": None,
#         "wen": None,
#         "wen_tlc_maximum_amount": None,
#         "thu": None,
#         "thu_tlc_maximum_amount": None,
#         "fri": None,
#         "fri_tlc_maximum_amount": None,
#         "sat": None,
#         "sat_tlc_maximum_amount": None
#     }


# Medallion-Only Lease
class MedallionOnlyFinancialInfo(BaseModel):
    weekly_lease_rate: Optional[float]
    week_tlc_maximum_amount: Optional[float]


class MedallionOnlyLease(BaseModel):
    leaseType: str
    financialInformation: MedallionOnlyFinancialInfo
