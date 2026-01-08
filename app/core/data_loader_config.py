### app/core/data_loader_config.py

"""
Data Loader Configuration

This config file loads data loader specific settings from .env-data-loader
while inheriting AWS credentials and other base configs from the main settings.
"""

import logging

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger("uvicorn")


class DataLoaderSettings(BaseSettings):
    """
    Data Loader Service Settings
    Loads only data loader specific configuration from .env-data-loader
    AWS credentials come from main settings (app.core.config.settings)
    """

    model_config = SettingsConfigDict(
        env_file=".env-data-loader", extra="ignore", case_sensitive=False
    )

    # S3 Configuration for Data Loader
    data_loader_s3_folder: str = Field(
        default="data-migrations",
        description="S3 folder prefix for data migration jobs",
    )

    # Valid role for accessing data load api's
    data_loader_role_name: str = Field(
        default="",
        description="Valid role for running migrations",
    )

    bat_parses: list[str]
    bpm_parses: list[str]
    
    # Parser sheet configurations - BPM
    parser_roles_sheet: str = Field(
        default="roles",
        description="Sheet name for roles parser",
    )
    parser_users_sheet: str = Field(
        default="users",
        description="Sheet name for users parser",
    )
    parser_case_types_sheet: str = Field(
        default="CaseTypes",
        description="Sheet name for case types parser",
    )
    parser_case_status_sheet: str = Field(
        default="CaseStatus",
        description="Sheet name for case status parser",
    )
    parser_case_step_sheet: str = Field(
        default="CaseStep",
        description="Sheet name for case step parser",
    )
    parser_case_step_config_sheet: str = Field(
        default="CaseStepConfig",
        description="Sheet name for case step config parser",
    )
    parser_case_step_config_paths_sheet: str = Field(
        default="CaseStepConfigFiles",
        description="Sheet name for case step config paths parser",
    )
    parser_case_first_step_config_sheet: str = Field(
        default="CaseFirstStepConfig",
        description="Sheet name for case first step config parser",
    )
    parser_slas_sheet: str = Field(
        default="SLA",
        description="Sheet name for SLAs parser",
    )
    
    # Parser sheet configurations - BAT
    parser_address_sheet: str = Field(
        default="address",
        description="Sheet name for address parser",
    )
    parser_bank_accounts_sheet: str = Field(
        default="bank_accounts",
        description="Sheet name for bank accounts parser",
    )
    parser_individuals_sheet: str = Field(
        default="Individual",
        description="Sheet name for individuals parser",
    )
    parser_vehicle_entity_sheet: str = Field(
        default="vehicle_entity",
        description="Sheet name for vehicle entity parser",
    )
    parser_corporation_sheet: str = Field(
        default="corporation",
        description="Sheet name for corporation parser",
    )
    parser_dealers_sheet: str = Field(
        default="dealers",
        description="Sheet name for dealers parser",
    )
    parser_medallion_sheet: str = Field(
        default="medallion",
        description="Sheet name for medallion parser",
    )
    parser_drivers_sheet: str = Field(
        default="drivers",
        description="Sheet name for drivers parser",
    )
    parser_vehicles_sheet: str = Field(
        default="vehicles",
        description="Sheet name for vehicles parser",
    )
    parser_vehicle_hackups_sheet: str = Field(
        default="vehicle_hackups",
        description="Sheet name for vehicle hackups parser",
    )
    parser_vehicle_registration_sheet: str = Field(
        default="vehicle_registration",
        description="Sheet name for vehicle registration parser",
    )
    parser_vehicle_expenses_sheet: str = Field(
        default="vehicle_expenses",
        description="Sheet name for vehicle expenses parser",
    )
    parser_leases_sheet: str = Field(
        default="leases",
        description="Sheet name for leases parser",
    )
    parser_lease_driver_sheet: str = Field(
        default="lease_driver",
        description="Sheet name for lease driver parser",
    )
    parser_entity_sheet: str = Field(
        default="entity",
        description="Sheet name for entity parser",
    )
    parser_medallion_owner_sheet: str = Field(
        default="medallion_owner",
        description="Sheet name for medallion owner parser",
    )
    parser_vehicle_inspections_sheet: str = Field(
        default="vehicle_inspections",
        description="Sheet name for vehicle inspections parser",
    )
    parser_mo_lease_sheet: str = Field(
        default="mo_lease",
        description="Sheet name for mo lease parser",
    )
    parser_ezpass_sheet: str = Field(
        default="ezpass",
        description="Sheet name for ezpass parser",
    )
    parser_pvb_sheet: str = Field(
        default="pvb",
        description="Sheet name for pvb parser",
    )
    parser_dtrs_sheet: str = Field(
        default="dtrs",
        description="Sheet name for dtrs parser",
    )
    parser_daily_receipts_sheet: str = Field(
        default="daily_receipts",
        description="Sheet name for daily receipts parser",
    )
    parser_curb_trips_sheet: str = Field(
        default="curb_trip",
        description="Sheet name for curb trips parser",
    )


    bat_tables: list[str]
    bpm_tables: list[str]


# Instantiate data loader settings
data_loader_settings = DataLoaderSettings()

logger.info(f"📦 Data Loader S3 Folder: {data_loader_settings.data_loader_s3_folder}")
 