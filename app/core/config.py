# app/core/config.py

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

import boto3
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.utils.logger import get_logger

logger = get_logger(__name__)


#
# =====================================================
#  GENERIC SECRET FETCH FUNCTION (DB, Redis, DocuSign, AWS)
# =====================================================
#


@lru_cache(maxsize=32)
def cached_secret_values(secret_id: str | None, region: str | None) -> dict:
    """
    Load arbitrary secret from AWS Secrets Manager.
    Returns {} if secret_id is not set or is an empty string.

    Because of @lru_cache, each unique (secret_id, region) pair is fetched
    only once per application start.
    """
    # Handle both None and empty string cases
    if not secret_id or secret_id.strip() == "":
        return {}

    region = region or os.getenv("AWS_REGION", "us-east-1")
    logger.info("Loading secret", secret_id=secret_id, region=region)

    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_id)
    data = json.loads(resp["SecretString"])

    logger.info("Loaded secret from Secrets Manager", secret_id=secret_id)
    return data


#
# =====================================================
#                    SETTINGS CLASS
# =====================================================
#


class Settings(BaseSettings):
    """
    Application Settings
    """

    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", case_sensitive=False
    )

    pythonpath: str
    environment: str
    allowed_cors_urls: str

    # AWS + secret IDs for secrets management
    aws_region: str = None

    db_secret_id: str = None  # e.g. bat/staging/db
    redis_secret_id: str = None  # e.g. bat/staging/redis
    docusign_secret_id: str = None  # e.g. bat/staging/docusign
    aws_credentials_secret_id: str = None  # e.g. bat/staging/aws

    # AWS credentials base fields (for .env / local)
    aws_access_key_id_base: str = None
    aws_secret_access_key_base: str = None
    aws_ses_configuration_set: str = None

    # Redis base fields (for .env / local)
    redis_host: str
    redis_port: str
    redis_username: Optional[str] = None
    redis_password: Optional[str] = None

    # DB values remain normal fields (support .env)
    db_host: str
    db_user: str
    db_password: str
    db_database: str
    db_port: int = 3360

    json_config: str = None

    bpm_file_key: str = None
    bat_file_key: str = None

    document_storage_dir: str = None
    allowed_file_types: str = None
    allowed_file_size: int = None

    aws_sns_sender_id: str = None
    aws_ses_sender_email: str = None
    aws_ses_configuration_set: str = None
    aws_admin_email: str = None
    s3_bucket_name: str = None

    # Logging configuration
    uvicorn_log_level: str = "DEBUG"
    # Email and SMS control flags
    enable_email_sending: bool = True
    enable_sms_sending: bool = True

    # Email override recipients (comma-separated)
    # When set, ALL emails will go to these addresses instead of actual recipients
    override_email_to: str  # e.g., "test@example.com,admin@example.com"
    override_email_cc: str  # e.g., "manager@example.com"

    # SMS override recipient (E.164 format)
    # When set, ALL SMS will go to this phone number instead of actual recipients
    override_sms_to: str = None  # e.g., "+11234567890"

    # SMS override recipient (E.164 format)
    # When set, ALL SMS will go to this phone number instead of actual recipients
    override_sms_to: str = None  # e.g., "+11234567890"

    claude_model_id: str = None
    app_base_url: str = None

    address_update_template_id: str = None
    storage_receipt_template_id: str = None
    dov_vehicle_lease_template_id: str = None
    long_term_lease_template_id: str = None
    medallion_lease_template_id: str = None
    driver_transaction_template_id: str = None
    royalty_agreement_corp_template_id: str = None
    royalty_agreement_llc_template_id: str = None
    royalty_agreement_individual_template_id: str = None
    rider_document_template_id: str = None
    medallion_cover_letter_template_id: str = None
    medallion_designation_template_id: str = None
    power_of_attorney_template_id: str = None
    additional_driver_template_id: str = None

    secret_key: str = None
    algorithm: str = None
    access_token_expire_minutes: int = None
    refresh_token_expire_days: int = None

    vin_x_auth_key: str = ""
    firebase_cred_path: str = ""

    # Docusign
    docusign_client_id: str = None
    docusign_user_id: str = None
    docusign_account_id: str = None
    docusign_auth_server: str = "account-d.docusign.com"
    docusign_base_path: str = "https://demo.docusign.net/restapi"
    docusign_private_key_s3_key: str = None  # base value from .env (fallback)
    docusign_webhook_secret: str = None
    docusign_pem_path: str = None
    docusign_connect_webhook_url: str = None
    docusign_host_name: str = None
    docusign_host_email: str = None
    docusign_placeholder_email: str = ""

    bat_manager_name: str = ""
    bat_authorized_agent: str = ""
    payment_date: str = ""
    security_deposit_holding_number: str = ""
    security_deposit_holding_bank: str = ""
    security_deposit_located_at: str = ""

    tlc_vehicle_cap_total: float = 0.00
    tlc_medallion_weekly_cap_regular: float = 0.00
    tlc_medallion_weekly_cap_hybrid: float = 0.00
    tlc_inspection_fees: float = 0.00
    tax_stamps: float = 0.00
    registration: float = 0.00

    common_date_format: str = ""
    common_time_format: str = ""
    common_signature_file: str = ""

    dov_security_deposit_cap: float = 0.00
    long_term_medallion_weekly_cap_regular_full: float = 0.00
    long_term_medallion_weekly_cap_hybrid_full: float = 0.00

    long_term_medallion_weekly_cap_regular_day: float = 0.00
    long_term_medallion_weekly_cap_hybrid_day: float = 0.00

    shift_lease_medallion_weekly_cap_regular_day: float = 0.00
    shift_lease_medallion_weekly_cap_hybrid_day: float = 0.00

    shift_lease_medallion_weekly_cap_regular_night: float = 0.00
    shift_lease_medallion_weekly_cap_hybrid_night: float = 0.00

    lease_deposit_release_days: Optional[int] = 30
    lease_termination_reasons: str = ""
    lease_6_months: int = 0
    day_name_to_num: dict = {}
    full_time_drivers: str = ""
    day_shift_drivers: str = ""
    night_shift_drivers: str = ""

    # Lease renewal periods (months)
    dov_lease_renewal_period: int = 6
    long_term_lease_renewal_period: int = 6
    medallion_lease_renewal_period: int = 6
    shift_lease_renewal_period: int = 6
    short_term_lease_renewal_period: int = 6

    lease_renewal_reminder_days: int = 29

    admin_renewal_notification_template_key: str = (
        "email_sms_templates/admin_renewal_notification.html"
    )
    lease_renewal_email_template_key: str = (
        "email_sms_templates/lease_renewal_email.html"
    )
    lease_renewal_sms_template_key: str = "email_sms_templates/lease_renewal_sms.txt"

    super_admin_email_id: str = "superadmin@bat.com"

    tlc_service_fee: int = 10

    # Version file path
    version_path: str = ".version"

    dmv_license_expiry_reminder_subject_template: str
    dmv_license_expiry_subject_template: str
    tlc_license_expiry_reminder_subject_template: str
    tlc_license_expiry_subject_template: str

    driver_license_expiry_email_template: str
    driver_license_expiry_sms_template: str
    
    lease_creation_welcome_template: str
    lease_creation_welcome_subject_template: str

    events_config_path: str
    driver_license_expiry_email_template: str
    driver_license_expiry_sms_template: str

    lease_creation_welcome_template: str
    lease_creation_welcome_subject_template: str

    # Password reset email configuration
    password_reset_email_template_key: str = None
    password_reset_email_subject_template: str = None

    #
    # ---------------------------
    #  DB ACCESS PROPERTIES
    # ---------------------------
    #
    @property
    def _db_tuple(self):
        """
        Resolve DB connection details:
        - If db_secret_id is set → use secret (DB_HOST, DB_USER, DB_PASSWORD, DB_DATABASE, DB_PORT)
        - Else → use .env values
        """
        data = cached_secret_values(self.db_secret_id, self.aws_region)

        if data:
            logger.info(
                "DB config source: Secrets Manager", secret_id=self.db_secret_id
            )
        else:
            logger.info("💾 DB config source: .env / environment variables")

        host = data.get("DB_HOST") or self.db_host
        user = data.get("DB_USER") or self.db_user
        password = data.get("DB_PASSWORD") or self.db_password
        database = data.get("DB_DATABASE") or self.db_database
        port = int(data.get("DB_PORT") or self.db_port)

        return host, user, password, database, port

    @property
    def db_url(self) -> str:
        """Construct the synchronous database URL."""
        host, user, password, database, port = self._db_tuple
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

    @property
    def async_db_url(self) -> str:
        """Construct the asynchronous database URL."""
        host, user, password, database, port = self._db_tuple
        return f"mysql+asyncmy://{user}:{password}@{host}:{port}/{database}"

    #
    # ---------------------------
    #  REDIS ACCESS PROPERTIES
    # ---------------------------
    #
    @property
    def _redis_tuple(self):
        """
        Resolve Redis connection details:
        - If redis_secret_id is set → use secret (REDIS_HOST, REDIS_PORT, REDIS_USERNAME, REDIS_PASSWORD)
        - Else → use .env values
        """
        data = cached_secret_values(self.redis_secret_id, self.aws_region)

        if data:
            logger.info(
                "Redis config source: Secrets Manager", secret_id=self.redis_secret_id
            )
        else:
            logger.info("Redis config source: .env / environment variables")

        # Use explicit defaults for localhost development
        host = data.get("REDIS_HOST") or self.redis_host or "localhost"
        port = data.get("REDIS_PORT") or self.redis_port or "6379"
        username = data.get("REDIS_USERNAME") or self.redis_username
        password = data.get("REDIS_PASSWORD") or self.redis_password

        return host, port, username, password

    @property
    def redis_url(self) -> str:
        """Construct the Redis URL."""
        host, port, username, password = self._redis_tuple

        if username and password:
            return f"redis://{username}:{password}@{host}:{port}"
        elif password:
            return f"redis://:{password}@{host}:{port}"
        else:
            return f"redis://{host}:{port}"

    @property
    def cache_manager(self) -> str:
        """Construct the Redis URL for cache manager (DB 0)."""
        return f"{self.redis_url}/0"

    @property
    def celery_broker(self) -> str:
        """Construct the Redis URL for Celery broker (DB 1)."""
        return f"{self.redis_url}/1"

    @property
    def celery_backend(self) -> str:
        """Construct the Redis URL for Celery backend (DB 2)."""
        return f"{self.redis_url}/2"

    #
    # ---------------------------
    #  AWS ACCESS KEYS
    # ---------------------------
    #
    @property
    def aws_access_key_id(self):
        """
        Resolve AWS_ACCESS_KEY_ID:

        1. If aws_credentials_secret_id is set:
           → Use AWS_ACCESS_KEY_ID from that secret
        2. Else:
           → Use AWS_ACCESS_KEY_ID from .env / environment variables
        """
        data = cached_secret_values(self.aws_credentials_secret_id, self.aws_region)
        if data:
            logger.info(
                "AWS_ACCESS_KEY_ID source: Secrets Manager",
                secret_id=self.aws_access_key_id_base,
            )
        else:
            logger.info("AWS_ACCESS_KEY_ID source: .env / environment variables")

        return data.get("AWS_ACCESS_KEY_ID") or self.aws_access_key_id_base

    @property
    def aws_secret_access_key(self):
        """
        Resolve AWS_SECRET_ACCESS_KEY:

        1. If aws_credentials_secret_id is set:
           → Use AWS_SECRET_ACCESS_KEY from that secret
        2. Else:
           → Use AWS_SECRET_ACCESS_KEY from .env / environment variables
        """
        data = cached_secret_values(self.aws_credentials_secret_id, self.aws_region)
        if data:
            logger.info(
                "AWS_SECRET_ACCESS_KEY source: Secrets Manager",
                secret_id=self.aws_secret_access_key_base,
            )
        else:
            logger.info("AWS_SECRET_ACCESS_KEY source: .env / environment variables")

        return data.get("AWS_SECRET_ACCESS_KEY") or self.aws_secret_access_key_base

    #
    # ---------------------------
    #  APPLICATION VERSION
    # ---------------------------
    #
    @property
    def app_version(self) -> str:
        """
        Read application version from JSON file specified in VERSION_PATH.
        Returns '-' if file not found or error occurs.
        This is read once and cached by the property.
        """
        try:
            version_file = Path(self.version_path)
            if version_file.exists():
                version_data = json.loads(version_file.read_text(encoding="utf-8"))
                version = version_data.get("version", "-")
                logger.info(
                    f"Application version found {version} from path {self.version_path}"
                )
                return version
            else:
                logger.warning("Version file not found", version_path=self.version_path)
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse version file as JSON", error=str(e))
        except Exception as e:
            logger.warning("Failed to read version file", error=str(e))
        return "-"


#
# Instantiate settings
#
settings = Settings()


#
# Helper for DocuSign private key S3 key
#


def get_docusign_private_key_s3_key():
    """
    Return the DocuSign private key S3 key:

    - If docusign_secret_id is set:
        Uses DOCUSIGN_PRIVATE_KEY_S3_KEY from Secrets Manager (bat/staging/docusign)
    - Else:
        Falls back to `docusign_private_key_s3_key` from .env
    """
    data = cached_secret_values(settings.docusign_secret_id, settings.aws_region)
    if data:
        logger.info(
            "DOCUSIGN_PRIVATE_KEY_S3_KEY source: Secrets Manager",
            secret_id=settings.docusign_secret_id,
        )
    else:
        logger.info("DOCUSIGN_PRIVATE_KEY_S3_KEY source: .env / environment variables")
    return (
        data.get("DOCUSIGN_PRIVATE_KEY_S3_KEY") or settings.docusign_private_key_s3_key
    )
