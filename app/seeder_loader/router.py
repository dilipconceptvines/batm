### app/data_loader/router.py

"""
Data Loader API Router

Provides endpoints for uploading and managing data migration files.
Files are organized in S3 by job folder names.

Access Control: Only users with 'data_load_api_user' role can access these endpoints.
"""

from datetime import datetime
from io import BytesIO
from typing import List

import pandas as pd
from app.core.config import settings
from app.core.data_loader_config import data_loader_settings
from app.core.db import get_db
from app.seeder_loader.parser_registry import get_parser, list_parsers , PARSER_REGISTRY
from app.seeder_loader.schemas import (
    FileInfo,
    FileListResponse,
    FileUploadResponse,
    FolderInfo,
    FolderListResponse,
    ListParsersResponse,
    ParseRequest,
    ParseResponse,
    ParserInfo,
    ParserResult,
)
from app.seeder.parsing_result import ParseResult as seeder_ParseResult , create_report , generate_report_key , generate_summary , write_summary_to_excel
from app.users.models import User
from app.users.utils import RoleChecker, get_current_user
from app.utils.logger import get_logger
from app.utils.s3_utils import s3_utils
from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    HTTPException,
    Query,
    UploadFile,
    status,
)
from sqlalchemy.orm import Session
from fastapi.responses import StreamingResponse
from app.seeder_loader.excel_generator import generate_excel_template_for_system

logger = get_logger(__name__)
router = APIRouter(prefix="/data-loader", tags=["Data Loader"])


# Role-based access control - only users with settings configured in data_load_api_user role can access
require_data_loader_role = RoleChecker(
    allowed_roles=[data_loader_settings.data_loader_role_name]
)


@router.get("/template/{system}")
def download_excel_template(system: str, include_examples: bool = False):
    """
    Download dynamically generated Excel template for the specified system (bpm or bat).
    The template is generated based on Pydantic schemas.
    """
    system = system.lower()
    if system not in ["bpm", "bat"]:
        raise HTTPException(status_code=400, detail="Invalid system. Must be 'bpm' or 'bat'.")

    try:
        excel_file = generate_excel_template_for_system(system, include_examples=include_examples)
        filename = f"{system}_excel_template_v1.xlsx"
        
        return StreamingResponse(
            excel_file,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate template: {str(e)}")

@router.post(
    "/upload", response_model=FileUploadResponse, status_code=status.HTTP_201_CREATED
)
async def upload_file(
    job_folder: str = Query(..., description="Job folder name for organizing files"),
    file: UploadFile = File(..., description="Excel file to upload"),
    _: User = Depends(require_data_loader_role),
    current_user: User = Depends(get_current_user),
):
    """
    Upload an Excel file to S3 under a specific job folder.

    **Access:** Requires 'data_load_api_user' role

    **Parameters:**
    - `job_folder`: Name of the folder to organize files (e.g., "migration-2025-01-15")
    - `file`: Excel file (.xlsx, .xls) to upload

    **Returns:**
    - Upload confirmation with S3 key and metadata

    **Example:**
    ```bash
    curl -X POST "http://localhost:8000/api/data-loader/upload?job_folder=migration-jan-2025" \\
         -H "Authorization: Bearer YOUR_TOKEN" \\
         -F "file=@data.xlsx"
    ```
    """
    try:
        # Validate file type
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="File name is required"
            )

        file_extension = file.filename.split(".")[-1].lower()
        allowed_types = ["xlsx", "xls"]

        if file_extension not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid file type. Only {', '.join(allowed_types)} files are allowed.",
            )

        # Sanitize job folder name (remove special characters)
        safe_job_folder = "".join(
            c for c in job_folder if c.isalnum() or c in ("-", "_")
        )
        if not safe_job_folder:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid job folder name. Use only alphanumeric characters, hyphens, and underscores.",
            )

        # Generate S3 key
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        safe_filename = "".join(
            c for c in file.filename if c.isalnum() or c in (".", "-", "_")
        )
        s3_key = f"{data_loader_settings.data_loader_s3_folder}/{safe_job_folder}/{timestamp}_{safe_filename}"

        # Read file content
        file_content = await file.read()
        file_obj = BytesIO(file_content)

        # Determine content type
        content_type = (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            if file_extension == "xlsx"
            else "application/vnd.ms-excel"
        )

        # Upload to S3 using s3_utils
        success = s3_utils.upload_file(
            file_obj=file_obj, key=s3_key, content_type=content_type
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to upload file to S3",
            )

        logger.info(
            f"User {current_user.email_address} uploaded file: {safe_filename} "
            f"to job folder: {safe_job_folder} (S3 key: {s3_key})"
        )

        return FileUploadResponse(
            status="success",
            message="File uploaded successfully",
            job_folder=safe_job_folder,
            file_name=safe_filename,
            s3_key=s3_key,
            uploaded_at=datetime.utcnow().isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during file upload: {str(e)}",
        )


@router.get("/folders", response_model=FolderListResponse)
async def list_folders(
    limit: int = Query(
        10,
        ge=1,
        le=50,
        description="Maximum number of folders to return (default: 10, max: 50)",
    ),
    _: User = Depends(require_data_loader_role),
    current_user: User = Depends(get_current_user),
):
    """
    List job folders sorted by most recent activity (newest first).

    **Access:** Requires 'data_load_api_user' role

    **Parameters:**
    - `limit`: Maximum number of folders to return (default: 10, max: 50)

    **Returns:**
    - List of job folders with last modified date and file count

    **Example:**
    ```bash
    # Get latest 10 folders
    curl -X GET "http://localhost:8000/api/data-loader/folders" \\
         -H "Authorization: Bearer YOUR_TOKEN"

    # Get latest 20 folders
    curl -X GET "http://localhost:8000/api/data-loader/folders?limit=20" \\
         -H "Authorization: Bearer YOUR_TOKEN"
    ```
    """
    try:
        # Get folders with metadata from S3
        folders_data = s3_utils.list_folders_with_metadata(
            prefix=data_loader_settings.data_loader_s3_folder + "/", limit=limit
        )

        # Convert datetime objects to ISO format strings
        folders_info = [
            FolderInfo(
                folder_name=folder["folder_name"],
                last_modified=folder["last_modified"].isoformat(),
                file_count=folder["file_count"],
            )
            for folder in folders_data
        ]

        logger.info(
            f"User {current_user.email_address} listed {len(folders_info)} folders "
            f"(limit={limit})"
        )

        return FolderListResponse(
            status="success", folder_count=len(folders_info), folders=folders_info
        )

    except Exception as e:
        logger.error(f"Error listing folders: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while listing folders: {str(e)}",
        )


@router.get("/list", response_model=FileListResponse)
async def list_files(
    job_folder: str = Query(..., description="Job folder name to list files from"),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Maximum number of files to return per page (default: 100, max: 1000)",
    ),
    continuation_token: str = Query(
        None, description="Token for fetching next page of results"
    ),
    include_presigned_urls: bool = Query(
        False, description="Include presigned URLs for files (default: False)"
    ),
    url_expiration: int = Query(
        3600, description="Presigned URL expiration in seconds (default: 1 hour)"
    ),
    _: User = Depends(require_data_loader_role),
    current_user: User = Depends(get_current_user),
):
    """
    List files in a specific job folder with pagination and optional presigned URLs.

    **Access:** Requires 'data_load_api_user' role

    **Parameters:**
    - `job_folder`: Name of the folder to list files from
    - `limit`: Maximum number of files to return (default: 100, max: 1000)
    - `continuation_token`: Token for fetching next page
    - `include_presigned_urls`: Whether to generate presigned URLs (default: False)
    - `url_expiration`: Expiration time for presigned URLs in seconds (default: 3600)

    **Returns:**
    - List of files with pagination info and optional presigned URLs

    **Example:**
    ```bash
    # List first page (fast, no presigned URLs)
    curl -X GET "http://localhost:8000/api/data-loader/list?job_folder=migration-jan-2025&limit=50" \\
         -H "Authorization: Bearer YOUR_TOKEN"

    # List with presigned URLs
    curl -X GET "http://localhost:8000/api/data-loader/list?job_folder=migration-jan-2025&include_presigned_urls=true" \\
         -H "Authorization: Bearer YOUR_TOKEN"

    # Get next page
    curl -X GET "http://localhost:8000/api/data-loader/list?job_folder=migration-jan-2025&continuation_token=TOKEN" \\
         -H "Authorization: Bearer YOUR_TOKEN"
    ```
    """
    try:
        # Sanitize job folder name
        safe_job_folder = "".join(
            c for c in job_folder if c.isalnum() or c in ("-", "_")
        )
        if not safe_job_folder:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid job folder name. Use only alphanumeric characters, hyphens, and underscores.",
            )

        # Construct S3 prefix
        s3_prefix = f"{data_loader_settings.data_loader_s3_folder}/{safe_job_folder}/"

        # List files from S3 with pagination
        s3_response = s3_utils.list_files(
            prefix=s3_prefix, max_keys=limit, continuation_token=continuation_token
        )

        file_keys = s3_response["keys"]
        is_truncated = s3_response["is_truncated"]
        next_token = s3_response["next_continuation_token"]

        if not file_keys:
            logger.info(f"No files found in job folder: {safe_job_folder}")
            return FileListResponse(
                status="success",
                job_folder=safe_job_folder,
                file_count=0,
                files=[],
                has_more=False,
                next_continuation_token=None,
            )

        # Get file metadata and optionally generate presigned URLs
        files_info = []
        import boto3

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region,
        )

        for key in file_keys:
            try:
                # Get file metadata
                head_response = s3_client.head_object(
                    Bucket=settings.s3_bucket_name, Key=key
                )

                # Generate presigned URL only if requested
                presigned_url = None
                if include_presigned_urls:
                    presigned_url = s3_utils.generate_presigned_url(
                        key=key, expiration=url_expiration
                    )

                files_info.append(
                    FileInfo(
                        file_name=key.split("/")[-1],  # Extract filename from key
                        s3_key=key,
                        file_size=head_response.get("ContentLength", 0),
                        last_modified=head_response.get("LastModified").isoformat(),
                        presigned_url=presigned_url,
                    )
                )
            except Exception as e:
                logger.warning(f"Failed to process file {key}: {e}")
                continue

        logger.info(
            f"User {current_user.email_address} listed {len(files_info)} files "
            f"from job folder: {safe_job_folder} (has_more={is_truncated}, "
            f"include_presigned_urls={include_presigned_urls})"
        )

        return FileListResponse(
            status="success",
            job_folder=safe_job_folder,
            file_count=len(files_info),
            files=files_info,
            has_more=is_truncated,
            next_continuation_token=next_token,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing files: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while listing files: {str(e)}",
        )


@router.get("/parsers", response_model=ListParsersResponse)
async def list_available_parsers(
    include_deprecated: bool = Query(False, description="Include deprecated parsers"),
    _: User = Depends(require_data_loader_role),
    current_user: User = Depends(get_current_user),
):
    """
    List all registered parsers.

    **Access:** Requires 'data_load_api_user' role

    **Parameters:**
    - `include_deprecated`: Whether to include deprecated parsers (default: False)

    **Returns:**
    - List of all registered parsers with their metadata

    **Example:**
    ```bash
    curl -X GET "http://localhost:8000/api/data-loader/parsers?include_deprecated=false" \\
         -H "Authorization: Bearer YOUR_TOKEN"
    ```
    """
    try:
        parsers = list_parsers(include_deprecated=include_deprecated)

        parser_info_list = [
            ParserInfo(
                name=p.name,
                sheet_names=p.sheet_names,
                version=p.version,
                deprecated=p.deprecated,
                description=p.description,
            )
            for p in parsers
        ]

        logger.info(
            f"User {current_user.email_address} listed {len(parser_info_list)} parsers "
            f"(include_deprecated={include_deprecated})"
        )

        return ListParsersResponse(
            status="success",
            total_parsers=len(parser_info_list),
            parsers=parser_info_list,
        )

    except Exception as e:
        logger.error(f"Error listing parsers: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while listing parsers: {str(e)}",
        )


@router.post("/parse", response_model=ParseResponse)
async def parse_file(
    request: ParseRequest = Body(...),
    _: User = Depends(require_data_loader_role),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Parse an Excel file using specified parsers.

    **Access:** Requires 'data_load_api_user' role

    **Parameters:**
    - `parser_names`: List of parser names to execute
    - `s3_key`: S3 key of the Excel file to parse
    - `job_folder`: Optional job folder name for reference
    - `dry_run`: If true, parse without committing to database (default: false)

    **Returns:**
    - Results of each parser execution

    **Example:**
    ```bash
    # Normal run (commits to database)
    curl -X POST "http://localhost:8000/api/data-loader/parse" \\
         -H "Authorization: Bearer YOUR_TOKEN" \\
         -H "Content-Type: application/json" \\
         -d '{
           "parser_names": ["roles"],
           "s3_key": "data-migrations/job123/file.xlsx",
           "job_folder": "job123",
           "dry_run": false
         }'

    # Dry run (validates without committing)
    curl -X POST "http://localhost:8000/api/data-loader/parse" \\
         -H "Authorization: Bearer YOUR_TOKEN" \\
         -H "Content-Type: application/json" \\
         -d '{
           "parser_names": ["roles"],
           "s3_key": "data-migrations/job123/file.xlsx",
           "dry_run": true
         }'
    ```
    """
    # Set dry_run flag on session if needed
    if request.dry_run:
        db.info["dry_run"] = True
        logger.info("Dry run mode enabled - changes will be rolled back")

    try:
        logger.info(
            f"User {current_user.email_address} initiated parsing "
            f"(dry_run={request.dry_run}): "
            f"parsers={request.parser_names}, s3_key={request.s3_key}"
        )

        # Download file from S3
        logger.info(f"Downloading file from S3: {request.s3_key}")
        file_data = s3_utils.download_file(request.s3_key)

        if not file_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File not found in S3: {request.s3_key}",
            )

        # Load Excel file
        try:
            excel_file = pd.ExcelFile(file_data)
            logger.info(f"Loaded Excel file with sheets: {excel_file.sheet_names}")
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to parse Excel file: {str(e)}",
            )

        # Execute each parser
        results = []
        successful = 0
        failed = 0
        parse_results = {}

        parse_names = request.parser_names

        if request.all_parses:
            sheet_names = excel_file.sheet_names
            parsers = data_loader_settings.bpm_parses + data_loader_settings.bat_parses
            parser_names = []
            for prs in parsers:
                if prs in sheet_names:
                    parser_names.append(prs)
            parse_names = parser_names
            
        report_key = generate_report_key(request.s3_key)
        if not report_key:
            logger.info(f"Report generation failed")

        logger.info(f"Report key: {report_key}")
        
        for parser_name in parse_names:
            try:
                # Get parser metadata
                parser_metadata = get_parser(parser_name)

                if not parser_metadata:
                    result = ParserResult(
                        parser_name=parser_name,
                        status="failed",
                        message="Parser not found",
                        sheets_processed=[],
                        error=f"No parser registered with name: {parser_name}",
                    )
                    results.append(result)
                    failed += 1
                    continue

                # Check if all required sheets exist
                missing_sheets = [
                    s
                    for s in parser_metadata.sheet_names
                    if s not in excel_file.sheet_names
                ]
                if missing_sheets:
                    result = ParserResult(
                        parser_name=parser_name,
                        status="failed",
                        message=f"Missing required sheets: {', '.join(missing_sheets)}",
                        sheets_processed=[],
                        error=f"Required sheets not found in Excel file",
                    )
                    results.append(result)
                    failed += 1
                    continue

                # Load DataFrames for required sheets
                sheet_dataframes = [
                    excel_file.parse(sheet_name)
                    for sheet_name in parser_metadata.sheet_names
                ]

                logger.info(
                    f"Executing parser '{parser_name}' with sheets: {parser_metadata.sheet_names}"
                )

                # Execute parser function (db session manager handles commit/rollback)
                if len(sheet_dataframes) == 1:
                    parse_result = parser_metadata.function(db, sheet_dataframes[0])
                    
                    report_generation = create_report(parser_name ,report_key,parse_result)
                    
                    if report_generation:
                        logger.info(f"Report generated successfull for {parser_name}")
                    else:
                        logger.info(f"Report generation failed for {parser_name}")

                    if isinstance(parse_result, seeder_ParseResult):
                        summary = parse_result.summary()
                        parse_results[parser_name] = summary
                    elif isinstance(result, dict):
                        parse_results.update(parse_result)
                else:
                    parse_result = parser_metadata.function(db, *sheet_dataframes)
                    parse_results[parser_name] = parse_result

                logger.info(f"Parser '{parser_name}' completed successfully")
                
                result = ParserResult(
                    parser_name=parser_name,
                    status="success",
                    message=f"Successfully processed {len(parser_metadata.sheet_names)} sheet(s)",
                    sheets_processed=parser_metadata.sheet_names,
                    error=None,
                )
                results.append(result)
                successful += 1

            except Exception as e:
                logger.error(f"Parser '{parser_name}' failed: {e}", exc_info=True)

                result = ParserResult(
                    parser_name=parser_name,
                    status="failed",
                    message="Parser execution failed",
                    sheets_processed=[],
                    error=str(e),
                )
                results.append(result)
                failed += 1


        if parse_results:
            summary_df = pd.DataFrame(columns=["sheet", "inserted", "updated", "failed"])
            for parser_name, summary in parse_results.items():
                summary_df = generate_summary(summary_df, summary, parser_name)

            write_summary_to_excel(report_key, summary_df)
        else:
            logger.info("No successful parsers — summary sheet not generated")

        logger.info(
            f"Parsing completed: {successful} successful, {failed} failed out of {len(results)} parsers"
        )

        logger.info(f"***********Parse results: {parse_results}")

        return ParseResponse(
            status="success" if failed == 0 else "partial",
            message=f"Processed {len(results)} parsers: {successful} successful, {failed} failed",
            job_folder=request.job_folder,
            s3_key=request.s3_key,
            report_presigned_url=s3_utils.generate_presigned_url(report_key) if report_key else None,
            results=results,
            total_parsers=len(results),
            successful=successful,
            failed=failed,
            parse_results=parse_results
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during parse operation: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred during parsing: {str(e)}",
        )
