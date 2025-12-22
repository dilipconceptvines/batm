# app/utils/pdf_filler_utils.py

import uuid
from io import BytesIO
from typing import Dict, Any

from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from fillpdf import fillpdfs

from app.utils.logger import get_logger
from app.pdf_filler.models import PDFTemplate
from app.utils.s3_utils import s3_utils

logger = get_logger(__name__)


class PDFfiller:
    """
    Utility to fill PDF forms based on templates stored in the database.
    """

    def __init__(self, db: Session):
        self.db = db


    def fill_pdf(self, template_name: str, data: Dict[str, Any]) -> str:
        """
        Fills a PDF template with provided data and uploads it to S3.

        Args:
            template_name: The unique name of the template to use.
            data: A dictionary containing the data to fill into the form.

        Returns:
            The S3 key of the newly generated, filled PDF
        """
        # 1. Fetch the template configuration from the database
        template = self.db.query(PDFTemplate).filter(PDFTemplate.template_name == template_name).first()

        if not template:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="PDF template '{template_name}' not found.")
        
        # 2. Download the template from S3 into memory
        try:
            template_bytes = s3_utils.download_file(template.s3_key)
            if not template_bytes:
                raise FileNotFoundError
            
            template_stream = BytesIO(template_bytes)
        except Exception as e:
            logger.error(f"Error downloading template from S3: {str(e)}", exc_info=True)
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error downloading PDF template.")
        
        # 3. Map the incoming data to the PDF field names
        pdf_data_map = {}
        for pdf_field, data_key in template.field_mapping.items():
            if data_key in data:
                pdf_data_map[pdf_field] = data[data_key]

        # 4. Fill the PDF in memory
        output_pdf_stream = BytesIO()
        fillpdfs.write_fillable_pdf(template_stream, output_pdf_stream, pdf_data_map)
        output_pdf_stream.seek(0) # Rewind the stream to the beginning for uploading

        # 5. Generate a unique S3 key and upload the filled PDF
        output_s3_key = f"filled_pdfs/{template_name}/{uuid.uuid4()}.pdf"
        
        try:
            s3_utils.upload_file(
                file_obj=output_pdf_stream,
                key=output_s3_key,
                content_type="application/pdf"
            )
            logger.info(f"Successfully uploaded filled PDF to S3: {output_s3_key}")
        except Exception as e:
            logger.error(f"Failed to upload filled PDF to S3: {output_s3_key} - {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not save the generated PDF.")

        return output_s3_key