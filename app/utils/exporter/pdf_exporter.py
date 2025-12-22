### app/utils/exporter/pdf_exporter.py

# Standard library imports
from io import BytesIO

# Third party imports
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

# Local imports
from app.utils.exporter.base import DataExportBase


class PDFExporter(DataExportBase):
    """Export data to PDF file"""

    def __init__(self, data, title: str = "Report"):
        self.title = title
        super().__init__(data)

    def export(self) -> BytesIO:
        """Export data to PDF file"""
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer, pagesize=landscape(A4), rightMargin=30,
            leftMargin=30, topMargin=30, bottomMargin=30
        )
        styles = getSampleStyleSheet()
        elements = [Paragraph(self.title, styles['Title']), Spacer(1, 12)]

        data = [self.df.columns.tolist()] + self.df.astype(str).values.tolist()
        table = Table(data, repeatRows=1)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#2F5597")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('BACKGROUND', (0, 1), (-1, -1), colors.whitesmoke),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ]))
        elements.append(table)
        doc.build(elements)
        buffer.seek(0)
        return buffer