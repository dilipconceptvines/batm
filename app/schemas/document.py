### app/schemas/document.py

from enum import Enum

from typing import Dict

from pydantic import BaseModel, Field, field_validator, model_validator, ValidationError


class EditPdfDocumentRequest(BaseModel):
    """
    Request body for editing a PDF document
    """
    template_id: str = Field(..., description="Template ID")
    template_body: dict = Field(..., description="Template Body")



class EditPdfDocumentResponse(BaseModel):
    """
    Response body for editing a PDF document
    """
    template_id: str = Field(..., description="Template ID")
    document_path: str = Field(..., description="Document Path")
    document_url: str = Field(..., description="Document URL")


class YesNo(str, Enum):
    """Enum for Yes/No options"""
    YES = "Yes"
    NO = "No"

class DropdownChoices(str, Enum):
    """Enum for dropdown choices"""
    CHOICE1 = "Choice 1"
    CHOICE2 = "Choice 2"
    CHOICE3 = "Choice 3"

class AddressUpdateFormData(BaseModel):
    """Validation model for address update form data"""
    Name: str = Field(
        ..., 
        min_length=2, 
        max_length=100,
        description="Full name of the person"
    )
    Name_of_Dependent: str = Field(
        ..., 
        alias="Name of Dependent",
        min_length=2,
        max_length=100,
        description="Full name of the dependent"
    )
    Age_of_Dependent: int = Field(
        ..., 
        alias="Age of Dependent",
        ge=0,
        le=120,
        description="Age of the dependent (0-120)"
    )
    Option_1: YesNo = Field(
        ..., 
        alias="Option 1",
        description="First option (Yes/No)"
    )
    Option_2: YesNo = Field(
        ..., 
        alias="Option 2",
        description="Second option (Yes/No)"
    )
    Option_3: YesNo = Field(
        ..., 
        alias="Option 3",
        description="Third option (Yes/No)"
    )
    Dropdown2: DropdownChoices = Field(
        ...,
        description="Dropdown selection (Choice 1, Choice 2, or Choice 3)"
    )

    @field_validator('Age_of_Dependent')
    @classmethod
    def validate_age_string(cls, value: str | int) -> str:
        """Validate and convert age to string"""
        if isinstance(value, str):
            try:
                age = int(value)
                if not 0 <= age <= 120:
                    raise ValueError("Age must be between 0 and 120")
                return str(age)
            except ValueError:
                raise ValueError("Age must be a valid number")
        return str(value)

    @model_validator(mode='before')
    @classmethod
    def validate_string_fields(cls, data: dict) -> dict:
        """Validate and clean string fields"""
        if isinstance(data, dict):
            # Clean and validate Name
            if 'Name' in data and isinstance(data['Name'], str):
                data['Name'] = data['Name'].strip()
                if not data['Name']:
                    raise ValueError("Name cannot be empty or just whitespace")

            # Clean and validate Name of Dependent
            dependent_name = data.get('Name of Dependent')
            if dependent_name and isinstance(dependent_name, str):
                data['Name of Dependent'] = dependent_name.strip()
                if not data['Name of Dependent']:
                    raise ValueError("Dependent name cannot be empty or just whitespace")

        return data

    model_config = {
        "allow_population_by_alias": True,
        "json_schema_extra": {
            "example": {
                "Name": "John Doe",
                "Name of Dependent": "Jane Doe",
                "Age of Dependent": "5",
                "Option 1": "Yes",
                "Option 2": "No",
                "Option 3": "Yes",
                "Dropdown2": "Choice 1"
            }
        }
    }


def validate_document_data(data: Dict):
    """
    Validate the document data
    """
    try:
        # Validate using the DocumentSchema model
        document = AddressUpdateFormData(**data)
        print("Validation successful!")
        return document
    except ValidationError as e:
        print("Validation failed!")
        print(e.json())
        return None