# {
#   "$schema": "http://json-schema.org/draft-07/schema#",
#   "title": "Create Repair Invoice Details",
#   "description": "Schema for validating the manual creation of a vehicle repair invoice.",
#   "type": "object",
#   "properties": {
#     "driver_id": {
#       "description": "The primary key of the associated driver.",
#       "type": "integer"
#     },
#     "lease_id": {
#       "description": "The primary key of the associated active lease.",
#       "type": "integer"
#     },
#     "vehicle_id": {
#       "description": "The primary key of the associated vehicle.",
#       "type": "integer"
#     },
#     "medallion_id": {
#       "description": "The primary key of the associated medallion.",
#       "type": "integer"
#     },
#     "total_amount": {
#       "description": "The total cost of the repair invoice.",
#       "type": "number",
#       "exclusiveMinimum": 0
#     },
#     "start_week": {
#       "description": "The Sunday date when the repayment schedule should begin.",
#       "type": "string",
#       "format": "date"
#     },
#     "workshop_type": {
#       "description": "The type of workshop that performed the repair.",
#       "type": "string",
#       "enum": ["Big Apple Workshop", "External Workshop"]
#     },
#     "invoice_number": {
#       "description": "The unique invoice number from the workshop.",
#       "type": "string",
#       "minLength": 1,
#       "maxLength": 255
#     },
#     "invoice_date": {
#       "description": "The date the repair invoice was issued.",
#       "type": "string",
#       "format": "date"
#     },
#     "notes": {
#       "description": "Optional notes or a description of the repair.",
#       "type": ["string", "null"]
#     }
#   },
#   "required": [
#     "driver_id",
#     "lease_id",
#     "vehicle_id",
#     "medallion_id",
#     "total_amount",
#     "start_week",
#     "workshop_type",
#     "invoice_number",
#     "invoice_date"
#   ],
#   "additionalProperties": false
# }

