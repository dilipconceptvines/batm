# {
#   "$schema": "http://json-schema.org/draft-07/schema#",
#   "title": "Choose Driver/Lease for TLC Violation",
#   "description": "Schema for validating the selected driver and lease context for a new TLC violation.",
#   "type": "object",
#   "properties": {
#     "driver_pk_id": {
#       "description": "The primary key (ID) of the selected driver from the 'drivers' table.",
#       "type": "integer"
#     },
#     "vehicle_id": {
#       "description": "The primary key (ID) of the associated vehicle from the 'vehicles' table.",
#       "type": "integer"
#     },
#     "medallion_id": {
#       "description": "The primary key (ID) of the associated medallion from the 'medallions' table.",
#       "type": "integer"
#     },
#     "lease_id": {
#       "description": "The primary key (ID) of the active lease from the 'leases' table.",
#       "type": "integer"
#     },
#     "vehicle_plate_no": {
#       "description": "The plate number of the associated vehicle, submitted for record keeping.",
#       "type": "string"
#     }
#   },
#   "required": [
#     "driver_pk_id",
#     "vehicle_id",
#     "medallion_id",
#     "lease_id",
#     "vehicle_plate_no"
#   ],
#   "additionalProperties": false
# }


# {
#   "$schema": "http://json-schema.org/draft-07/schema#",
#   "title": "TLC Violation Details",
#   "description": "Schema for validating the details of a manually entered TLC violation.",
#   "type": "object",
#   "properties": {
#     "plate": {
#       "description": "The license plate number of the vehicle.",
#       "type": "string",
#       "minLength": 1,
#       "maxLength": 50
#     },
#     "state": {
#       "description": "The state of the license plate (e.g., 'NY').",
#       "type": "string",
#       "minLength": 2,
#       "maxLength": 10
#     },
#     "type": {
#       "description": "The type code of the violation.",
#       "type": "string",
#       "enum": ["FI", "FN", "RF"]
#     },
#     "summons": {
#       "description": "The unique summons number from the ticket.",
#       "type": "string",
#       "minLength": 1,
#       "maxLength": 255
#     },
#     "issue_date": {
#       "description": "The date the violation was issued.",
#       "type": "string",
#       "format": "date"
#     },
#     "issue_time": {
#       "description": "The time the violation was issued in HHMM(A/P) format (e.g., '0550P').",
#       "type": ["string", "null"],
#       "pattern": "^(0[1-9]|1[0-2])[0-5][0-9][AP]$"
#     },
#     "description": {
#       "description": "Violation description, required if type is 'FN'.",
#       "type": ["string", "null"]
#     },
#     "amount": {
#       "description": "The base ticket amount of the violation.",
#       "type": "number",
#       "exclusiveMinimum": 0
#     },
#     "service_fee": {
#       "description": "Optional service fee.",
#       "type": "number",
#       "minimum": 0
#     },
#     "disposition": {
#       "description": "The current disposition of the ticket.",
#       "type": "string",
#       "enum": ["Paid", "Reduced", "Dismissed"]
#     }
#   },
#   "required": [
#     "plate",
#     "state",
#     "type",
#     "summons",
#     "issue_date",
#     "amount",
#     "disposition"
#   ],
#   "if": {
#     "properties": { "type": { "const": "FN" } }
#   },
#   "then": {
#     "required": ["description"]
#   },
#   "additionalProperties": false
# }