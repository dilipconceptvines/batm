# ## app/bpm_flows/vehiclerepair/flows.py

# # Standard library imports
# from datetime import datetime

# # Third party imports
# from fastapi import HTTPException

# # Local imports
# from app.utils.logger import get_logger
# from app.audit_trail.services import audit_trail_service
# from app.bpm.step_info import step
# from app.bpm.services import bpm_service
# from app.vehicles.services import vehicle_service
# from app.medallions.services import medallion_service
# from app.uploads.services import upload_service

# logger = get_logger(__name__)
# entity_mapper = {
#     "VEHICLE_REPAIR": "vehicle_repairs",
#     "VEHICLE_REPAIR_IDENTIFIER": "id",
# }

# @step(step_id="129", name="Fetch - Vehicle Repair Details", operation='fetch')
# def fetch_vehicle_repair_details(db, case_no, case_params=None):
#     """Fetch vehicle repair details"""
#     try:
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no)

#         # Get vehicle and repair details if case exists
#         vehicle = None
#         repair = None

#         if case_entity:
#             repair = vehicle_service.get_repair(db, vehicle_id=int(case_entity.identifier_value))
#             vehicle = vehicle_service.get_vehicles(db, vehicle_id=int(case_entity.identifier_value))
#         elif case_params:
#             vehicle = vehicle_service.get_vehicles(db, vin=case_params['object_lookup'])

#         if not vehicle:
#             return {}
        
#         if not case_entity:
#             case_entity = bpm_service.create_case_entity(
#                 db, case_no=case_no,
#                 entity_name=entity_mapper['VEHICLE_REPAIR'],
#                 identifier=entity_mapper['VEHICLE_REPAIR_IDENTIFIER'],
#                 identifier_value=str(vehicle.id)
#             )
#             logger.info("Entity is created for %s", case_entity.entity_name)
     
#         return {
#             "vehicle_info": {
#                 "vehicle_id": vehicle.id,
#                 "entity_name": vehicle.vehicle_entity.entity_name,
#                 "vin": vehicle.vin if vehicle.vin else "",
#                 "make": vehicle.make if vehicle.make else "",
#                 "model": vehicle.model if vehicle.model else "",
#                 "year": vehicle.year if vehicle.year else "",
#                 "vehicle_type": vehicle.vehicle_type if vehicle.vehicle_type else "",
#                 "cylinders": vehicle.cylinders if vehicle.cylinders else 0,
#             },
#             "repair_details": repair.to_dict() if repair else {},
#             'invoice_document': upload_service.get_documents(
#                 db, object_type="vehicle", object_id=vehicle.vin, document_type="repair_invoice"
#             ),
#         }
#     except Exception as e:
#         logger.error("Error fetching vehicle repair details: %s", e)
#         raise HTTPException(status_code=500, detail=str(e)) from e


# @step(step_id="129", name="Process - Save Vehicle Repair Details", operation='process')
# def process_vehicle_repair_details(db, case_no, step_data):
#     """Process and save vehicle repair details"""
#     try:
#         case_entity = bpm_service.get_case_entity(db, case_no=case_no)
        
#         # Get vehicle by VIN
#         vehicle = vehicle_service.get_vehicles(db, vin=step_data['vin'])
#         if not vehicle:
#             raise ValueError(f"Vehicle with VIN {step_data['vin']} not found")
        
#         # Prepare repair data
#         repair_data = {}

#         date_fields = ['invoice_date', 'vehicle_in_date', 'vehicle_out_date', 'next_service_due_by']
    
#         for field in date_fields:
#             if field in step_data and step_data[field]:
#                 try:
#                     repair_data[field] = datetime.strptime(step_data[field], '%Y-%m-%d').date()
#                 except ValueError:
#                     logger.error("Invalid date format for %s: %s", field, step_data[field])
                
#         other_fields = ['vehicle_in_time', 'vehicle_out_time', 'invoice_amount', 'repair_paid_by', 'remarks']
#         for field in other_fields:
#             if field in step_data:
#                 repair_data[field] = step_data[field]

#         # Get or create repair record
    
#         repair = vehicle_service.get_repair(db, vehicle_id=int(case_entity.identifier_value))
        
#         if repair:
#             repair  = vehicle_service.upsert_repair(db, {
#                 "id": repair.id,
#                 **repair_data
#             })
#         else:
#             repair = vehicle_service.upsert_repair(db, {
#                 "vehicle_id": vehicle.id,
#                 **repair_data
#             })

#         case = bpm_service.get_cases(db=db , case_no= case_no)
#         if case:
#             medallion_info = medallion_service.get_medallion(db, medallion_id=vehicle.medallion_id if vehicle.medallion_id else None)
#             if medallion_info:
#                 medallion_owner = medallion_service.get_medallion_owner(db, medallion_owner_id=medallion_info.owner_id)
#                 if medallion_owner:
#                     audit_trail_service.create_audit_trail(
#                         db=db,
#                         case=case,
#                         description=f"Saved vehicle repair details for vehicle {vehicle.vin} with medallion {medallion_info.medallion_number} for owner {medallion_owner.id}",
#                         meta_data={"medallion_owner_id": medallion_owner.id , "medallion_id": medallion_info.id , "vehicle_id": vehicle.id}
#                     )
                    
#         return "Ok"
#     except Exception as e:
#         logger.error("Error processing vehicle repair details: %s", e)
#         raise HTTPException(status_code=500, detail=str(e)) from e
