## app/bpm_flows/allocate_medallion_vehicle/utils.py

def format_vehicle_details(vehicle):
    """Format vehicle details"""
    return {
        "vehicle": {
            "id":vehicle.id,
            "vin": vehicle.vin,
            "make": vehicle.make,
            "model": vehicle.model,
            "year": vehicle.year,
            "tsp":vehicle.tsp,
            "security_type": vehicle.security_type,
            "cylinders": str(vehicle.cylinders) if vehicle.cylinders else 0,
            "color": vehicle.color,
            "vehicle_type": vehicle.vehicle_type,
            "base_price": vehicle.base_price,
            "sales_tax": vehicle.sales_tax,
            "vehicle_status": vehicle.vehicle_status,
            "vehicle_total_price": vehicle.vehicle_total_price or 0,
            "vehicle_hack_up_cost": vehicle.vehicle_hack_up_cost or 0,
            "vehicle_true_cost": vehicle.vehicle_true_cost or 0,
            "vehicle_lifetime_cap" : vehicle.vehicle_lifetime_cap or 0,
            "invoice_number":vehicle.invoice_number,
            "invoice_date": vehicle.invoice_date,
            "entity_name": vehicle.vehicle_entity.entity_name if vehicle.vehicle_entity else "",
            "medallion_number": vehicle.medallions.medallion_number if vehicle.medallions else "",
            "plate_number": vehicle.registrations[0].plate_number if vehicle.registrations else "",
            "vehicle": " ".join(filter(None, [part.strip() if part else None for part in [vehicle.make, vehicle.model, str(vehicle.year) if vehicle.year else None]])),
        },
        "dealer": {
            "dealer_id": vehicle.dealer.id if vehicle.dealer else None,
            "dealer_name": vehicle.dealer.dealer_name if vehicle.dealer else "",
            "dealer_bank_name": vehicle.dealer.dealer_bank_name if vehicle.dealer else "",
            "dealer_bank_account_number": vehicle.dealer.dealer_bank_account_number if vehicle.dealer else "",
        }
    }