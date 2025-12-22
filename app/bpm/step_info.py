## app/bpm/step_info.py

# Standard library imports
import os
import importlib
from typing import Callable

# Local imports
from app.utils.logger import get_logger

logger = get_logger(__name__)
STEP_REGISTRY = {}

def step(step_id: str, name: str, operation: str):
    """Decorator for registering steps in the step registry"""
    def decorator(func: Callable):
        if step_id in STEP_REGISTRY:
            raise ValueError(f"Step {step_id} is already registered ")

        STEP_REGISTRY[f"{step_id}-{operation}"] = {
            "name": name, "function": func}
        return func
    return decorator

def import_bpm_flows():
    """Method for importing all the bpm flows"""
    current_dir = os.path.dirname(os.path.dirname(__file__))
    bpm_flows_dir = os.path.join(current_dir, "bpm_flows")
    for flow_dir in os.listdir(bpm_flows_dir):
        flow_dir_path = os.path.join(bpm_flows_dir, flow_dir)
        if os.path.isdir(flow_dir_path) and "flows.py" in os.listdir(flow_dir_path):
            module_name = f"app.bpm_flows.{flow_dir}.flows"
            logger.info("Importing flow", flow_name=flow_dir, module=module_name)
            importlib.import_module(module_name)