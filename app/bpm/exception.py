## app/bpm/exception.py

class CaseStopException(Exception):
    """Exception to be raised when there are no further steps to move"""
    def __init__(self, message="Case has no further steps"):
        self.message = message
        super().__init__(self.message)