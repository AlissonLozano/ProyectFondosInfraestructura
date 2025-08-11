"""utils"""
import json
from decimal import Decimal
from datetime import datetime

class JSONEncoder(json.JSONEncoder):
    """Clase custimizada que codifica el json"""

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        elif isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)

class ExceptionCustom(Exception):
    """Excepcion custumizada"""

class ExceptionPeticion(Exception):
    """ExceptionPeticion"""
    def __init__(self, error, msg: str) -> None:
        self.msg= msg
        super().__init__(error)
