"""excepciones custumizada"""

class RaiseExceptionDynamon(Exception):
    """Exception custumizada para raise y omitir errores de pylint"""

class ExceptionDynamon(Exception):
    """Exception custumizada donde podras enviar el mensaje de usuario"""
    def __init__(self, error_context:str, error_user:str) -> None:
        self.error_user= error_user
        super().__init__(error_context)