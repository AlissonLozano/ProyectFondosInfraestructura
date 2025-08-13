"""Archivo con las constantes de las variables de entorno"""
import os
from enum import Enum

class VariablesEnv(Enum):
    """clase que tiene almacenada las variables
    de entorno"""

    AWS_REGION_SERVICES= os.getenv("AWS_REGION_SERVICES")
    TABLE_FONDOS_PRODUCTOS= os.getenv("TABLE_FONDOS_PRODUCTOS")
    TABLE_FONDOS_MOVIMIENTOS= os.getenv("TABLE_FONDOS_MOVIMIENTOS")
    TABLE_FONDOS_GESTIONAR_PRODUCTOS= os.getenv("TABLE_FONDOS_GESTIONAR_PRODUCTOS")
