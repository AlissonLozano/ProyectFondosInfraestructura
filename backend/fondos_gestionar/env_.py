"""Archivo con las constantes de las variables de entorno"""
import os
from enum import Enum

class VariablesEnv(Enum):
    """clase que tiene almacenada las variables
    de entorno"""
    AWS_REGION_SERVICES= "us-east-1"
    TABLE_FONDOS_PRODUCTOS= "fondos_product"
    TABLE_FONDO_SUSER= "fondos_user"
    TABLE_FONDOS_MOVIMIENTOS= "fondos_movimientos"
    TABLE_FONDOS_GESTIONAR_PRODUCTOS= "fondos_gestionar_productos"
