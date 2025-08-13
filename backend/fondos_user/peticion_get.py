"""peticion post"""
import os
from typing import List, Dict, Any
from schema import Use, And, Schema

from utils import ExceptionPeticion
from lib.dynamo_lib import DynamoDBHandler

def peticion_get(params:Any)->Dict:
    """peticion para listar tus productos"""
    if params is None:
        raise ExceptionPeticion(
            "<<Raise Custom>> params vacio",
            "Datos de entrada invalidos"
        )

    try:
        schema_params= Schema(
            Schema({
                "id_user": And(
                    Use(int),
                    error= "'id_user' debe ser numerico"
                )
            })
        )
        params= schema_params.validate(params)
    except Exception as ex:
        raise ExceptionPeticion(
            f"<<SchemaError>> {ex}",
            "Datos de entrada invalidos"
        ) from ex

    instance_dynamo= DynamoDBHandler(
        os.getenv("AWS_REGION_SERVICES")
    )

    #secuencia buscar gestion del usuario
    list_gestionar:List= instance_dynamo.get_item(
        os.getenv("TABLE_FONDOS_GESTIONAR_PRODUCTOS"),
        {"id": str(params["id_user"])}
    )
    if len(list_gestionar) == 0:
        raise ExceptionPeticion(
            "<<Raise Custom>> len(list_prod) == 0",
            "No se puede gestionar los productos y  el usuario"
        )
    if len(list_gestionar) > 1:
        raise ExceptionPeticion(
            "<<Raise Custom>> len(list_prod) > 1",
            "No se puede gestionar los productos y  el usuario"
        )
    gestion= list_gestionar[0]


    res= {
        "status":True,
        "codigo":200,
        "msg":"Consulta exitosa",
        "msg_context":"",
        "obj":{
            "cupo": gestion["cupo"],
            "cuenta": gestion["cuenta"]
        }
    }
    return res
