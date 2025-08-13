"""peticion post"""
import os
from typing import Dict, Any
from schema import Use, And, Schema, Optional as OptionalSchema

from utils import ExceptionPeticion
from lib.dynamo_lib import DynamoDBHandler, parse_format_dynamo

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
                ),
                OptionalSchema("fecha"): And(
                    Use(str),
                    error= "'Nombre' debe ser string"
                ),
                OptionalSchema("limit", default= 10): And(
                    Use(int),
                    error= "'limit' debe ser numerico"
                ),
                OptionalSchema("id_movimiento_start"): And(
                    Use(int),
                    error= "'id_movimiento_start' debe ser numerico"
                ),
                OptionalSchema("page", default= 1): And(
                    Use(int),
                    error= "'page' debe ser numerico"
                )
            })
        )
        params:Dict= schema_params.validate(params)
    except Exception as ex:
        raise ExceptionPeticion(
            f"<<SchemaError>> {ex}",
            "Datos de entrada invalidos"
        ) from ex

    instance_dynamo= DynamoDBHandler(
        os.getenv("AWS_REGION_SERVICES")
    )

    #secuencia buscar movimientos
    start_key= None
    if params["page"] >= 1 and params.get("id_movimiento_start") is not None:
        start_key=parse_format_dynamo({
            "id": str(params["id_movimiento_start"]),
            "id_user": params["id_user"]
        })
    list_movimientos_, last_key= instance_dynamo.get_item_paginated(
        os.getenv("TABLE_FONDOS_MOVIMIENTOS"),
        {"id_user": params["id_user"]},
        params["limit"],
        start_key= start_key,
        index_name= "id_user-index"
    )
    id_movimiento_start= None
    if len(list_movimientos_) > 0 and params["page"] > 1:
        indmov:Dict= list_movimientos_[0]
        id_movimiento_start= indmov["id"]
    id_movimiento_last= None
    if last_key is not None:
        id_movimiento_last= last_key.get("id")


    list_movimientos= []
    for indmov_ in list_movimientos_:
        indmov:Dict= indmov_
        producto:Dict= indmov["producto"]
        list_movimientos.append({
            "id_movimiento": int(indmov["id"]),
            "descripcion": indmov["descripcion"],
            "producto":{
                "id":int(producto["id"]),
                "nombre":producto["Nombre"],
                "categoria":producto["Categoria"],
            },
            "fecha": indmov["fecha"],
            "valor": indmov["valor"]
        })

    res= {
        "status":True,
        "codigo":200,
        "msg":"Consulta exitosa",
        "msg_context":"",
        "obj":{
            "movimientos": list_movimientos,
            "id_movimiento_start": id_movimiento_start,
            "id_movimiento_last": id_movimiento_last
        }
    }
    return res
