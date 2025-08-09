"""peticion post"""
import json
import copy
from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime, timedelta
from schema import Use, And, Schema

from utils import ExceptionPeticion, ExceptionCustom
from lib.dynamo_lib import DynamoDBHandler, parse_format_dynamo
from env_ import VariablesEnv

def peticion_post(body:Any)->Dict:
    """peticion para subcribirse al fondo"""
    if body is None:
        raise ExceptionPeticion(
            "<<Raise Custom>> Body vacio",
            "Datos de entrada invalidos"
        )

    try:
        if isinstance(body, str):
            body= json.loads(body)

        if not isinstance(body, dict):
            raise ExceptionCustom("<<raise custom>> formato incorrecto")

        schema_body= Schema({
            "id_user": And(
                Use(int),
                error= "'id_user' debe ser numerico"
            ),
            "id_producto": And(
                Use(int),
                error= "'id_producto' debe ser numerico"
            ),
            "valor": And(
                Use(Decimal),
                error= "'valor' debe ser float"
            ),
        })
        body= schema_body.validate(body)
        id_user= body["id_user"]
        id_producto= body["id_producto"]
        valor= body["valor"]
    except Exception as ex:
        raise ExceptionPeticion(
            f"<<SchemaError>> {ex}",
            "Datos de entrada invalidos"
        ) from ex


    #secuencia buscar gestion del usuario
    instance_dynamo= DynamoDBHandler(
        VariablesEnv.AWS_REGION_SERVICES.value
    )
    list_gestionar:List= instance_dynamo.get_item(
        VariablesEnv.TABLE_FONDOS_GESTIONAR_PRODUCTOS.value,
        {"id": str(id_user)}
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
    gestionar= list_gestionar[0]


    #secuencia buscar producto
    list_prod:List= instance_dynamo.get_item(
        VariablesEnv.TABLE_FONDOS_PRODUCTOS.value,
        {"id": str(id_user)}
    )
    if len(list_prod) == 0:
        raise ExceptionPeticion(
            "<<Raise Custom>> len(list_prod) == 0",
            "No se encontro el producto"
        )
    if len(list_prod) > 1:
        raise ExceptionPeticion(
            "<<Raise Custom>> len(list_prod) > 1",
            "No se encontro el producto"
        )
    producto= list_prod[0]


    #secuencia analizar transaccion
    productos_activos_old:Dict= gestionar["productos_activos"]
    if productos_activos_old.get(f"id_producto_{id_producto}") is not None:
        raise ExceptionPeticion(
            "<<Raise Custom>> productos_activos ya tiene el producto",
            "Usted ya cuenta con este producto"
        )
    producto_actual_name= f"id_producto_{id_producto}"
    producto_actual_value= {
        "valor": valor
    }
    productos_activos_new= copy.deepcopy(productos_activos_old)
    productos_activos_new.update({
        producto_actual_name:producto_actual_value
    })

    cupo_old= Decimal(gestionar["cupo"])
    cupo_new= cupo_old - valor

    if cupo_new < 0:
        raise ExceptionPeticion(
            f"<<Raise Custom>> float(user['Cupo']) - valor = {cupo_new}",
            f"No tiene saldo disponible para vincularse al fondo {producto['Nombre']}"
        )

    #secuencia ejecutar transacción
    datetime_trx= datetime.now()-timedelta(hours=5)
    datetime_trx_= datetime_trx.strftime('%Y-%m-%d %H:%M:%S')
    id_movimiento= int(f"{id_user}{datetime_trx.strftime('%Y%m%d%H%M%S')}")

    instance_dynamo.multiple_operations(
        operations=[
            {
                "Put": {  # Operación de inserción
                    "TableName": VariablesEnv.TABLE_FONDOS_MOVIMIENTOS.value,
                    "Item": {
                        "id": {"S": str(id_movimiento)},
                        "id_user": {"N": str(id_user)},
                        "id_producto": {"N": str(id_producto)},
                        "producto": {"M": parse_format_dynamo(producto)},
                        "valor": {"N": str(valor)},
                        "fecha": {"S": datetime_trx_},
                        "saldo_antes": {"N": str(cupo_old)},
                        "saldo_despues":{"N": str(cupo_new)},
                        "productos_activos": {"M": parse_format_dynamo(productos_activos_new)}
                    },
                    "ConditionExpression": "attribute_not_exists(id)"  # Evita sobreescribir
                }
            },
            {
                "Update": {
                    "TableName": VariablesEnv.TABLE_FONDOS_GESTIONAR_PRODUCTOS.value,
                    "Key": {"id": {"S": str(id_user)}},
                    "UpdateExpression": (
                        "SET cupo = :cupo, fecha = :fecha, id_movimiento = :id_movimiento,"
                        " productos_activos.#producto_activo = :producto_activo"
                    ),
                    "ExpressionAttributeNames":{
                        "#producto_activo": producto_actual_name
                    },
                    "ExpressionAttributeValues": {
                        ":cupo": {"N": str(cupo_new)},
                        ":fecha": {"S": datetime_trx_},
                        ":id_movimiento": {"N": str(id_movimiento)},
                        ":producto_activo": {"M": parse_format_dynamo(producto_actual_value)}
                    },
                    "ConditionExpression": "attribute_exists(id)",  # Falla si no existe
                }
            },
        ]
    )


    res= {
        "status":True,
        "codigo":200,
        "msg":"Suscripción exitosa",
        "msg_context":"",
        "obj":{
            "id_movimiento":id_movimiento,
            "fecha":datetime_trx_,
            "id_producto": id_producto,
            "valor": valor
        }
    }
    return res
