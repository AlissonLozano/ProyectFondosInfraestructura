"""peticion post"""
import math
import copy
from typing import List, Dict, Any
from schema import Use, And, Schema, Optional as OptionalSchema

from utils import ExceptionPeticion
from lib.dynamo_lib import DynamoDBHandler
from env_ import VariablesEnv

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
                "tipo": And(
                    Use(str),
                    lambda x: x in ["activos", "disponibles"],
                    error= "'tipo' debe ser str [activos, disponibles]"
                ),
                OptionalSchema("Nombre"): And(
                    Use(str),
                    error= "'Nombre' debe ser string"
                ),
                OptionalSchema("Categoria"): And(
                    Use(str),
                    error= "'Categoria' debe ser string"
                ),
                OptionalSchema("limit", default= 10): And(
                    Use(int),
                    error= "'limit' debe ser numerico"
                ),
                OptionalSchema("page", default= 1): And(
                    Use(int),
                    error= "'page' debe ser numerico"
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
        VariablesEnv.AWS_REGION_SERVICES.value
    )

    #secuencia buscar gestion del usuario
    list_gestionar:List= instance_dynamo.get_item(
        VariablesEnv.TABLE_FONDOS_GESTIONAR_PRODUCTOS.value,
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
    gestionar= list_gestionar[0]
    productos_activos_bd= gestionar["productos_activos"]

    #secuencia buscar productos
    list_prods_bd= search_table_product(params)

    prods= {}
    for index, prodind_ in enumerate(list_prods_bd):
        prodind:Dict= prodind_
        prods.update({
            f"id_producto_{prodind['id']}":{
                "position": index,
                "data":prodind
            }
        })


    #secuencia ajustar productos
    productos_activos=[]
    productos_disponibles= copy.deepcopy(list_prods_bd)
    for keyprod in productos_activos_bd:
        if prods.get(keyprod) is None:
            continue
        prodind= prods.get(keyprod)
        productos_activos.append(prodind["data"])
        productos_disponibles.pop(prodind["position"])

    if params["tipo"] == "activos":
        productos_all= copy.deepcopy(productos_activos)
    else:
        productos_all= copy.deepcopy(productos_disponibles)

    limit= params["limit"]
    page= params["page"]
    pages_max= math.ceil(len(productos_all)/limit)
    page_next= True if  page < pages_max else False
    productos= productos_all[limit*(page-1):limit*page]

    res= {
        "status":True,
        "codigo":200,
        "msg":"Consulta exitosa",
        "msg_context":"",
        "obj":{
            "productos": productos,
            "page_next": page_next
        }
    }
    return res


def search_table_product(params:Dict):
    """funcion para ser la busqueda especifica"""
    instance_dynamo= DynamoDBHandler(
        VariablesEnv.AWS_REGION_SERVICES.value
    )

    expression_vector= []
    attribute_value= {}
    attribute_name= {}
    if params.get("Nombre") is not None:
        expression_vector.append("contains(Nombre, :Nombre)")
        attribute_value.update({":Nombre":str(params["Nombre"]).upper()})
    if params.get("Categoria") is not None:
        expression_vector.append("#cat = :Categoria")
        attribute_value.update({":Categoria":str(params["Categoria"])})
        attribute_name.update({"#cat":"Categoria"})

    expression= None if len(expression_vector) == 0 else " AND ".join(expression_vector)

    list_prods_bd:List= instance_dynamo.get_search_base(
        VariablesEnv.TABLE_FONDOS_PRODUCTOS.value,
        expression= expression,
        attribute_name=  None if len(attribute_name) == 0 else attribute_name,
        attribute_value= None if len(attribute_value) == 0 else attribute_value
    )

    return list_prods_bd