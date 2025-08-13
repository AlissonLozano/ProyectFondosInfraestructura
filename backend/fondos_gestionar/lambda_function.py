"""controlador"""
import json
import logging

from utils import ExceptionPeticion, ExceptionCustom, JSONEncoder
from peticion_post import peticion_post
from peticion_put import peticion_put
from peticion_get import peticion_get

logger_servicio = logging.getLogger("logger_servicio")

def lambda_handler(event, context):
    """controlador del lambda"""
    res= {
        "status":False,
        "codigo":400,
        "msg":"Error de aplicación",
        "msg_context":"Error de aplicación",
        "obj":{}
    }

    try:
        method= event.get("httpMethod")
        if method == "POST":
            body= event.get("body")
            res= peticion_post(body)
        elif method == "PUT":
            params= event.get("queryStringParameters")
            body= event.get("body")
            res= peticion_put(params, body)
        elif method == "GET":
            params= event.get("queryStringParameters")
            res= peticion_get(params)
        else:
            raise ExceptionCustom("<<RaiseCustom>> No se encontro metodo")
        return {
            "statusCode": 200,
            "body": json.dumps(res, cls=JSONEncoder),
            "headers": {
                "Access-Control-Allow-Origin": "*", 
                "Access-Control-Allow-Methods": "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
            }
        }
    except ExceptionPeticion as ex:
        res["codigo"]= 400
        res["msg"]= ex.msg
        res["msg_context"]= f"{ex}"
        logger_servicio.error("Error de peticion %s", res)
        return {
            "statusCode": 400,
            "body": json.dumps(res, cls=JSONEncoder),
            "headers": {
                "Access-Control-Allow-Origin": "*", 
                "Access-Control-Allow-Methods": "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
            }
        }
    except ExceptionCustom as ex:
        res["codigo"]= 404
        res["msg"]= "Metodo no encontrado"
        res["msg_context"]= f"{ex}"
        logger_servicio.error("Error custumizado %s", res)
        return {
            "statusCode": 404,
            "body": json.dumps(res, cls=JSONEncoder),
            "headers": {
                "Access-Control-Allow-Origin": "*", 
                "Access-Control-Allow-Methods": "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
            }
        }
    except Exception as ex:
        res["codigo"]= 400
        res["msg_context"]= f"{ex}"
        logger_servicio.error("Error general %s", res)
        return {
            "statusCode": 400,
            "body": json.dumps(res, cls=JSONEncoder),
            "headers": {
                "Access-Control-Allow-Origin": "*", 
                "Access-Control-Allow-Methods": "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token"
            }
        }
