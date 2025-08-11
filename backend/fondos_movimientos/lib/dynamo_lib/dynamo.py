import math
from typing import Dict, List, Optional
from decimal import Decimal
import boto3

class DynamoDBHandler():
    """Clase para conectarse a dynamoDB"""

    def __init__(self, region:str) -> None:
        self._dynamodb_resource= boto3.resource('dynamodb', region_name=region)
        self._dynamodb_client= boto3.client('dynamodb', region_name = region)

    def multiple_operations(self, operations:List ):
        """multiple_operations"""
        response = self._dynamodb_client.transact_write_items(
            TransactItems= operations
        )
        return response

    def insert_items(self, table_name:str, data: dict ):
        """insertar informaciòn"""
        table_client= self._dynamodb_resource.Table(table_name)
        response=[]
        response = table_client.put_item(
            Item=data,
            # ReturnValues='ALL_OLD',
        )
        return response

    def get_item(self, table_name:str, searchobj:dict, index_name:Optional[str]=None):
        """consultar item"""
        response=[]

        dic_dynamo= parse_format_dynamo(searchobj)
        dic = {f""":{key}""":val for key ,val in dic_dynamo.items()}
        params={
            "TableName": table_name,
            "KeyConditionExpression":f"""{" AND ".join([f"{x} = :{x}" for x in searchobj.keys()])}""",
            "ExpressionAttributeValues":dic
        }
        if index_name is not None:
            params.update({"IndexName": index_name})

        response =  self._dynamodb_client.query(**params)
        items = response["Items"]
        responseitems=[]
        for item in items:
            parse_format_python(item)
            responseitems.append(parse_format_python(item))

        return  responseitems


    def get_item_paginated(self,
        table_name:str,
        searchobj:dict,
        limit:int,
        start_key:Optional[str]=None,
        index_name:Optional[str]=None
    ):
        """consultar item"""
        response=[]
        dic_dynamo= parse_format_dynamo(searchobj)
        dic = {f""":{key}""":val for key ,val in dic_dynamo.items()}
        params={
            "TableName": table_name,
            "KeyConditionExpression":f"""{" AND ".join([f"{x} = :{x}" for x in searchobj.keys()])}""",
            "ExpressionAttributeValues":dic,
            "Limit": limit
        }
        if index_name is not None:
            params.update({"IndexName": index_name})
        if start_key is not None:
            params['ExclusiveStartKey']= start_key

        response =  self._dynamodb_client.query(**params)

        last_key= None
        if "LastEvaluatedKey" in response:
            last_key= parse_format_python(response["LastEvaluatedKey"])

        items = response["Items"]
        responseitems=[]
        for item in items:
            parse_format_python(item)
            responseitems.append(parse_format_python(item))

        return  responseitems, last_key


    def update_item(self, table_name:str, keyparam:dict, updatevalues:dict):
        """modificar item"""
        table_client= self._dynamodb_resource.Table(table_name)
        response=[]
        dic = {f""":{key}""":val for key ,val in updatevalues.items()}
        response= table_client.update_item(
            Key=keyparam,
            UpdateExpression=f"""SET {", ".join([f"{x} = :{x}" for x in updatevalues.keys()])}""",
            ExpressionAttributeValues=dic,
            ReturnValues="UPDATED_NEW"
        )
        return response


    def update_item_column_map(self,
        table_name:str,
        keyparam:dict,
        columna:str,
        name_item:str,
        data:dict
    ):
        """consultar item"""
        response=[]

        keyparam= parse_format_dynamo(keyparam)
        data= parse_format_dynamo(data)
        response= self._dynamodb_client.update_item(
            TableName=table_name,
            Key=keyparam,
            UpdateExpression=f"SET {columna}.#campo = :data",
            ExpressionAttributeNames={
                "#campo": name_item
            },
            ExpressionAttributeValues={
                ":data": {
                    "M": data
                }
            },
            ReturnValues="UPDATED_NEW"
        )
        return response

    def get_all_items(self, table_name:str, searchobj:Optional[Dict]=None):
        """consultar toda la información"""
        table_client= self._dynamodb_resource.Table(table_name)

        expression= None
        attribute_value= None
        if searchobj is not None:
            if len(searchobj) > 0:
                expression= f"{', '.join([f'{x} = :{x}' for x in searchobj.keys()])}"
                attribute_value = {f""":{key}""":val for key ,val in searchobj.items()}

        response=[]
        if expression is None:
            response = table_client.scan()
        else:
            response = table_client.scan(
                FilterExpression= expression,
                ExpressionAttributeValues=attribute_value
            )
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            if expression is None:
                response = table_client.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
            else:
                response = table_client.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey'],
                    FilterExpression= expression,
                    ExpressionAttributeValues=attribute_value
                )
            data.extend(response['Items'])

        return data

    def get_search_base(self,
        table_name:str,
        expression:Optional[str]= None,
        attribute_name:Optional[Dict]= None,
        attribute_value:Optional[Dict]= None
    ):
        """consultar toda la información"""
        table_client= self._dynamodb_resource.Table(table_name)

        response=[]
        if expression is None:
            response = table_client.scan()
        else:
            if attribute_name is None:
                response = table_client.scan(
                    FilterExpression= expression,
                    ExpressionAttributeValues= attribute_value
                )
            else:
                response = table_client.scan(
                    FilterExpression= expression,
                    ExpressionAttributeNames= attribute_name,
                    ExpressionAttributeValues= attribute_value
                )
        data = response['Items']

        while 'LastEvaluatedKey' in response:
            if expression is None:
                response = table_client.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
            else:
                if attribute_name is None:
                    response = table_client.scan(
                        ExclusiveStartKey=response['LastEvaluatedKey'],
                        FilterExpression= expression,
                        ExpressionAttributeValues= attribute_value
                    )
                else:
                    response = table_client.scan(
                        ExclusiveStartKey=response['LastEvaluatedKey'],
                        FilterExpression= expression,
                        ExpressionAttributeNames= attribute_name,
                        ExpressionAttributeValues= attribute_value
                    )
            data.extend(response['Items'])

        return data


def parse_format_dynamo(data:Dict)->Dict:
    """convierte a formato dynamon"""
    data_new= {}
    for key_ind, value_ind in data.items():
        if isinstance(value_ind, str):
            data_new.update({key_ind: {"S":value_ind}})
        elif isinstance(value_ind, (int, Decimal)):
            data_new.update({key_ind: {"N": str(value_ind)}})
        elif isinstance(value_ind, dict):
            data_new.update({key_ind: {"M": parse_format_dynamo(value_ind)}})

    return data_new

def parse_format_python(item:Dict)->Dict:
    """convierte a formato python"""
    tempdata = {}
    for subitem, subvalues_ in item.items():
        subvalues:Dict=subvalues_
        if subvalues.get("M"):
            tempdata[subitem]= parse_format_python(subvalues.get("M"))
        else:
            for x in item[subitem].keys():
                tempdata[subitem]=item[subitem][x]

    return tempdata

