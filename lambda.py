import io
import json
from pprint import pprint
from urllib.request import urlopen, Request
import boto3
from boto3.dynamodb.types import TypeSerializer
import multipart as mp
from base64 import b64decode
from datetime import datetime
from botocore.exceptions import ClientError
from decimal import Decimal


API_TOKEN = "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"

headers = {"Authorization": f"Bearer {API_TOKEN}"}

APIS = {
      "fb":"https://api-inference.huggingface.co/models/facebook/detr-resnet-50",
      "hustvl": "https://api-inference.huggingface.co/models/hustvl/yolos-tiny",
      "ms": "https://api-inference.huggingface.co/models/microsoft/resnet-50"
    }
DYNAMODB_TABLE = 'hugged-faces'
s3_client = boto3.client("s3")


def query_image(file, model):
    file.seek(0)
    http_request = Request(APIS[model], data=file, headers=headers)
    with urlopen(http_request) as response:
        result = response.read().decode()
        return result


def save_to_dynamodb(image_name, model_name, data):
    dynamodb = boto3.client('dynamodb')
    timestamp = datetime.utcnow().replace(microsecond=0).isoformat()
    serializer = TypeSerializer()
    dynamo_serialized_data = []
    for item in json.loads(data, parse_float=Decimal):
        dynamo_serialized_item = {'M':{}}
        for key, value in item.items():
            if isinstance(value, (float, Decimal)):
                dynamo_serialized_item['M'][key] = {'N': str(value)}
            elif isinstance(value, dict):
                dynamo_serialized_item['M'][key] = {
                'M': {k: serializer.serialize(v)
                        for k, v in value.items()}
                }
            else:
                dynamo_serialized_item['M'][key] = {'S': str(value)}
        dynamo_serialized_data.append(dynamo_serialized_item)

    data_ready_to_be_saved = {
        'image': {
        'S': image_name
        },
        'RawAiJson': {
        'S': data
        },
        'AiData': {
        'L': dynamo_serialized_data
        },
        'Model': {
        'S': model_name
        },
        'UploadDate': {
        'S': timestamp
        }
    }
    print(json.dumps(data_ready_to_be_saved))

    try:
        dynamodb.put_item(TableName=DYNAMODB_TABLE, Item=data_ready_to_be_saved)
        pass
    except ClientError as e:
        print(e.response['Error']['Message'])
        raise e
    return


def lambda_handler(event, context):
    print(event)
    data = b64decode(event.get("body"))
    s = data.split("\r".encode())[0][2:]
    data = mp.MultipartParser(io.BytesIO(data),s)
    for p in data.parts():
        print(p.name)
    model = data.parts()[0].value
    image = data.parts()[1]
    returnJson = len(data.parts())==3
    
    fileToSave = io.BytesIO()
    fileToSave.write(image.raw)
    fileToSave.seek(0)
    fileToQuery = io.BytesIO()
    fileToQuery.write(image.raw)
    fileToQuery.seek(0)

    s3_client.upload_fileobj(fileToSave, "hugging-photo-bucket", image.filename)

    result = query_image(fileToQuery, model)

    result_key = f"{model}.{image.filename}.json"
    result_file = io.BytesIO()
    result_file.write(result.encode("utf-8"))
    result_file.seek(0)

    s3_client.upload_fileobj(result_file, "hugging-photo-bucket", result_key)

    save_to_dynamodb(image.filename, model, result)

    if returnJson:
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": result
        }

    return {"statusCode": 200, "body": json.dumps("Saved!")}