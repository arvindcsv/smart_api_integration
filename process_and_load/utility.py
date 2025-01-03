
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
import json


def dynamodb_batch_serializer(batch_of_images):
    """ To serialize list(batch) of JSONS into dynamoDB JSON"""
    serialized_batch = []
    for image in batch_of_images:
        serialized_batch.append(
            TypeSerializer().serialize(image)['M']
        )

    return serialized_batch


def dynamodb_batch_deserializer(serialized_batch):
    """ To deserialize a list(batch) of dynamoDB JSONS"""
    deserialized_batch = []
    for image in serialized_batch:
        deserialized_batch.append(
            json.loads(
                json.dumps(TypeDeserializer().deserialize({'M': image}), default=str)
            )
        )

    return deserialized_batch
