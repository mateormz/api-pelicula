import boto3
import uuid
import os
import json
import logging

# Configuración de logging para imprimir logs en formato JSON
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Entrada (json)
        logger.info(json.dumps({
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Inicio del procesamiento",
                "event": event
            }
        }))
        
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Generar UUID para la película
        uuidv4 = str(uuid.uuid4())
        
        # Crear item para insertar en DynamoDB
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        # Conectar a DynamoDB y agregar el item
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Salida (json)
        logger.info(json.dumps({
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película registrada correctamente",
                "pelicula": pelicula,
                "response": response
            }
        }))
        
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
    
    except Exception as e:
        # Manejo de errores
        logger.error(json.dumps({
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error al procesar la solicitud",
                "error": str(e),
                "event": event
            }
        }))
        
        return {
            'statusCode': 500,
            'message': 'Error interno del servidor',
            'error_details': str(e)
        }
