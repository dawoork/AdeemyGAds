import azure.functions as func
import logging
import os
import json
import google.analytics.data_v1beta
import google.oauth2.service_account  # Asegúrate de que este módulo está importado
import azure.storage.blob


def get_ga4_client():
    credentials_info = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
    # Usar la referencia completa para acceder a service_account
    credentials = google.oauth2.service_account.Credentials.from_service_account_info(credentials_info)
    client = google.analytics.data_v1beta.BetaAnalyticsDataClient(credentials=credentials)
    return client


def upload_to_blob_storage(data):
    blob_service_client = azure.storage.blob.BlobServiceClient.from_connection_string(os.environ['BLOB_CONNECTION_STRING'])
    container_name = os.environ['BLOB_CONTAINER_NAME']
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='ga4_data.json')
    blob_client.upload_blob(data, overwrite=True)


# Define la FunctionApp si es necesario en tu entorno de Azure Functions
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="GAdeemy")
def GAdeemy(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        client = get_ga4_client()
        response = client.run_report(request={
            "property": f"properties/{os.environ['GA4_PROPERTY_ID']}",
            "dimensions": [{"name": "campaignId"}],
            "metrics": [
                {"name": "advertiserAdImpressions"},  # Impresiones
                {"name": "advertiserAdClicks"},  # Clics
                {"name": "advertiserAdCostPerClick"},  # Costo por Clic CPC
                {"name": "advertiserAdCost"},  # Costo total
                {"name": "returnOnAdSpend"},  # Retorno sobre Inversión Publicitaria (ROAS)
                {"name": "keyEvents"},
                {"name": "bounceRate"}
            ],
            "date_ranges": [{"start_date": "2024-01-01", "end_date": "today"}]
        })

        # Procesar la respuesta
        data = []
        for row in response.rows:
            data.append({
                "campaignId": row.dimension_values[0].value,
                "advertiserAdImpressions": row.metric_values[0].value,
                "advertiserAdClicks": row.metric_values[1].value,
                "advertiserAdCostPerClick": row.metric_values[2].value,
                "advertiserAdCost": row.metric_values[3].value,
                "returnOnAdSpend": row.metric_values[4].value,
                "keyEvents": row.metric_values[5].value,
                "bounceRate": row.metric_values[6].value
            })

        # Convertir datos a JSON
        data_json = json.dumps(data)

        # Subir datos a Blob Storage
        upload_to_blob_storage(data_json)
        return func.HttpResponse("Datos subidos a Blob Storage.", status_code=200)

    except Exception as e:
        logging.error(f"Error al obtener datos de GA4: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)
