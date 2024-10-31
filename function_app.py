import azure.functions as func
import logging
import os
import json
import google.analytics.data_v1beta
import google.oauth2.service_account
import azure.storage.blob
from datetime import datetime

# Función para autenticarse y crear el cliente de GA4
def get_ga4_client():
    credentials_info = json.loads(os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'])
    credentials = google.oauth2.service_account.Credentials.from_service_account_info(credentials_info)
    client = google.analytics.data_v1beta.BetaAnalyticsDataClient(credentials=credentials)
    return client

# Función para subir los datos al Blob Storage
def upload_to_blob_storage(data):
    blob_service_client = azure.storage.blob.BlobServiceClient.from_connection_string(os.environ['BLOB_CONNECTION_STRING'])
    container_name = os.environ['BLOB_CONTAINER_NAME']
    blob_client = blob_service_client.get_blob_client(container=container_name, blob='ga4_data.json')
    blob_client.upload_blob(data, overwrite=True)

# Inicialización de la FunctionApp con Timer Trigger
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name(name="GAdeemyTimerTrigger")
@app.schedule(schedule="0 0 */4 * * *", arg_name="timer", run_on_startup=True, use_monitor=True)
def GAdeemy(timer: func.TimerRequest) -> None:
    logging.info("Timer trigger function ran at %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    try:
        client = get_ga4_client()

        # Primera solicitud para obtener las métricas publicitarias sin eventos
        metrics_response = client.run_report(request={
            "property": f"properties/{os.environ['GA4_PROPERTY_ID']}",
            "dimensions": [{"name": "campaignId"}],
            "metrics": [
                {"name": "advertiserAdImpressions"},  # Impresiones
                {"name": "advertiserAdClicks"},       # Clics
                {"name": "advertiserAdCostPerClick"}, # CPC
                {"name": "advertiserAdCost"},         # Costo total
                {"name": "returnOnAdSpend"},          # ROAS
                {"name": "keyEvents"},                # Eventos clave
                {"name": "bounceRate"}                # Tasa de rebote
            ],
            "date_ranges": [{"start_date": "2024-01-01", "end_date": "today"}]
        })

        # Procesar las métricas publicitarias
        data = []
        for row in metrics_response.rows:
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

        # Segunda solicitud para obtener el conteo de eventos específicos
        events_response = client.run_report(request={
            "property": f"properties/{os.environ['GA4_PROPERTY_ID']}",
            "dimensions": [{"name": "eventName"}],
            "metrics": [{"name": "eventCount"}],
            "date_ranges": [{"start_date": "2024-01-01", "end_date": "today"}],
            "dimension_filter": {
                "filter": {
                    "field_name": "eventName",
                    "in_list_filter": {
                        "values": ["invitee_select_day", "invitee_select_time", "invitee_meeting_scheduled"]
                    }
                }
            }
        })

        # Añadir los eventos específicos a las métricas al mismo nivel
        for row in events_response.rows:
            event_name = row.dimension_values[0].value
            event_count = row.metric_values[0].value
            
            for item in data:
                item[event_name] = event_count  # Añadir cada evento al mismo nivel

        # Convertir datos a JSON
        data_json = json.dumps(data)

        # Subir datos a Blob Storage
        upload_to_blob_storage(data_json)
        logging.info("Datos de métricas y eventos subidos a Blob Storage exitosamente.")

    except Exception as e:
        logging.error(f"Error al obtener datos de GA4: {e}")
