# tests/test_function_app.py

import os
import pytest
import json
from unittest.mock import patch, MagicMock
from function_app import get_ga4_client, upload_to_blob_storage, GAdeemy

# Datos de ejemplo para simular la respuesta de Google Analytics 4
MOCK_GA4_RESPONSE = {
    "rows": [
        {
            "dimension_values": [{"value": "test_campaign"}],
            "metric_values": [
                {"value": "1000"},  # Impresiones
                {"value": "50"},    # Clics
                {"value": "0.5"},   # CPC
                {"value": "25"},    # Costo total
                {"value": "2.0"}    # ROAS
            ]
        }
    ]
}

# Configuración de pruebas para variables de entorno necesarias
@pytest.fixture(scope="module", autouse=True)
def setup_env():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS_JSON'] = json.dumps({
        "type": "service_account",
        "project_id": "test_project",
        # Simulación de datos adicionales para la cuenta de servicio
    })
    os.environ['BLOB_CONNECTION_STRING'] = "test_connection_string"
    os.environ['BLOB_CONTAINER_NAME'] = "test_container"
    os.environ['GA4_PROPERTY_ID'] = "test_property_id"

# Prueba para verificar la importación de dependencias
def test_imports():
    try:
        import azure.functions
        import google.analytics.data_v1beta
        import azure.storage.blob
    except ImportError as e:
        pytest.fail(f"Dependency import failed: {e}")

# Prueba de la función get_ga4_client
@patch('function_app.BetaAnalyticsDataClient')
def test_get_ga4_client(mock_beta_client):
    client_instance = mock_beta_client.return_value
    client = get_ga4_client()
    assert client == client_instance
    mock_beta_client.assert_called_once()

# Prueba de la función upload_to_blob_storage
@patch('function_app.BlobServiceClient')
def test_upload_to_blob_storage(mock_blob_service_client):
    # Simular la subida a Blob Storage
    mock_blob_client = MagicMock()
    mock_blob_service_client.from_connection_string.return_value.get_blob_client.return_value = mock_blob_client

    # Llamar a la función
    upload_to_blob_storage('{"test": "data"}')
    
    # Verificar que la función haya sido llamada correctamente
    mock_blob_client.upload_blob.assert_called_once_with('{"test": "data"}', overwrite=True)

# Prueba de la función HTTP GAdeemy
@patch('function_app.get_ga4_client')
@patch('function_app.upload_to_blob_storage')
def test_GAdeemy(mock_upload_to_blob_storage, mock_get_ga4_client):
    # Simulación del cliente GA4
    mock_client = MagicMock()
    mock_get_ga4_client.return_value = mock_client

    # Simular la respuesta de GA4
    mock_client.run_report.return_value.rows = MOCK_GA4_RESPONSE['rows']

    # Simular la solicitud HTTP (no se usa un objeto real en esta prueba)
    mock_request = MagicMock()

    # Llamar a la función GAdeemy
    response = GAdeemy(mock_request)

    # Verificar que los datos se subieron correctamente y la respuesta fue la esperada
    mock_upload_to_blob_storage.assert_called_once()
    assert response.status_code == 200
    assert response.get_body().decode() == "Datos subidos a Blob Storage."
