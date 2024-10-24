# tests/test_dependencies.py

# Ejemplo de prueba básica para verificar que las dependencias están instaladas correctamente

def test_imports():
    # Intenta importar todas las dependencias críticas para verificar si están instaladas correctamente
    try:
        import azure.functions
        import azure.core
        import azure.storage.blob
        import google.analytics.data_v1beta
        import requests
        import grpc
        # Agrega más importaciones según lo necesites
    except ImportError as e:
        # Falla la prueba si hay un error de importación
        assert False, f"Dependency import failed: {e}"

    # Si todas las importaciones funcionan, la prueba pasa
    assert True
