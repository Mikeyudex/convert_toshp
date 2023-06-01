import base64
import requests
import json
from configs import URL_SERVICE_S3
import shutil

def upload_file(path_zip: str, filename: str):
    try:
        # Leer el archivo ZIP en forma binaria
        with open(path_zip, "rb") as file:
            zip_data = file.read()
        base64_data = base64.b64encode(zip_data).decode("utf-8")
        params = {
            "namespace": "arqocidevsqadem",
            "bucketName": "pliga-bucket",
            "fileName": filename,
            "phoenix": "true",
            "urlExpirationTimeHours": "24",
            "fileBase64": base64_data,
        }

        return {"success": True, "file_base64": base64_data}

        # Realizar petición a servicio web externo
        r = requests.post(URL_SERVICE_S3, params=params)
        if r.status_code == 200:
            # Convertir la respuesta a un GeoDataFrame
            data = json.loads(r.content)
            try:
                print('== Eliminando archivo Zip.. ==')
                shutil.rmtree(path_zip, ignore_errors=True)
                print('==  Zip Eliminado ==')
            except Exception as e:
                print("Ocurrió un error intentando eliminar el archivo Zip")
                print(e)
            if "accessUri" in data:
                return {"success": True, "url_file": data["accessUri"]}
        else:
            return {"success": False, "url_file": "", "error": r.text}
    except Exception as e:
        return {
            "success": False,
            "file_base64": "",
            "error": "Ocurrió un error al intentar subir archivo en S3.",
        }
