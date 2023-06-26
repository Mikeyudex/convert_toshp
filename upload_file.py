import base64
import urllib3
import certifi
import json
from configs import URL_SERVICE_S3

def upload_file(path_file: str, filename: str, ext:str = ""):
    try:
        # Leer el archivo ZIP en forma binaria
        with open(path_file, "rb") as file:
            file_data = file.read()

        file_base64 = base64.b64encode(file_data).decode("utf-8")

        # Realizar petición a servicio web externo
        http = urllib3.PoolManager(
            cert_reqs="CERT_NONE",
            ca_certs=certifi.where()
        )
        params = {
            "namespace": "arqocidevsqadem",
            "bucketName": "pliga-bucket",
            "fileName": filename + ext,
            "phoenix": True,
            "urlExpirationTimeHours": "24",
            "fileBase64": file_base64,
        }
        r = http.request(
            "POST",
            URL_SERVICE_S3,
            body=json.dumps(params),
            headers={"Content-Type": "application/json"}
        )
        #print(file_base64)
        if r.status == 200:
            # Convertir la respuesta a un GeoDataFrame
            data = r.json()
            if "accessUri" in data:
                print("Archivo cargado en s3 exitosamente.")
                return {"success": True, "url_file": data["accessUri"]}
            else:
                return {"success": False, "url_file": ""}
        else:
            print(r.data)
            return {"success": False, "url_file": "", "error": str(r.data)}

    except Exception as e:
        print(e)
        return {
            "success": False,
            "url_file": "",
            "error": "Ocurrió un error al intentar subir archivo en S3."
        }
