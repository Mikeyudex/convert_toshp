import geopandas as gpd
import requests
import json
import uuid
import zipfile
from upload_file import upload_file

def to_geojson(url_service: str, service_name: str, project: str):
    # URL del Feature Service
    url = url_service
    service_name = service_name

    if not url.endswith("/query"):
        url = f"{url}/query"

    # Parámetros de la consulta
    params = {
        "where": "1=1",  # Esto selecciona todos los registros
        "outFields": "*",  # Esto selecciona todos los campos
        "returnGeometry": "true",  # Esto asegura que la geometría esté incluida
        "f": "geojson",  # El formato de salida
        "resultOffset": 0,  # Desplazamiento inicial
        "resultRecordCount": 1000,  # Cantidad de registros por página
    }

    all_features = []  # Lista para almacenar todos los registros

    while True:
        try:
            # Realizar la consulta
            r = requests.get(url, params=params)
            # Convertir la respuesta a un GeoDataFrame
            data = json.loads(r.content)
            # Obtener los registros de la página actual
            features = data["features"]
            # Agregar los registros a la lista
            all_features.extend(features)
            # Verificar si hay más registros para recuperar
            if len(features) < params["resultRecordCount"]:
                break
            # Actualizar el desplazamiento para obtener la siguiente página
            params["resultOffset"] += params["resultRecordCount"]
        except Exception as e:
            print(e)

    try:
        print(f"Total de registros a procesar {len(all_features)}")
        gdf = gpd.GeoDataFrame.from_features(all_features)

        crs = "EPSG:4326" if project == None else project

        gdf.crs = crs

        # Acortar los nombres de columna si exceden los 10 caracteres
        #gdf = gdf.rename(columns=lambda x: f"{x}_p" if x == 'ENABLED' else x )

        output_folder = "temp"
        output_geojson = f"{output_folder}/{service_name}.geojson"

        # Guardar el GeoDataFrame como un shapefile
        gdf.to_file(output_geojson, crs=crs, driver='GeoJSON')

        # Comprimir el shapefile en un archivo ZIP
        filename_zip = f"{service_name}_{uuid.uuid4()}.zip"
        zip_path = output_folder + f"/{filename_zip}"
        
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(output_geojson, arcname=f"{service_name}.geojson")


        print("Archivo ZIP creado en: " + zip_path)

        response_upload = upload_file(zip_path, filename_zip)
        return response_upload
    
    except Exception as e:
        print(e)
        return {"success": False, "message": "Proceso terminado con errores"}


""" to_geojson(
    "https://arcgis-pliga-dev.is.arqbs.com/arcgis/rest/services/PIGA/PIGA_ALL_GEODB/MapServer/6/query",
    "drenaje",
    "EPSG:4326",
    "geojson"
) 
"""
