import geopandas as gpd
import requests
import json
import zipfile
import os, shutil
from upload_file import upload_file



def to_shp(url_service: str, service_name: str, project: str):
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
        output_shp = f"{output_folder}/{service_name}.shp"
        #print("Columnas del dataframe: ", gdf.columns)
        #print("Primera fila del dataframe: ", gdf.head(1))
        #gdf.head(1).to_csv(f'{service_name}.csv')

        # Guardar el GeoDataFrame como un shapefile
        gdf.to_file(output_shp, crs=crs, driver='ESRI Shapefile')

        # Comprimir el shapefile en un archivo ZIP
        filename_zip = f"{service_name}.zip"
        zip_path = output_folder + f"/{filename_zip}"

        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.write(output_shp, arcname=f"{service_name}.shp")
            zipf.write(
                f"{output_folder}/{service_name}.shx", arcname=f"{service_name}.shx"
            )
            zipf.write(
                f"{output_folder}/{service_name}.dbf", arcname=f"{service_name}.dbf"
            )
            zipf.write(
                f"{output_folder}/{service_name}.prj", arcname=f"{service_name}.prj"
            )

        print("Archivo ZIP creado en: " + zip_path)

        response_upload = upload_file(zip_path, filename_zip)
        return response_upload
        
    except Exception as e:
        print(e)
        return {"success": False, "message": "Proceso terminado con errores"}


def deleteAllFiles(dir):
    print(f'Eliminando todos los archivos de la carpeta {dir}')
    for files in os.listdir(dir):
        path = os.path.join(dir, files)
        try:
            shutil.rmtree(path)
        except OSError:
            os.remove(path)
    print(f'Archivos removidos correctamente de la carpeta {dir}')

""" to_shp(
    "https://arcgis-pliga-dev.is.arqbs.com/arcgis/rest/services/PIGA/PIGA_ALL_GEODB/MapServer/6/query",
    "drenaje",
    "EPSG:4326"
) 
"""
