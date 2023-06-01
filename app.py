from flask import request, Flask, jsonify, make_response
from configs import PORT, ENV_SERVICE, APP
from main import to_shp
app = Flask(__name__)

@app.route('/', methods = ['GET'])
def home():
    return "Service up on PROD PORT {}, {} [{}]".format(PORT, APP, ENV_SERVICE)

"""  
Received Payload
{
    "name": "RHS_Canales2",
    "url_service": "https://arcgis-pliga-dev.is.arqbs.com/arcgis/rest/services/PIGA/PIGA_ALL_GEODB/MapServer/8/query",
    "project" : "EPSG:3115"
}"""
@app.route('/api/v1/convert-shp', methods=['POST'])
def receive_data():
    if request.method == 'POST':
        try:
            project = None
            req_body = request.get_json()
            print('Body Recibido: ', req_body)
            if "name" and "url_service" in req_body:
                url_service = req_body["url_service"]
                name_service = req_body["name"]

                if len(url_service) == 0 or len(name_service) == 0:
                    return make_response(jsonify(
                            { "success": False, "body": "La propiedad url_service y/o name está vacía, por favor verificar."}
                        ), 400)

                if "project" in req_body:
                    project = req_body["project"]
    
                response = to_shp(url_service, name_service, project)

                if response['success'] is True:
                    return make_response(jsonify(
                    response
                    ), 200)

                return make_response(jsonify(
                        { "success": False, "body": response}
                    ), 500)
            else:
                return make_response(jsonify(
                        { "success": False, "body": "No se recibieron las propiedades name y/o url_service"}
                    ), 400)

        except Exception as error_response:
            print(error_response)
            return make_response(jsonify({
                'message': 'Ha ocurrido un error interno, por favor intente mas tarde.',
                'trace': str(error_response)
            }), 500)


def run():
    """ with daemon.DaemonContext():
        app.run(host="0.0.0.0", port=PORT, debug=True) """
    app.debug = True
    app.run(host='0.0.0.0', port=PORT)
    
if __name__ == '__main__':
    run()

"""
Ejecutar Flask
python -m  flask run --host=0.0.0.0 --port=5500

"""


