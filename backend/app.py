from flask import Flask, jsonify, request
from flask_cors import CORS, cross_origin
import apis
import modelling
import json
import city_data_json

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/testserver',methods=['GET'])
def testserver():
    return jsonify({'msg': 'Server running'})

# @app.route('/updaterating',methods=['POST'])
# def update():
#     user = request.args.get('user')
#     rating = request.args.get('rating')
#     response = NoNo.update(user,rating)
#     return response

# @app.route('/read', methods=['GET'])
# def get_text():
#     location = request.args.get('location')
#     response = apis.read(location)
#     return response

# @app.route('/write', methods=['POST'])
# def write():
#     response = apis.enter()
#     return response

# @app.route('/delete', methods=['DELETE'])
# def delete():
#     apis.delete()
@app.route('/',methods=['GET'])
@cross_origin()
def send():
    response = city_data_json.process()
    return response
    
@app.before_first_request
def process_once():    
    # modelling.process()    
    city_data_json.process()


if __name__ == '__main__':    
    # process_once()
    app.run(debug=True)    