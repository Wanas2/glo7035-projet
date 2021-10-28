from flask import Flask
import os
import json

flask_env = os.getenv('flask_env')
application = Flask('test_app')

if flask_env == "prod":
    debug = False
    port = 80
elif flask_env == "dev":
    debug = True
    port = 8080
else :
    raise ValueError("Aucun comportement attendu pour l'environnement {}".format(flask_env))

@application.route('/')
def index():
    return("<p>Hello world!</p>")
                           
@application.route('/heartbeat')
def heartbeat():
    return json.dumps({
        "villeChoisie" : "Sherbrooke"
    })

application.run('0.0.0.0',port, debug=debug)