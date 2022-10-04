import logging

from flask import render_template
import connexion
from flask_cors import CORS, cross_origin

# Create the application instance
app = connexion.App(__name__, specification_dir='./')

# Read the swagger.yml file to configure the endpoints
app.add_api('swagger.yml')

# Enable CORS support
CORS(app.app)

# Create a URL route in our application for "/"
@app.route('/')
@cross_origin()
def home():
    """
    This function just responds to the browser ULR
    localhost:5000/
    :return:        the rendered template 'home.html'
    """
    return render_template('home.html')

# If we're running in stand alone mode, run the application
if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    app.run(host='0.0.0.0', port=5000, debug=True)