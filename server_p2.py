from flask import Flask
import os
from flask import Flask, jsonify, request
from werkzeug.utils import secure_filename
import mammoth
import json

from project_2 import p2_process

UPLOAD_FOLDER = './uploads'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

if not os.path.exists(UPLOAD_FOLDER):
    os.mkdir(UPLOAD_FOLDER)

def allowed_file(filename, extensions):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in extensions

@app.route('/')
def hello():
    return 'Hello World project 2'


@app.route('/project2', methods=['POST'])
def project2():
    if 'file' not in request.files:
        return jsonify({ 'error': 'No file provided' }), 400

    file = request.files['file']

    if file and allowed_file(file.filename, ['doc', 'docx', 'html', 'xls', 'xlsx']):
        filename = secure_filename(file.filename)
        ext = filename.rsplit('.', 1)[1]
        path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(path)

        if ext == 'doc' or ext == 'docx':
            with open(path + '.html', 'wb') as wf:
                with open(path, 'rb') as f:
                    document = mammoth.convert_to_html(f)
                    wf.write(document.value.encode('utf8'))
                    path = path + '.html'

        result = p2_process(path, verbose=False)

        return jsonify(result)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, threaded=True, port=5026)

