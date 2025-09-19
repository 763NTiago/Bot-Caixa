from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import logging
import os
import sys 

db = SQLAlchemy()

def create_app():
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

    instance_path = os.path.join(base_dir, 'instance')

    app = Flask(__name__, instance_path=instance_path)

    os.makedirs(app.instance_path, exist_ok=True)

    db_path = os.path.join(app.instance_path, 'imoveis.db')
    app.config['SQLALCHEMY_DATABASE_URI'] = f'sqlite:///{db_path}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SECRET_KEY'] = 'uma_chave_secreta_muito_segura'

    db.init_app(app)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    with app.app_context():
        from . import models
        db.create_all()

        from . import routes
        app.register_blueprint(routes.bp)

    return app