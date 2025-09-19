import webbrowser
import threading
import os
from app import create_app, db

app = create_app()

def abrir_navegador():
    """Função para abrir o navegador na página inicial da aplicação."""
    webbrowser.open_new('http://127.0.0.1:5000/')

if __name__ == '__main__':
    os.makedirs('temporarios', exist_ok=True)
    
    with app.app_context():
        db.create_all()
    thread_navegador = threading.Timer(1.25, abrir_navegador)
    thread_navegador.daemon = True 
    thread_navegador.start()
    app.run(port=5000, debug=False)