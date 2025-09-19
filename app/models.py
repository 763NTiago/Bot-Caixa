from app import db
from sqlalchemy.sql import func

class Imovel(db.Model):
    __tablename__ = 'imoveis'
    MATRICULA = db.Column(db.String, primary_key=True)
    TIPO = db.Column(db.String)
    UF = db.Column(db.String, index=True)
    CIDADE = db.Column(db.String)
    BAIRRO = db.Column(db.String)
    ENDERECO = db.Column(db.String)
    AREA_PRIVATIVA = db.Column(db.String)
    AREA_DO_TERRENO = db.Column(db.String)
    DATA_DISPUTA = db.Column(db.String)
    DESCONTO = db.Column(db.String)
    PRECO = db.Column(db.Float)
    AVALIACAO = db.Column(db.Float)
    LINK = db.Column(db.String)
    MODALIDADE = db.Column(db.String)
    CONDOMINIO = db.Column(db.String)
    FGTS = db.Column(db.String)
    FINANCIAMENTO = db.Column(db.String)
    Status = db.Column(db.String, index=True)
    updated_at = db.Column(db.DateTime, server_default=func.now(), onupdate=func.now())

    def to_dict(self):
        """Converte o objeto para dicionário, excluindo campos internos."""
        result = {}
        for column in self.__table__.columns:
            if column.name != 'updated_at': 
                value = getattr(self, column.name)
                
                if value is None:
                    if column.name in ['PRECO', 'AVALIACAO']:
                        result[column.name] = 0.0
                    else:
                        result[column.name] = ''
                else:
                    result[column.name] = value
                    
        return result

class Atualizacao(db.Model):
    __tablename__ = 'atualizacoes'
    MATRICULA = db.Column(db.String, primary_key=True)
    TIPO = db.Column(db.String)
    UF = db.Column(db.String)
    CIDADE = db.Column(db.String)
    PRECO = db.Column(db.Float)
    LINK = db.Column(db.String)
    Change = db.Column(db.String)
    ChangedFields = db.Column(db.String)

    def to_dict(self):
        """Converte o objeto para dicionário."""
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            result[column.name] = value if value is not None else ''
        return result