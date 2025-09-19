import pandas as pd
import os
from app import db
from app.models import Imovel, Atualizacao
import logging

logging.basicConfig(level=logging.INFO)

def _clean_currency(value):
    """Limpa e converte valores monetários para float."""
    if not isinstance(value, str):
        return float(value) if pd.notna(value) else 0.0
    try:
        cleaned = value.upper().replace('R$', '').replace('.', '').replace(',', '.').strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0

def _clean_area(value):
    """Limpa e converte valores de área para float."""
    if not isinstance(value, str):
        return float(value) if pd.notna(value) else None
    try:
        cleaned = value.lower().replace('m²', '').strip().replace('.', '').replace(',', '.')
        return float(cleaned) if cleaned else None
    except (ValueError, TypeError):
        return None

def process_excel_file(file_path):
    """Processa um arquivo Excel com a lógica de status detalhada."""
    try:
        df = pd.read_excel(file_path)
        column_mapping = {
            'MATRICULA': 'MATRICULA', 'TIPO': 'TIPO', 'UF': 'UF', 'CIDADE': 'CIDADE',
            'BAIRRO': 'BAIRRO', 'ENDERECO': 'ENDERECO', 'ENDEREÇO': 'ENDERECO',
            'Área privativa': 'AREA_PRIVATIVA', 'Area_privativa': 'AREA_PRIVATIVA', 'AREA_PRIVATIVA': 'AREA_PRIVATIVA',
            'Área do terreno': 'AREA_DO_TERRENO', 'Area_do_terreno': 'AREA_DO_TERRENO', 'AREA_DO_TERRENO': 'AREA_DO_TERRENO',
            'DATA DISPUTA': 'DATA_DISPUTA', 'DESCONTO': 'DESCONTO', 'PRECO': 'PRECO', 'PREÇO': 'PRECO',
            'AVALIACAO': 'AVALIACAO', 'AVALIAÇÃO': 'AVALIACAO', 'LINK': 'LINK', 'MODALIDADE': 'MODALIDADE',
            'CONDOMINIO': 'CONDOMINIO', 'CONDOMÍNIO': 'CONDOMINIO', 'FGTS': 'FGTS', 'FINANCIAMENTO': 'FINANCIAMENTO'
        }
        df.rename(columns=column_mapping, inplace=True)

        db.session.query(Atualizacao).delete()
        db.session.commit()

        processed_count = 0
        for _, row in df.iterrows():
            if pd.isna(row.get('MATRICULA')):
                continue

            matricula = str(row['MATRICULA'])
            imovel_dict = {}
            for col in df.columns:
                if col in Imovel.__table__.columns.keys() and pd.notna(row.get(col)):
                    if col in ['PRECO', 'AVALIACAO']:
                        imovel_dict[col] = _clean_currency(row[col])
                    elif col in ['AREA_PRIVATIVA', 'AREA_DO_TERRENO']:
                        imovel_dict[col] = _clean_area(row[col])
                    else:
                        imovel_dict[col] = str(row[col]).strip()

            imovel_existente = db.session.get(Imovel, matricula)

            if not imovel_existente:
                imovel_dict['Status'] = 'Novo'
                novo_imovel = Imovel(**imovel_dict)
                db.session.add(novo_imovel)
                
                atualizacao = Atualizacao(
                    MATRICULA=matricula, Change='Novo', ChangedFields=",".join(imovel_dict.keys()))
                db.session.add(atualizacao)
                processed_count += 1
            else:
                changed_fields = []
                for key, value in imovel_dict.items():
                    current_value = getattr(imovel_existente, key)
                    if key in ['AREA_PRIVATIVA', 'AREA_DO_TERRENO']:
                        current_value = current_value if current_value is not None else None
                        value = value if value is not None else None
                    
                    if str(current_value) != str(value):
                        changed_fields.append(key)
                
                if changed_fields:
                    for key in changed_fields:
                        setattr(imovel_existente, key, imovel_dict[key])
                    imovel_existente.Status = 'Atualizado'
                    
                    atualizacao = Atualizacao(
                        MATRICULA=matricula, Change='Atualizado', ChangedFields=",".join(changed_fields))
                    db.session.merge(atualizacao)
                    processed_count += 1
                else:
                    if imovel_existente.Status == 'Novo':
                        imovel_existente.Status = 'Existente'

        db.session.commit()
        
        logging.info(f"Arquivo processado. {processed_count} imóveis novos/atualizados de {len(df)} linhas lidas.")
        return True, f"Sucesso! {processed_count} imóveis foram adicionados ou atualizados."
        
    except Exception as e:
        db.session.rollback()
        logging.error(f"Erro fatal ao processar arquivo Excel: {e}", exc_info=True)
        return False, f"Erro fatal ao processar arquivo: {str(e)}"

def convert_excel_to_db(file_path):
    if not os.path.exists(file_path):
        return False, "Arquivo não encontrado."
    return process_excel_file(file_path)