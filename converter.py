import pandas as pd
import os
import re
import unidecode
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

def _generate_address_initials(address):
    """Gera as 3 primeiras iniciais do endereço em maiúsculas, ignorando acentos e símbolos."""
    if not isinstance(address, str) or not address.strip():
        return ''
    normalized_address = unidecode.unidecode(address).upper()
    cleaned_address = re.sub(r'[^A-Z\s]', '', normalized_address)
    words = cleaned_address.split()
    initials = [word[0] for word in words if word]
    return "".join(initials[:3])

def process_excel_file(file_path):
    """Processa um arquivo Excel com uma chave única e previne duplicatas na mesma execução."""
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

        ufs_no_arquivo = [str(uf).strip().upper() for uf in df['UF'].dropna().unique()]
        if ufs_no_arquivo:
            Atualizacao.query.filter(Atualizacao.UF.in_(ufs_no_arquivo)).delete(synchronize_session=False)
            db.session.commit()

        processed_ids = set() 
        processed_count = 0
        
        for _, row in df.iterrows():
            if pd.isna(row.get('MATRICULA')) or pd.isna(row.get('UF')) or pd.isna(row.get('ENDERECO')):
                continue

            original_matricula = str(row['MATRICULA']).strip()
            uf = str(row['UF']).strip().upper()
            endereco = str(row['ENDERECO'])

            address_initials = _generate_address_initials(endereco)
            unique_matricula_id = f"{uf}{original_matricula}{address_initials}"
            
            if unique_matricula_id in processed_ids:
                continue
            processed_ids.add(unique_matricula_id)

            imovel_dict = {}
            for col in df.columns:
                if col in Imovel.__table__.columns.keys() and pd.notna(row.get(col)):
                    if col in ['PRECO', 'AVALIACAO']:
                        imovel_dict[col] = _clean_currency(row[col])
                    elif col in ['AREA_PRIVATIVA', 'AREA_DO_TERRENO']:
                        imovel_dict[col] = _clean_area(row[col])
                    else:
                        imovel_dict[col] = str(row[col]).strip()
            
            imovel_dict['MATRICULA'] = unique_matricula_id
            imovel_dict['UF'] = uf

            imovel_existente = db.session.query(Imovel).filter_by(UF=uf, MATRICULA=unique_matricula_id).first()

            if not imovel_existente:
                imovel_dict['Status'] = 'Novo'
                novo_imovel = Imovel(**imovel_dict)
                db.session.add(novo_imovel)
                
                atualizacao = Atualizacao(
                    UF=uf,
                    MATRICULA=unique_matricula_id,
                    Change='Novo',
                    ChangedFields=",".join([k for k in imovel_dict.keys() if k not in ['UF', 'MATRICULA']])
                )
                for field in ['TIPO', 'CIDADE', 'PRECO', 'LINK']:
                    if field in imovel_dict:
                        setattr(atualizacao, field, imovel_dict[field])
                db.session.add(atualizacao)
                processed_count += 1
            else:
                changed_fields = []
                for key, value in imovel_dict.items():
                    if key not in ['UF', 'MATRICULA']: 
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
                        UF=uf,
                        MATRICULA=unique_matricula_id,
                        Change='Atualizado',
                        ChangedFields=",".join(changed_fields)
                    )
                    for field in ['TIPO', 'CIDADE', 'PRECO', 'LINK']:
                        if field in imovel_dict:
                            setattr(atualizacao, field, imovel_dict[field])
                    db.session.add(atualizacao)
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