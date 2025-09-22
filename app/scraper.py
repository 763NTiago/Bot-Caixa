import pandas as pd
import requests
from bs4 import BeautifulSoup
import time, os, glob, logging, re
import unidecode
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PASTA_TEMPORARIOS = 'temporarios'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

def _generate_address_initials(address):
    if not isinstance(address, str) or not address.strip():
        return ''
    normalized_address = unidecode.unidecode(address).upper()
    cleaned_address = re.sub(r'[^A-Z\s]', '', normalized_address)
    words = cleaned_address.split()
    initials = [word[0] for word in words if word]
    return "".join(initials[:3])

def parse_valor(texto_valor):
    if not isinstance(texto_valor, str): return 0.0
    try: return float(texto_valor.upper().replace('R$', '').replace('.', '').replace(',', '.').strip())
    except (ValueError, TypeError): return 0.0

def limpar_pasta_temporarios():
    for f in glob.glob(os.path.join(PASTA_TEMPORARIOS, '*')):
        try: os.remove(f)
        except OSError as e: logging.error(f"Erro ao remover {f}: {e.strerror}")

def baixar_listas_por_estados(estados):
    os.makedirs(PASTA_TEMPORARIOS, exist_ok=True)
    if not estados:
        return
    
    caminho_arquivo_antigo = os.path.join(PASTA_TEMPORARIOS, f'{estados[0]}.csv')
    if os.path.exists(caminho_arquivo_antigo):
        try:
            os.remove(caminho_arquivo_antigo)
        except OSError as e:
            logging.error(f"Erro ao remover arquivo antigo {caminho_arquivo_antigo}: {e}")

    for estado in estados:
        yield {"type": "download", "message": f'Baixando lista de {estado}...'}
        url_download = f"https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{estado}.csv"
        try:
            resposta = requests.get(url_download, headers=HEADERS, timeout=300)
            resposta.raise_for_status()
            caminho_arquivo = os.path.join(PASTA_TEMPORARIOS, f'{estado}.csv')
            with open(caminho_arquivo, 'wb') as f:
                f.write(resposta.content)
            yield {"type": "download", "message": f"Lista de {estado} baixada."}
            time.sleep(1) 
        except requests.RequestException as e:
            yield {"type": "error", "message": f"Falha ao baixar lista de {estado}: {e}"}

def extrair_dados_pagina_imovel(url_imovel, modalidade):
    dados_extras = {}
    try:
        time.sleep(0.1)
        response = requests.get(url_imovel, headers=HEADERS, timeout=60)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        if modalidade == 'Leilão':
            preco_tag = soup.select_one('p:-soup-contains("Valor de avaliação") + p, p:-soup-contains("Valor de venda") + p')
            if preco_tag:
                dados_extras['PRECO'] = parse_valor(preco_tag.text)
        datas = soup.find_all('p', class_='data')
        if len(datas) > 1 and modalidade == 'Leilão':
            texto_data = datas[1].text.strip()
            if m := re.search(r'(\d{2}/\d{2}/\d{4}) às (\d{2}h\d{2})', texto_data):
                dados_extras['DATA_DISPUTA'] = f"{m.group(1)} {m.group(2)}"
    except requests.RequestException as e:
        logging.warning(f"Não foi possível acessar a página do imóvel {url_imovel}. Erro: {e}")
    except Exception as e:
        logging.error(f"Erro inesperado ao processar a página {url_imovel}: {e}")
    return dados_extras

def processar_arquivos_csv(arquivos_csv=None):
    if arquivos_csv is None:
        arquivos_csv = glob.glob(os.path.join(PASTA_TEMPORARIOS, '*.csv'))

    todos_dados = []
    for arquivo in arquivos_csv:
        try:
            df = pd.read_csv(arquivo, sep=';', encoding='latin-1', skiprows=2)
            df['UF'] = os.path.basename(arquivo).replace('.csv', '')
            todos_dados.append(df)
        except Exception as e:
            logging.error(f"Erro ao processar o arquivo {arquivo}: {e}")
            
    if not todos_dados:
        yield {"type": "scraping_done", "message": "Nenhum dado para processar.", "data": []}
        return

    df_completo = pd.concat(todos_dados, ignore_index=True)
    df_completo.columns = [col.strip() for col in df_completo.columns]
    
    mapeamento_colunas = {
        'N° do imóvel': 'MATRICULA', 'Matrícula(s)': 'MATRICULA', 'UF': 'UF', 
        'Cidade': 'CIDADE', 'Bairro': 'BAIRRO', 'Endereço': 'ENDERECO',
        'Preço': 'PRECO', 'Valor de avaliação': 'AVALIACAO', 'Desconto': 'DESCONTO',
        'Descrição': 'DESCRICAO', 'Modalidade de venda': 'MODALIDADE', 'Link de acesso': 'LINK'
    }
    
    df_selecionado = df_completo.rename(columns=mapeamento_colunas)
    colunas_necessarias = list(mapeamento_colunas.values())
    df_final = df_selecionado[[col for col in colunas_necessarias if col in df_selecionado.columns]]
    
    dados_processados = []
    total_linhas = len(df_final)
    
    for idx, row in df_final.iterrows():
        yield {"type": "progress", "current_item": idx + 1, "total_items": total_linhas}
        desc_texto = str(row.get('DESCRICAO', '')).lower()
        
        matricula_value = row.get('MATRICULA', '')
        if isinstance(matricula_value, pd.Series):
            matricula_value = matricula_value.iloc[0]

        dados_linha = {
            'UF': row.get('UF'), 'CIDADE': row.get('CIDADE'), 'BAIRRO': row.get('BAIRRO'),
            'ENDERECO': row.get('ENDERECO'), 'PRECO': parse_valor(row.get('PRECO')),
            'AVALIACAO': parse_valor(row.get('AVALIACAO')), 'DESCONTO': row.get('DESCONTO'),
            'MODALIDADE': row.get('MODALIDADE'), 'LINK': row.get('LINK'), 
            'MATRICULA': str(matricula_value).strip() if pd.notna(matricula_value) else '',
            'TIPO': next((t for t in ['casa', 'apartamento', 'terreno'] if t in desc_texto), 'Não especificado'),
            'FGTS': 'Sim' if 'fgts' in desc_texto else 'Não',
            'FINANCIAMENTO': 'Sim' if 'financiamento' in desc_texto else 'Não',
            'Status': 'Novo'  
        }
        if m := re.search(r'(\d+[.,]?\d*)\s*de área privativa', desc_texto):
            dados_linha['AREA_PRIVATIVA'] = f"{m.group(1).replace(',', '.')} m²"
        if m := re.search(r'(\d+[.,]?\d*)\s*de área do terreno', desc_texto):
            dados_linha['AREA_DO_TERRENcO'] = f"{m.group(1).replace(',', '.')} m²"
        if pd.notna(dados_linha['LINK']):
            if extras := extrair_dados_pagina_imovel(dados_linha['LINK'], dados_linha['MODALIDADE']):
                if 'PRECO' in extras:
                    dados_linha['PRECO'] = extras['PRECO']
                dados_linha.update(extras)
        dados_processados.append(dados_linha)

    df_final = pd.DataFrame(dados_processados)
    if not df_final.empty:
        def criar_id_unico(row):
            matricula_original = str(row['MATRICULA']).strip() if pd.notna(row.get('MATRICULA')) and str(row.get('MATRICULA')).strip() else ''
            uf = str(row['UF']).strip().upper()
            iniciais_endereco = _generate_address_initials(row.get('ENDERECO', ''))
            return f"{uf}{matricula_original}{iniciais_endereco}"
        df_final['MATRICULA'] = df_final.apply(criar_id_unico, axis=1)
        
    yield {"type": "scraping_done", "message": "Processamento concluído.", "data": df_final.to_dict('records')}