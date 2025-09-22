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
        yield {"type": "download_start", "state": estado, "message": f'Baixando lista de {estado}...'}
        url_download = f"https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{estado}.csv"
        try:
            resposta = requests.get(url_download, headers=HEADERS, timeout=300)
            resposta.raise_for_status()
            caminho_arquivo = os.path.join(PASTA_TEMPORARIOS, f'{estado}.csv')
            with open(caminho_arquivo, 'wb') as f:
                f.write(resposta.content)
            yield {"type": "download_completed", "state": estado, "message": f"Download de {estado} concluído"}
            time.sleep(1) 
        except requests.RequestException as e:
            yield {"type": "error", "message": f"Falha ao baixar lista de {estado}: {e}"}

def extrair_dados_pagina_imovel(url_imovel, modalidade):
    dados_extras = {}
    try:
        time.sleep(0.2)
        response = requests.get(url_imovel, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        texto_pagina = soup.get_text(separator='\n', strip=True)
        texto_lower = texto_pagina.lower()

        matricula_match = re.search(r'matrícula\(s\):.*?([\d,\s]+)', texto_lower, re.DOTALL)
        if matricula_match:
            dados_extras['MATRICULA'] = re.sub(r'\s+', '', matricula_match.group(1).strip())

        modalidade_lower = modalidade.lower() if modalidade else ''
        
        if 'leilão' in modalidade_lower:
            price1_match = re.search(r'1º leilão[\s\S]*?R\$\s*([\d.,]+)', texto_pagina, re.IGNORECASE)
            price2_match = re.search(r'2º leilão[\s\S]*?R\$\s*([\d.,]+)', texto_pagina, re.IGNORECASE)
            date1_match = re.search(r'data do 1º leilão[\s\S]*?(\d{2}/\d{2}/\d{4})', texto_pagina, re.IGNORECASE)
            date2_match = re.search(r'data do 2º leilão[\s\S]*?(\d{2}/\d{2}/\d{4})', texto_pagina, re.IGNORECASE)

            if price1_match and date1_match and price2_match and date2_match:
                price1 = parse_valor(price1_match.group(1))
                price2 = parse_valor(price2_match.group(1))
                if price1 <= price2:
                    dados_extras['PRECO'] = price1
                    dados_extras['DATA_DISPUTA'] = date1_match.group(1)
                else:
                    dados_extras['PRECO'] = price2
                    dados_extras['DATA_DISPUTA'] = date2_match.group(1)
            else:
                match_data_generica = re.search(r'data d[oa] leilão[\s\S-]*?(\d{2}/\d{2}/\d{4})', texto_pagina, re.IGNORECASE)
                if match_data_generica:
                    dados_extras['DATA_DISPUTA'] = match_data_generica.group(1)

        elif 'licitação' in modalidade_lower:
            match_licitacao = re.search(r'data da licitação aberta[\s\S-]*?(\d{2}/\d{2}/\d{4})', texto_pagina, re.IGNORECASE)
            if match_licitacao:
                dados_extras['DATA_DISPUTA'] = match_licitacao.group(1)
        
        condominio_percent_match = re.search(r'condomínio: sob responsabilidade do comprador, até o limite de (\d+)%', texto_lower)
        if condominio_percent_match:
            percentual = condominio_percent_match.group(1)
            dados_extras['CONDOMINIO'] = f'Arrematante {percentual}%'
        elif 'condomínio: sob responsabilidade do comprador.' in texto_lower:
            dados_extras['CONDOMINIO'] = 'Arrematante'
        else:
            dados_extras['CONDOMINIO'] = ''

        dados_extras['FINANCIAMENTO'] = 'SIM' if 'permite financiamento' in texto_lower or 'com financiamento' in texto_lower else 'NÃO'
        dados_extras['FGTS'] = 'SIM' if 'permite utilização de fgts' in texto_lower or 'com utilização de fgts' in texto_lower else 'NÃO'
        
        return dados_extras
        
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
            
            estado = os.path.basename(arquivo).replace('.csv', '')
            yield {"type": "csv_processed", "state": estado, "items_count": len(df), "message": f"{estado}: {len(df)} itens encontrados no CSV"}
            
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
    
    current_state = None
    
    for idx, row in df_final.iterrows():
        estado_linha = row.get('UF', '')
        if estado_linha != current_state:
            current_state = estado_linha
            state_processed = 0
            state_total = len(df_final[df_final['UF'] == current_state])
        
        state_processed = len([r for r in dados_processados if r.get('UF') == current_state]) + 1
        
        yield {
            "type": "state_progress", 
            "state": current_state,
            "current": state_processed,
            "total": state_total,
            "overall_current": idx + 1,
            "overall_total": total_linhas,
            "message": f"Processando {current_state}: {state_processed}/{state_total}"
        }
        
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
            'Status': 'Novo'
        }
        
        if desc_texto:
            dados_linha['TIPO'] = desc_texto.split(',')[0].strip().title()
        else:
            dados_linha['TIPO'] = next((t for t in ['casa', 'apartamento', 'terreno'] if t in desc_texto), 'Não especificado')
        
        if m := re.search(r'(\d+[.,]?\d*)\s*de área privativa', desc_texto):
            area_priv = float(m.group(1).replace(',', '.'))
            dados_linha['AREA_PRIVATIVA'] = f"{area_priv:.2f} m²".replace('.', ',')
        else:
            dados_linha['AREA_PRIVATIVA'] = ''
            
        if m := re.search(r'(\d+[.,]?\d*)\s*de área do terreno', desc_texto):
            area_terr = float(m.group(1).replace(',', '.'))
            dados_linha['AREA_DO_TERRENO'] = f"{area_terr:.2f} m²".replace('.', ',')
        else:
            dados_linha['AREA_DO_TERRENO'] = ''
        
        dados_linha['FGTS'] = 'NÃO'
        dados_linha['FINANCIAMENTO'] = 'NÃO'
        dados_linha['CONDOMINIO'] = ''
        dados_linha['DATA_DISPUTA'] = ''
        
        if pd.notna(dados_linha['LINK']):
            if extras := extrair_dados_pagina_imovel(dados_linha['LINK'], dados_linha['MODALIDADE']):
                for key, value in extras.items():
                    if value: 
                        dados_linha[key] = value
        
        dados_processados.append(dados_linha)

    df_final = pd.DataFrame(dados_processados)
    
    if not df_final.empty:
        if 'PRECO' in df_final.columns and 'AVALIACAO' in df_final.columns:
            def calcular_desconto(row):
                preco = row.get('PRECO', 0)
                avaliacao = row.get('AVALIACAO', 0)
                if pd.notna(preco) and pd.notna(avaliacao) and avaliacao > 0 and preco < avaliacao:
                    desconto = int((1 - preco / avaliacao) * 100)
                    return f"{desconto}%"
                return "0%"
            
            df_final['DESCONTO'] = df_final.apply(calcular_desconto, axis=1)
        
        def criar_id_unico(row):
            matricula_original = str(row['MATRICULA']).strip() if pd.notna(row.get('MATRICULA')) and str(row.get('MATRICULA')).strip() else ''
            uf = str(row['UF']).strip().upper()
            iniciais_endereco = _generate_address_initials(row.get('ENDERECO', ''))
            return f"{uf}{matricula_original}{iniciais_endereco}"
        df_final['MATRICULA'] = df_final.apply(criar_id_unico, axis=1)
        
    yield {"type": "scraping_done", "message": "Processamento concluído.", "data": df_final.to_dict('records')}