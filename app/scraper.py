import pandas as pd
import requests
from bs4 import BeautifulSoup
import time, os, glob, logging, re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
PASTA_TEMPORARIOS = 'temporarios'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

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
    limpar_pasta_temporarios()
    for estado in estados:
        yield {"type": "download", "message": f'Baixando lista de {estado}...'}
        url_download = f"https://venda-imoveis.caixa.gov.br/listaweb/Lista_imoveis_{estado}.csv"
        try:
            time.sleep(0.5)
            response = requests.get(url_download, headers=HEADERS, timeout=60)
            if response.status_code == 200:
                response.encoding = 'latin-1'
                with open(os.path.join(PASTA_TEMPORARIOS, f"{estado}.csv"), 'w', encoding='utf-8') as f:
                    f.write(response.text)
            else:
                yield {"type": "download", "message": f"Aviso: Falha ao baixar lista de {estado}."}
        except Exception as e:
            yield {"type": "download", "message": f"ERRO ao baixar lista de {estado}: {e}"}

def extrair_dados_pagina_imovel(url, modalidade):
    try:
        time.sleep(0.2)
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        texto_pagina = soup.get_text(separator='\n', strip=True)
        texto_lower = texto_pagina.lower()
        dados_imovel = {}

        if m := re.search(r'matrícula\(s\):.*?([\d,\s]+)', texto_lower, re.DOTALL):
            dados_imovel['MATRICULA'] = re.sub(r'\s+', '', m.group(1).strip())

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
                    dados_imovel['PRECO'] = price1
                    dados_imovel['DATA_DISPUTA'] = date1_match.group(1)
                else:
                    dados_imovel['PRECO'] = price2
                    dados_imovel['DATA_DISPUTA'] = date2_match.group(1)
            else:
                if m := re.search(r'data d[oa] leilão[\s\S-]*?(\d{2}/\d{2}/\d{4})', texto_pagina, re.IGNORECASE):
                    dados_imovel['DATA_DISPUTA'] = m.group(1)
        
        elif 'licitação' in modalidade_lower:
            if m := re.search(r'data da licitação.*?(\d{2}/\d{2}/\d{4})', texto_pagina, re.IGNORECASE):
                dados_imovel['DATA_DISPUTA'] = m.group(1)

        if m := re.search(r'condomínio: sob responsabilidade do comprador, até o limite de (\d+)%', texto_lower):
            dados_imovel['CONDOMINIO'] = f'Arrematante {m.group(1)}%'
        elif 'condomínio: sob responsabilidade do comprador' in texto_lower:
            dados_imovel['CONDOMINIO'] = 'Arrematante'

        dados_imovel['FINANCIAMENTO'] = 'SIM' if 'permite financiamento' in texto_lower else 'NÃO'
        dados_imovel['FGTS'] = 'SIM' if 'permite utilização de fgts' in texto_lower else 'NÃO'
        return dados_imovel
    except Exception as e:
        logging.error(f"Erro ao processar URL {url}: {e}")
        return None

def processar_arquivos():
    arquivos_csv = glob.glob(os.path.join(PASTA_TEMPORARIOS, '*.csv'))
    if not arquivos_csv:
        yield {"type": "error", "message": "Nenhum arquivo CSV para processar."}; return

    yield {"type": "stage", "message": f"Lendo {len(arquivos_csv)} arquivos CSV..."}
    
    lista_dfs = []
    for f in arquivos_csv:
        try:
            df = pd.read_csv(f, sep=';', skiprows=2, encoding='utf-8', on_bad_lines='skip')
            for col in df.select_dtypes(['object']).columns:
                df[col] = df[col].str.strip()
            
            lista_dfs.append(df)
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {f}: {e}")

    if not lista_dfs:
        yield {"type": "error", "message": "Não foi possível ler nenhum arquivo CSV."}; return

    df_completo = pd.concat(lista_dfs, ignore_index=True).dropna(subset=['Endereço'])
    
    coluna_link = next((c for c in df_completo.columns if 'link' in c.lower()), None)
    if not coluna_link:
        yield {"type": "error", "message": "Coluna de link não encontrada."}; return

    dados_processados = []
    total_imoveis = len(df_completo)
    for i, row in df_completo.iterrows():
        estado = row.get('UF', 'Desconhecido')
        yield {"type": "property_progress", "current": i + 1, "total": total_imoveis, "estado": estado}
        
        desc_texto = str(row.get('Descrição', ''))
        
        dados_linha = {
            'UF': row.get('UF'), 'CIDADE': row.get('Cidade'), 'BAIRRO': row.get('Bairro'),
            'ENDERECO': row.get('Endereço'), 'PRECO': parse_valor(row.get('Preço')),
            'AVALIACAO': parse_valor(row.get('Valor de avaliação')), 'MODALIDADE': row.get('Modalidade de venda'),
            'TIPO': desc_texto.split(',')[0].strip(), 'LINK': row.get(coluna_link)
        }

        if m := re.search(r'(\d+[.,]?\d*)\s*de área privativa', desc_texto):
            dados_linha['AREA_PRIVATIVA'] = f"{m.group(1).replace(',', '.')} m²"
        if m := re.search(r'(\d+[.,]?\d*)\s*de área do terreno', desc_texto):
            dados_linha['AREA_DO_TERRENO'] = f"{m.group(1).replace(',', '.')} m²"

        if pd.notna(dados_linha['LINK']):
            if extras := extrair_dados_pagina_imovel(dados_linha['LINK'], dados_linha['MODALIDADE']):
                if 'PRECO' in extras:
                    dados_linha['PRECO'] = extras['PRECO']
                dados_linha.update(extras)
        
        dados_processados.append(dados_linha)

    df_final = pd.DataFrame(dados_processados)
    if not df_final.empty:
        df_final['MATRICULA'] = df_final.apply(
            lambda row: row['MATRICULA'] if pd.notna(row.get('MATRICULA')) else (re.search(r'num_imovel=(\d+)', row['LINK']).group(1) if pd.notna(row['LINK']) and re.search(r'num_imovel=(\d+)', row['LINK']) else None),
            axis=1
        )
        df_final.dropna(subset=['MATRICULA'], inplace=True)
        df_final.drop_duplicates(subset=['MATRICULA'], keep='last', inplace=True)
        df_final['DESCONTO'] = df_final.apply(
            lambda r: f"{int((1 - r['PRECO'] / r['AVALIACAO']) * 100)}%" if r['AVALIACAO'] > 0 and r['PRECO'] < r['AVALIACAO'] else "0%", axis=1)

    yield {"type": "scraping_done", "data": df_final.to_dict(orient='records')}