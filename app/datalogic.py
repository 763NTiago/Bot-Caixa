from app import db, create_app
from app.models import Imovel, Atualizacao
from sqlalchemy import func
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def get_summary_stats():
    app = create_app()
    with app.app_context():
        total_imoveis = db.session.query(Imovel).count()
        novos_imoveis = db.session.query(Imovel).filter(Imovel.Status == 'Novo').count()
        atualizados = db.session.query(Imovel).filter(Imovel.Status == 'Atualizado').count()
        expirados = db.session.query(Imovel).filter(Imovel.Status == 'Expirado').count()
        ativos = db.session.query(Imovel).filter(Imovel.Status.in_(['Novo', 'Existente', 'Atualizado'])).count()
        media_preco = db.session.query(db.func.avg(Imovel.PRECO)).filter(
            Imovel.PRECO > 0,
            Imovel.Status.in_(['Novo', 'Existente', 'Atualizado'])
        ).scalar() or 0

        return {
            "total_imoveis": total_imoveis,
            "novos_imoveis": novos_imoveis,
            "atualizados": atualizados,
            "expirados": expirados,
            "ativos": ativos,
            "media_preco": round(media_preco, 2)
        }

def get_uf_summary():
    app = create_app()
    with app.app_context():
        query = db.session.query(
            Imovel.UF,
            db.func.count(Imovel.MATRICULA).label('Total'),
            db.func.sum(db.case((Imovel.Status == 'Novo', 1), else_=0)).label('Novos'),
            db.func.sum(db.case((Imovel.Status == 'Atualizado', 1), else_=0)).label('Atualizados'),
            db.func.sum(db.case((Imovel.Status == 'Expirado', 1), else_=0)).label('Expirados')
        ).group_by(Imovel.UF).order_by(db.desc('Total'))

        resultado = []
        for row in query.all():
            resultado.append({
                'UF': row.UF,
                'Total': row.Total,
                'Novos': row.Novos or 0,
                'Atualizados': row.Atualizados or 0,
                'Expirados': row.Expirados or 0
            })
        return resultado

def process_scraped_data(data):
    app = create_app()
    with app.app_context():
        if not data:
            logging.warning("Nenhum dado recebido para processamento.")
            return

        df_novos = pd.DataFrame(data)
        if df_novos.empty:
            logging.info("DataFrame de novos imóveis está vazio. Nenhum dado para processar.")
            return

        df_novos.drop_duplicates(subset=['MATRICULA'], keep='last', inplace=True)
        df_novos = df_novos.where(pd.notnull(df_novos), None)

        ufs_processados = df_novos['UF'].unique().tolist()
        logging.info(f"Iniciando processamento para os estados: {ufs_processados}")

        if not ufs_processados:
            return

        imoveis_db_list = Imovel.query.filter(Imovel.UF.in_(ufs_processados)).all()
        imoveis_db_dict = {(imovel.UF, imovel.MATRICULA): imovel for imovel in imoveis_db_list}

        Atualizacao.query.filter(Atualizacao.UF.in_(ufs_processados)).delete(synchronize_session=False)
        db.session.commit()

        chaves_processadas = set()

        for _, row in df_novos.iterrows():
            imovel_dict = row.to_dict()
            matricula = str(imovel_dict.get('MATRICULA'))
            uf = str(imovel_dict.get('UF'))
            chave_composta = (uf, matricula)

            if not matricula or not uf or chave_composta in chaves_processadas:
                continue

            chaves_processadas.add(chave_composta)
            imovel_existente = imoveis_db_dict.get(chave_composta)
            changed_fields = []
            change_type = None
            final_status = None

            if imovel_existente:
                for key, new_value in imovel_dict.items():
                    if hasattr(imovel_existente, key) and key not in ['MATRICULA', 'UF', 'updated_at', 'Status']:
                        old_value = getattr(imovel_existente, key)
                        if str(old_value) != str(new_value):
                            changed_fields.append(key)
                            setattr(imovel_existente, key, new_value)

                if changed_fields:
                    final_status = 'Atualizado'
                    change_type = 'Atualizado'
                else:
                    if imovel_existente.Status == 'Atualizado':
                        final_status = 'Existente'
                    elif imovel_existente.Status == 'Novo':
                        final_status = 'Existente'
                    else:
                        final_status = 'Existente'

                imovel_existente.Status = final_status

                if chave_composta in imoveis_db_dict:
                    imoveis_db_dict.pop(chave_composta)

            else:
                imovel_existente_check = db.session.query(Imovel).filter_by(UF=uf, MATRICULA=matricula).first()
                if not imovel_existente_check:
                    imovel_novo_dict = {k: v for k, v in imovel_dict.items() if hasattr(Imovel, k)}
                    imovel_novo_dict['Status'] = 'Novo'

                    try:
                        novo_imovel = Imovel(**imovel_novo_dict)
                        db.session.add(novo_imovel)
                        db.session.flush()
                        change_type = 'Novo'
                        final_status = 'Novo'
                    except Exception as e:
                        logging.warning(f"Erro ao inserir imóvel {uf}-{matricula}: {e}")
                        db.session.rollback()
                        continue
                else:
                    for key, new_value in imovel_dict.items():
                        if hasattr(imovel_existente_check, key) and key not in ['MATRICULA', 'UF', 'updated_at', 'Status']:
                            old_value = getattr(imovel_existente_check, key)
                            if str(old_value) != str(new_value):
                                changed_fields.append(key)
                                setattr(imovel_existente_check, key, new_value)

                    if changed_fields:
                        imovel_existente_check.Status = 'Atualizado'
                        change_type = 'Atualizado'
                    else:
                        imovel_existente_check.Status = 'Existente'

            if change_type:
                atualizacao_dados = {
                    k: imovel_dict.get(k) for k in ['MATRICULA', 'UF', 'TIPO', 'CIDADE', 'PRECO', 'LINK']
                    if k in imovel_dict
                }
                atualizacao_dados['Change'] = change_type
                atualizacao_dados['ChangedFields'] = ",".join(changed_fields) if changed_fields else ""

                existing_update = Atualizacao.query.filter_by(UF=uf, MATRICULA=matricula).first()
                if existing_update:
                    for key, value in atualizacao_dados.items():
                        setattr(existing_update, key, value)
                else:
                    db.session.add(Atualizacao(**atualizacao_dados))

        if imoveis_db_dict:
            chaves_expiradas = list(imoveis_db_dict.keys())
            logging.info(f"Marcando {len(chaves_expiradas)} imóveis como expirados.")

            for (uf, matricula), imovel in imoveis_db_dict.items():
                imovel.Status = 'Expirado'

        try:
            db.session.flush()
            db.session.commit()
            logging.info(f"Processamento concluído para os estados: {ufs_processados}")
        except Exception as e:
            db.session.rollback()
            logging.error(f"Erro ao salvar dados: {e}")
            raise

def get_imoveis_agrupados_por_bairro():
    app = create_app()
    with app.app_context():
        imoveis = Imovel.query.filter(
            Imovel.Status.in_(['Novo', 'Existente', 'Atualizado'])
        ).order_by(Imovel.UF, Imovel.CIDADE, Imovel.BAIRRO, Imovel.PRECO).all()

        imoveis_agrupados = {}
        for imovel in imoveis:
            uf_key = imovel.UF or "N/A"
            cidade_key = imovel.CIDADE or "N/A"
            bairro_key = imovel.BAIRRO or "N/A"

            if uf_key not in imoveis_agrupados:
                imoveis_agrupados[uf_key] = {}
            if cidade_key not in imoveis_agrupados[uf_key]:
                imoveis_agrupados[uf_key][cidade_key] = {}
            if bairro_key not in imoveis_agrupados[uf_key][cidade_key]:
                imoveis_agrupados[uf_key][cidade_key][bairro_key] = []

            imoveis_agrupados[uf_key][cidade_key][bairro_key].append(imovel.to_dict())

        resultado_final = {}
        for uf, cidades in imoveis_agrupados.items():
            for cidade, bairros in cidades.items():
                for bairro, lista in bairros.items():
                    if len(lista) > 1:
                        if uf not in resultado_final:
                            resultado_final[uf] = {}
                        if cidade not in resultado_final[uf]:
                            resultado_final[uf][cidade] = {}
                        resultado_final[uf][cidade][bairro] = lista

        return resultado_final

def get_filter_options():
    app = create_app()
    with app.app_context():
        normalized_city = func.upper(func.trim(Imovel.CIDADE))
        cidades_query = db.session.query(normalized_city).distinct().filter(
            Imovel.CIDADE.isnot(None) & (func.trim(Imovel.CIDADE) != '')
        ).order_by(normalized_city)

        return {
            'ufs': [r[0] for r in db.session.query(Imovel.UF).distinct().order_by(Imovel.UF).all() if r[0]],
            'cidades': [r[0] for r in cidades_query.all()],
            'tipos': [r[0] for r in db.session.query(Imovel.TIPO).distinct().order_by(Imovel.TIPO).all() if r[0]],
            'modalidades': [r[0] for r in db.session.query(Imovel.MODALIDADE).distinct().order_by(Imovel.MODALIDADE).all() if r[0]]
        }

def get_imoveis_abaixo_de_100k(filtros=None):
    """
    Busca imóveis com preço abaixo de 100k, aplicando filtros dinâmicos.
    'filtros' é um dicionário com os critérios de busca.
    """
    app = create_app()
    with app.app_context():
        query = Imovel.query.filter(
            Imovel.PRECO < 100000,
            Imovel.Status.in_(['Novo', 'Existente', 'Atualizado'])
        )

        if filtros:
            # Filtros de texto exato (UF, TIPO, MODALIDADE, etc.)
            for chave, valor in filtros.items():
                if valor and hasattr(Imovel, chave.upper()) and chave not in ['preco_min', 'preco_max', 'status']:
                    coluna = getattr(Imovel, chave.upper())
                    query = query.filter(func.upper(func.trim(coluna)) == valor.upper())

            # Filtros de preço
            preco_min = filtros.get('preco_min')
            if preco_min:
                query = query.filter(Imovel.PRECO >= float(preco_min))

            preco_max = filtros.get('preco_max')
            if preco_max:
                query = query.filter(Imovel.PRECO <= float(preco_max))

            # Filtro de status
            status = filtros.get('status')
            if status and status != 'Todos':
                if status == 'Ativos':
                    query = query.filter(Imovel.Status.in_(['Novo', 'Existente', 'Atualizado']))
                elif status == 'Apenas Novos':
                    query = query.filter(Imovel.Status == 'Novo')
                elif status == 'Expirado':
                    query = query.filter(Imovel.Status == 'Expirado')

        imoveis = query.order_by(Imovel.PRECO.asc()).all()
        return [imovel.to_dict() for imovel in imoveis]

def get_distinct_ufs_from_db():
    app = create_app()
    with app.app_context():
        return [uf[0] for uf in db.session.query(Imovel.UF).distinct().order_by(Imovel.UF).all() if uf[0]]

def get_imoveis_for_export(estados=[]):
    app = create_app()
    with app.app_context():
        try:
            query = Imovel.query

            if estados:
                estados_limpos = [uf.strip().upper() for uf in estados if uf.strip()]
                if estados_limpos:
                    query = query.filter(Imovel.UF.in_(estados_limpos))

            imoveis = query.order_by(Imovel.UF, Imovel.CIDADE, Imovel.PRECO).all()

            if not imoveis:
                logging.warning("Nenhum imóvel encontrado para exportação")
                colunas = [c.name for c in Imovel.__table__.columns if c.name != 'updated_at']
                return pd.DataFrame(columns=colunas)

            dados = []
            for imovel in imoveis:
                imovel_dict = imovel.to_dict()
                for campo in ['PRECO', 'AVALIACAO']:
                    if campo in imovel_dict and imovel_dict[campo] is not None:
                        try:
                            imovel_dict[campo] = float(imovel_dict[campo])
                        except (ValueError, TypeError):
                            imovel_dict[campo] = 0.0
                dados.append(imovel_dict)

            df = pd.DataFrame(dados)

            colunas_ordem = [
                'MATRICULA', 'UF', 'CIDADE', 'BAIRRO', 'ENDERECO', 'Status',
                'PRECO', 'AVALIACAO', 'DESCONTO', 'AREA_PRIVATIVA', 'AREA_DO_TERRENO',
                'TIPO', 'MODALIDADE', 'DATA_DISPUTA', 'FGTS', 'FINANCIAMENTO',
                'CONDOMINIO', 'LINK'
            ]

            colunas_existentes = [col for col in colunas_ordem if col in df.columns]
            colunas_extras = [col for col in df.columns if col not in colunas_ordem]

            df = df[colunas_existentes + colunas_extras]

            logging.info(f"DataFrame criado para exportação: {len(df)} imóveis, {len(df.columns)} colunas")
            return df

        except Exception as e:
            logging.error(f"Erro ao buscar dados para exportação: {e}", exc_info=True)
            colunas = [c.name for c in Imovel.__table__.columns if c.name != 'updated_at']
            return pd.DataFrame(columns=colunas)

# --- NOVAS FUNÇÕES ADICIONADAS ---
def get_comparable_locations():
    """
    Retorna um dicionário estruturado de UFs, cidades e bairros que possuem
    mais de um imóvel, ideal para os filtros da página de comparação.
    """
    app = create_app()
    with app.app_context():
        subquery = db.session.query(
            Imovel.UF,
            Imovel.CIDADE,
            Imovel.BAIRRO
        ).filter(
            Imovel.Status.in_(['Novo', 'Existente', 'Atualizado']),
            Imovel.UF.isnot(None),
            Imovel.CIDADE.isnot(None),
            Imovel.BAIRRO.isnot(None)
        ).group_by(
            Imovel.UF,
            Imovel.CIDADE,
            Imovel.BAIRRO
        ).having(
            func.count(Imovel.MATRICULA) > 1
        ).subquery()

        results = db.session.query(
            subquery.c.UF,
            subquery.c.CIDADE,
            subquery.c.BAIRRO
        ).order_by(subquery.c.UF, subquery.c.CIDADE, subquery.c.BAIRRO).all()

        locations = {}
        for uf, cidade, bairro in results:
            if uf not in locations:
                locations[uf] = {}
            if cidade not in locations[uf]:
                locations[uf][cidade] = []
            locations[uf][cidade].append(bairro)
        return locations

def get_baratos_locations():
    """
    Retorna um dicionário estruturado de UFs, cidades e bairros
    para imóveis com preço abaixo de 100k.
    """
    app = create_app()
    with app.app_context():
        query = db.session.query(
            Imovel.UF,
            Imovel.CIDADE,
            Imovel.BAIRRO
        ).filter(
            Imovel.PRECO < 100000,
            Imovel.Status.in_(['Novo', 'Existente', 'Atualizado']),
            Imovel.UF.isnot(None),
            Imovel.CIDADE.isnot(None)
        ).distinct().order_by(Imovel.UF, Imovel.CIDADE, Imovel.BAIRRO).all()

        locations = {}
        for uf, cidade, bairro in query:
            if not bairro: continue
            if uf not in locations:
                locations[uf] = {}
            if cidade not in locations[uf]:
                locations[uf][cidade] = []
            if bairro not in locations[uf][cidade]:
                locations[uf][cidade].append(bairro)
        return locations

def get_comparable_ufs():
    """ Retorna uma lista de UFs que têm bairros comparáveis. """
    comparable_data = get_comparable_locations()
    return sorted(list(comparable_data.keys()))

def get_comparable_cidades(uf):
    """ Retorna uma lista de cidades para uma UF que têm bairros comparáveis. """
    if not uf: return []
    comparable_data = get_comparable_locations()
    return sorted(list(comparable_data.get(uf, {}).keys()))

def get_comparable_bairros(uf, cidade):
    """ Retorna uma lista de bairros para uma UF/Cidade que são comparáveis. """
    if not uf or not cidade: return []
    comparable_data = get_comparable_locations()
    return sorted(comparable_data.get(uf, {}).get(cidade, []))