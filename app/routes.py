from flask import Blueprint, render_template, request, Response, jsonify, send_file
import json
import io
import logging
import os
import pandas as pd
from app import datalogic, scraper, db
from app.planilha import formatar_planilha_excel
from app.models import Imovel, Atualizacao
from sqlalchemy import func
from werkzeug.utils import secure_filename
from converter import convert_excel_to_db

bp = Blueprint('main', __name__)

@bp.route('/')
def dashboard():
    return render_template('dashboard.html')

@bp.route('/api/data')
def api_data():
    try:
        query = db.session.query(Imovel, Atualizacao.ChangedFields).outerjoin(
            Atualizacao, Imovel.MATRICULA == Atualizacao.MATRICULA
        )
        
        status_filter = request.args.get('status', '').strip()
        
        if status_filter == 'Ativos':
            query = query.filter(Imovel.Status.in_(['Novo', 'Existente', 'Atualizado']))
        elif status_filter == 'Apenas Novos':
            query = query.filter(Imovel.Status == 'Novo')
        elif status_filter == 'Apenas Atualizados':
            query = query.filter(Imovel.Status == 'Atualizado')
        elif status_filter == 'Expirado':
            query = query.filter(Imovel.Status == 'Expirado')

        filtros = {
            'uf': 'UF',
            'cidade': 'CIDADE', 
            'bairro': 'BAIRRO',
            'tipo': 'TIPO',
            'modalidade': 'MODALIDADE',
            'fgts': 'FGTS',
            'financiamento': 'FINANCIAMENTO'
        }
        
        for param, column in filtros.items():
            valor = request.args.get(param, '').strip()
            if valor:
                query = query.filter(getattr(Imovel, column) == valor)
        
        try:
            preco_min = request.args.get('preco_min')
            if preco_min:
                preco_min = float(preco_min)
                query = query.filter(Imovel.PRECO >= preco_min)
        except (ValueError, TypeError):
            pass
            
        try:
            preco_max = request.args.get('preco_max')
            if preco_max:
                preco_max = float(preco_max)
                query = query.filter(Imovel.PRECO <= preco_max)
        except (ValueError, TypeError):
            pass
        
        data_inicio = request.args.get('data_inicio', '').strip()
        data_fim = request.args.get('data_fim', '').strip()
        
        if data_inicio:
            query = query.filter(Imovel.DATA_DISPUTA >= data_inicio)
        if data_fim:
            query = query.filter(Imovel.DATA_DISPUTA <= data_fim)
        
        results = query.order_by(Imovel.PRECO.asc()).all()
        
        imoveis_list = []
        for imovel, changed_fields in results:
            imovel_dict = imovel.to_dict()
            imovel_dict['ChangedFields'] = changed_fields or ""
            imoveis_list.append(imovel_dict)
            
        return jsonify(imoveis_list)
        
    except Exception as e:
        logging.error(f"Erro na API de dados: {e}", exc_info=True)
        return jsonify([])

@bp.route('/api/summary')
def api_summary():
    try:
        return jsonify({
            'stats': datalogic.get_summary_stats(),
            'uf_summary': datalogic.get_uf_summary()
        })
    except Exception as e:
        logging.error(f"Erro ao obter resumo: {e}", exc_info=True)
        return jsonify({
            'stats': {
                'total_imoveis': 0,
                'novos_imoveis': 0,
                'atualizados': 0,
                'expirados': 0,
                'ativos': 0,
                'media_preco': 0
            },
            'uf_summary': []
        })

@bp.route('/api/filters')
def api_filters():
    try:
        filters_data = datalogic.get_filter_options()
        
        filters_data['bairros'] = [
            r[0] for r in db.session.query(Imovel.BAIRRO).distinct().order_by(Imovel.BAIRRO).all() 
            if r[0]
        ]
        
        preco_stats = db.session.query(
            func.min(Imovel.PRECO), 
            func.max(Imovel.PRECO)
        ).filter(Imovel.PRECO > 0).first()
        
        filters_data['preco_range'] = {
            'min': float(preco_stats[0] or 0),
            'max': float(preco_stats[1] or 1000000)
        }
        
        return jsonify(filters_data)
        
    except Exception as e:
        logging.error(f"Erro ao obter filtros: {e}", exc_info=True)
        return jsonify({
            'ufs': [], 
            'cidades': [], 
            'tipos': [], 
            'modalidades': [], 
            'bairros': [],
            'preco_range': {'min': 0, 'max': 1000000}
        })

@bp.route('/api/imoveis_baratos')
def api_imoveis_baratos():
    try:
        return jsonify(datalogic.get_imoveis_abaixo_de_100k())
    except Exception as e:
        logging.error(f"Erro ao obter imóveis baratos: {e}", exc_info=True)
        return jsonify([])

@bp.route('/api/distinct_ufs')
def api_distinct_ufs():
    try:
        return jsonify(datalogic.get_distinct_ufs_from_db())
    except Exception as e:
        logging.error(f"Erro ao obter UFs: {e}", exc_info=True)
        return jsonify([])

@bp.route('/api/cidades_por_uf')
def api_cidades_por_uf():
    try:
        uf = request.args.get('uf', '').strip()
        if not uf:
            return jsonify([])
            
        cidades = [
            r[0] for r in db.session.query(Imovel.CIDADE)
            .filter(Imovel.UF == uf)
            .distinct()
            .order_by(Imovel.CIDADE).all() 
            if r[0]
        ]
        return jsonify(cidades)
        
    except Exception as e:
        logging.error(f"Erro ao obter cidades: {e}", exc_info=True)
        return jsonify([])

@bp.route('/api/bairros_por_cidade')
def api_bairros_por_cidade():
    try:
        query = db.session.query(Imovel.BAIRRO).distinct()
        
        cidade = request.args.get('cidade', '').strip()
        if cidade:
            query = query.filter(Imovel.CIDADE == cidade)
            
        uf = request.args.get('uf', '').strip()
        if uf:
            query = query.filter(Imovel.UF == uf)
        
        bairros = [r[0] for r in query.order_by(Imovel.BAIRRO).all() if r[0]]
        return jsonify(bairros)
        
    except Exception as e:
        logging.error(f"Erro ao obter bairros: {e}", exc_info=True)
        return jsonify([])
    
@bp.route('/imoveis_baratos')
def imoveis_baratos_page():
    return render_template('imoveis_baratos.html')

@bp.route('/comparacao')
def comparacao_page():
    try:
        imoveis_agrupados = datalogic.get_imoveis_agrupados_por_bairro()
        return render_template('comparacao.html', imoveis_agrupados=imoveis_agrupados)
    except Exception as e:
        logging.error(f"Erro na página de comparação: {e}", exc_info=True)
        return render_template('comparacao.html', imoveis_agrupados={})

@bp.route('/processar')
def processar():
    estados = [uf.strip() for uf in request.args.get('estados', '').split(',') if uf.strip()]
    if not estados:
        return Response(
            f"data: {json.dumps({'type': 'error', 'message': 'Nenhum estado selecionado.'})}\n\n", 
            mimetype='text/event-stream'
        )

    def generate_events():
        try:
            yield f"data: {json.dumps({'type': 'stage', 'message': 'Iniciando raspagem...'})}\n\n"
            
            for event in scraper.baixar_listas_por_estados(estados):
                yield f"data: {json.dumps(event)}\n\n"
            
            scraped_data = []
            
            for event in scraper.processar_arquivos():
                if event.get('type') == 'scraping_done':
                    scraped_data = event.get('data', [])
                yield f"data: {json.dumps(event)}\n\n"
            
            yield f"data: {json.dumps({'type': 'updating_status', 'message': 'Atualizando status dos imóveis...'})}\n\n"
            datalogic.process_scraped_data(scraped_data)
            
            yield f"data: {json.dumps({'type': 'done', 'message': 'Processo finalizado com sucesso!'})}\n\n"
            
        except Exception as e:
            logging.error(f"Erro no stream de processamento: {e}", exc_info=True)
            yield f"data: {json.dumps({'type': 'error', 'message': f'Erro no processamento: {str(e)}'})}\n\n"
            
    return Response(generate_events(), mimetype='text/event-stream')

@bp.route('/upload_excel', methods=['POST'])
def upload_excel():
    try:
        files = request.files.getlist('files')
        if not files or all(f.filename == '' for f in files):
            return jsonify({
                'success': False, 
                'message': 'Nenhum arquivo selecionado.'
            }), 400

        temp_dir = 'temporarios'
        results = []
        os.makedirs(temp_dir, exist_ok=True)
        
        for file in files:
            if file and file.filename.endswith(('.xlsx', '.xls')):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir, filename)
                
                try:
                    file.save(file_path)
                    success, message = convert_excel_to_db(file_path)
                    results.append({
                        'file': file.filename, 
                        'success': success, 
                        'message': message
                    })
                    
                except Exception as e:
                    results.append({
                        'file': file.filename, 
                        'success': False, 
                        'message': f'Erro ao processar arquivo: {str(e)}'
                    })
                    
                finally:
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except OSError:
                            pass
        
        success_count = sum(1 for r in results if r['success'])
        total_count = len(results)
        
        return jsonify({
            'success': success_count == total_count,
            'message': f'Processados {success_count}/{total_count} arquivos com sucesso.',
            'results': results
        })
    
    except Exception as e:
        logging.error(f"Erro no upload de Excel: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'Erro no upload: {str(e)}'
        }), 500

@bp.route('/export/xlsx-hyperlink')
def export_xlsx_hyperlink():
    try:
        estados_param = request.args.get('estados', '').strip()
        estados = []
        
        if estados_param:
            estados = [uf.strip().upper() for uf in estados_param.split(',') if uf.strip()]
            logging.info(f"Exportando para estados: {estados}")
        else:
            logging.info("Exportando todos os estados")
        
        df = datalogic.get_imoveis_for_export(estados)
        
        if df.empty:
            logging.warning("Nenhum dado encontrado para exportação")
        
        buffer = io.BytesIO()
        
        formatar_planilha_excel(df, buffer)
        buffer.seek(0)
        
        if estados:
            download_name = f'imoveis_{"_".join(estados[:3])}.xlsx'
            if len(estados) > 3:
                download_name = f'imoveis_{len(estados)}_estados.xlsx'
        else:
            download_name = 'todos_imoveis.xlsx'
        
        logging.info(f"Exportação concluída: {download_name}, {len(df)} registros")
        
        return send_file(
            buffer,
            as_attachment=True,
            download_name=download_name,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        logging.error(f"Erro na exportação: {e}", exc_info=True)
        
        return jsonify({
            'success': False,
            'message': f'Erro na exportação: {str(e)}'
        }), 500