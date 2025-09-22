class ProgressBar {
    constructor() {
        this.isActive = false;
        this.startTime = null;
        this.timerInterval = null;
        this.totalStates = 0;
        this.completedStates = 0;
        this.totalProperties = 0;
        this.currentState = '';
        this.currentProcessed = 0;
        this.currentTotal = 0;
        this.scrapingEventSource = null;

        this.initEvents();
    }

    initEvents() {
        $('#btn-stop-enhanced-scraping').on('click', () => this.stop());
    }

    show() {
        $('#enhanced-progress-container').addClass('show');
        this.isActive = true;
        this.startTime = new Date();
        this.startTimer();
        $('body').css('padding-bottom', '180px');
    }

    hide() {
        $('#enhanced-progress-container').removeClass('show');
        this.isActive = false;
        this.stopTimer();
        $('body').css('padding-bottom', '0');
        setTimeout(() => this.reset(), 300);
    }

    reset() {
        this.totalStates = 0;
        this.completedStates = 0;
        this.totalProperties = 0;
        this.currentState = '';
        this.currentProcessed = 0;
        this.currentTotal = 0;

        this.updateMainProgress(0, 0);
        this.updateCurrentProgress(0, 0, '');
        this.updateInfo(0);
        this.updateStatus('Preparando para iniciar o processamento...');
    }

    startTimer() {
        this.timerInterval = setInterval(() => {
            if (this.startTime) {
                const elapsed = new Date() - this.startTime;
                const minutes = Math.floor(elapsed / 60000);
                const seconds = Math.floor((elapsed % 60000) / 1000);
                $('#enhanced-elapsed-time').text(`${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`);
            }
        }, 1000);
    }

    stopTimer() {
        if (this.timerInterval) {
            clearInterval(this.timerInterval);
            this.timerInterval = null;
        }
    }

    updateMainProgress(completed, total) {
        this.completedStates = Math.max(0, completed);
        this.totalStates = Math.max(0, total);

        const percent = this.totalStates > 0 ? Math.round((this.completedStates / this.totalStates) * 100) : 0;
        const clampedPercent = Math.min(100, Math.max(0, percent));

        $('#main-progress-fill-enhanced').css('width', clampedPercent + '%');
        $('#main-progress-text-enhanced').text(clampedPercent > 10 ? clampedPercent + '%' : '');
        $('#overall-counter-enhanced').text(`${this.completedStates}/${this.totalStates} Estados`);
        $('#enhanced-states-processed').text(`${this.completedStates}/${this.totalStates}`);
    }

    updateCurrentProgress(processed, total, stateName = '') {
        this.currentProcessed = Math.max(0, processed);
        this.currentTotal = Math.max(0, total);

        if (stateName) {
            this.currentState = stateName;
            $('#current-state-label-enhanced').html(`<i class="bi bi-download"></i> ${stateName}`);
            $('#enhanced-current-state-name').text(stateName);
        }

        const percent = this.currentTotal > 0 ? Math.round((this.currentProcessed / this.currentTotal) * 100) : 0;
        const clampedPercent = Math.min(100, Math.max(0, percent));

        $('#current-progress-fill-enhanced').css('width', clampedPercent + '%');
        $('#current-progress-text-enhanced').text(clampedPercent > 15 ? clampedPercent + '%' : '');
        $('#current-state-counter-enhanced').text(`${this.currentProcessed.toLocaleString('pt-BR')}/${this.currentTotal.toLocaleString('pt-BR')} Itens`);
    }

    updateInfo(totalProperties) {
        this.totalProperties = Math.max(0, totalProperties);
        $('#enhanced-total-properties').text(this.totalProperties.toLocaleString('pt-BR'));
    }

    updateStatus(message, type = '') {
        const statusEl = $('#enhanced-progress-status');
        statusEl.removeClass('error success').text(message);
        if (type) statusEl.addClass(type);
    }

    stop() {
        if (this.scrapingEventSource) {
            this.scrapingEventSource.close();
            this.scrapingEventSource = null;
        }
        this.updateStatus('Processamento interrompido pelo usuário', 'error');
        $('#btn-nova-raspagem').prop('disabled', false).html('<i class="bi bi-arrow-clockwise"></i> Nova Raspagem');
        setTimeout(() => this.hide(), 1500);
    }

    startScraping(selectedStates) {
        this.show();
        this.totalStates = selectedStates.length;
        this.updateMainProgress(0, this.totalStates);
        this.updateStatus('Iniciando processamento...');

        $('#btn-nova-raspagem').prop('disabled', true).html('<i class="bi bi-hourglass-split"></i> Processando...');

        this.scrapingEventSource = new EventSource('/processar?estados=' + selectedStates.join(','));

        this.scrapingEventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleScrapingEvent(data);
            } catch (e) {
                console.error('Error parsing SSE data:', e);
            }
        };

        this.scrapingEventSource.onerror = () => {
            this.updateStatus('Conexão com servidor perdida', 'error');
            setTimeout(() => this.hide(), 5000);
        };
    }

    handleScrapingEvent(data) {
        switch (data.type) {
            case 'start':
                this.updateStatus(`Iniciando processamento de ${data.total_states || this.totalStates} estados...`);
                break;

            case 'download_start':
                this.updateCurrentProgress(0, 100, data.state);
                this.updateStatus(`Baixando dados de ${data.state}...`);
                break;

            case 'download_completed':
                this.updateCurrentProgress(100, 100, data.state);
                this.updateStatus(`Download de ${data.state} concluído`);
                break;

            case 'csv_processed':
                this.updateCurrentProgress(0, data.items_count, data.state);
                this.updateStatus(`${data.state}: ${data.items_count} itens encontrados, iniciando processamento...`);
                break;

            case 'state_progress':
                this.updateCurrentProgress(data.current || 0, data.total || 0, data.state);

                if (data.total_properties) {
                    this.updateInfo(data.total_properties);
                }

                if (data.overall_current && data.overall_total) {
                    const overallPercent = Math.round((data.overall_current / data.overall_total) * 100);
                    this.updateStatus(`${data.state}: ${data.current}/${data.total} processados (${overallPercent}% do total geral)`);
                } else {
                    this.updateStatus(`${data.state}: ${data.current}/${data.total} itens processados`);
                }
                break;

            case 'db_start':
                this.updateCurrentProgress(0, 100, data.state);
                this.updateStatus(`Salvando dados de ${data.state} no banco de dados...`);
                break;

            case 'db_progress':
                this.updateCurrentProgress(data.current || 0, data.total || 0, data.state);
                this.updateStatus(`${data.state}: Salvando ${data.current}/${data.total} no banco...`);
                break;

            case 'state_completed':
                this.updateMainProgress(data.current_state || (this.completedStates + 1), data.total_states || this.totalStates);
                this.updateCurrentProgress(100, 100, data.state);

                if (data.total_properties) {
                    this.updateInfo(data.total_properties);
                }

                const result = data.result || {};
                const newCount = result.new || 0;
                const updatedCount = result.updated || 0;
                const totalProcessed = newCount + updatedCount;

                this.updateStatus(`${data.state} finalizado: ${totalProcessed} itens processados (${newCount} novos, ${updatedCount} atualizados)`, 'success');
                break;

            case 'done':
                this.updateMainProgress(this.totalStates, this.totalStates);
                this.updateCurrentProgress(100, 100);
                if (data.total_properties) {
                    this.updateInfo(data.total_properties);
                }
                this.updateStatus(`Processamento finalizado! ${this.totalProperties} imóveis processados`, 'success');

                $('#btn-nova-raspagem').prop('disabled', false).html('<i class="bi bi-arrow-clockwise"></i> Nova Raspagem');

                setTimeout(() => {
                    this.hide();
                    location.reload();
                }, 3000);
                break;

            case 'error':
                this.updateStatus(`Erro: ${data.message}`, 'error');
                $('#btn-nova-raspagem').prop('disabled', false).html('<i class="bi bi-arrow-clockwise"></i> Nova Raspagem');
                setTimeout(() => this.hide(), 5000);
                break;

            default:
                if (data.message) {
                    this.updateStatus(data.message);
                }
                break;
        }
    }
}

$(document).ready(function() {
    let priceSlider;
    let currentFilters = {
        status: 'Ativos'
    };

    const progressBar = new ProgressBar();

    const formatCurrency = (value) => {
        if (!value || value === 0 || value === '0.00') return 'R$ 0,00';
        const num = parseFloat(value);
        if (isNaN(num)) return 'N/A';
        return `R$ ${num.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    };

    const formatArea = (value) => {
        if (!value || value === 'N/A' || value === 'null') return 'N/A';
        if (typeof value === 'string' && value.includes('m²')) return value;
        const num = parseFloat(value);
        if (isNaN(num)) return 'N/A';
        return `${num.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} m²`;
    };

    const calculatePricePerM2 = (preco, areaPrivativa, areaTerreno) => {
        const price = parseFloat(preco);
        if (isNaN(price) || price <= 0) return 'N/A';

        let area = 0;
        if (areaPrivativa && areaPrivativa !== 'N/A' && areaPrivativa !== 'null') {
            const areaPrivNum = parseFloat(areaPrivativa.toString().replace(/[^\d.,]/g, '').replace(',', '.'));
            if (!isNaN(areaPrivNum) && areaPrivNum > 0) {
                area = areaPrivNum;
            }
        }

        if (area === 0 && areaTerreno && areaTerreno !== 'N/A' && areaTerreno !== 'null') {
            const areaTerNum = parseFloat(areaTerreno.toString().replace(/[^\d.,]/g, '').replace(',', '.'));
            if (!isNaN(areaTerNum) && areaTerNum > 0) {
                area = areaTerNum;
            }
        }

        if (area <= 0) return 'N/A';

        const pricePerM2 = price / area;
        return `R$ ${pricePerM2.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    };

    const formatStatus = (status) => {
        if (!status) status = 'Existente';
        const statusClass = status.toLowerCase();
        return `<span class="badge status-${statusClass}">${status}</span>`;
    };

    const table = $('#imoveis-table').DataTable({
        processing: true,
        serverSide: false,
        ajax: {
            url: '/api/data',
            dataSrc: '',
            data: function(d) {
                return currentFilters;
            }
        },
        columns: [{
            data: 'UF',
            defaultContent: 'N/A'
        }, {
            data: 'CIDADE',
            defaultContent: 'N/A'
        }, {
            data: 'BAIRRO',
            defaultContent: 'N/A'
        }, {
            data: 'ENDERECO',
            defaultContent: 'N/A'
        }, {
            data: 'Status',
            defaultContent: 'Existente',
            render: function(data, type, row) {
                return formatStatus(data);
            }
        }, {
            data: 'PRECO',
            defaultContent: '0',
            render: function(data, type, row) {
                return `<span class="price-column">${formatCurrency(data)}</span>`;
            }
        }, {
            data: 'AVALIACAO',
            defaultContent: '0',
            render: function(data, type, row) {
                return `<span class="price-column">${formatCurrency(data)}</span>`;
            }
        }, {
            data: 'DESCONTO',
            defaultContent: '0%',
            render: function(data, type, row) {
                return `<span class="discount-column">${data || '0%'}</span>`;
            }
        }, {
            data: 'AREA_PRIVATIVA',
            defaultContent: 'N/A',
            render: function(data, type, row) {
                return `<span class="area-column">${formatArea(data)}</span>`;
            }
        }, {
            data: 'AREA_DO_TERRENO',
            defaultContent: 'N/A',
            render: function(data, type, row) {
                return `<span class="area-column">${formatArea(data)}</span>`;
            }
        }, {
            data: null,
            defaultContent: 'N/A',
            render: function(data, type, row) {
                const precoM2 = calculatePricePerM2(row.PRECO, row.AREA_PRIVATIVA, row.AREA_DO_TERRENO);
                return `<span class="preco-m2-column">${precoM2}</span>`;
            }
        }, {
            data: 'TIPO',
            defaultContent: 'N/A',
            render: function(data, type, row) {
                return `<span class="tipo-column">${data || 'N/A'}</span>`;
            }
        }, {
            data: 'MODALIDADE',
            defaultContent: 'N/A'
        }, {
            data: 'DATA_DISPUTA',
            defaultContent: 'N/A'
        }, {
            data: 'FGTS',
            defaultContent: 'NÃO',
            render: function(data, type, row) {
                const isFgts = data === 'SIM';
                const badgeClass = isFgts ? 'bg-success' : 'bg-secondary';
                const text = isFgts ? 'Sim' : 'Não';
                return `<span class="badge ${badgeClass}">${text}</span>`;
            }
        }, {
            data: 'FINANCIAMENTO',
            defaultContent: 'NÃO',
            render: function(data, type, row) {
                const isFinanciamento = data === 'SIM';
                const badgeClass = isFinanciamento ? 'bg-success' : 'bg-secondary';
                const text = isFinanciamento ? 'Sim' : 'Não';
                return `<span class="badge ${badgeClass}">${text}</span>`;
            }
        }],
        createdRow: function(row, data, dataIndex) {
            $(row).on('click', function() {
                if (data.LINK) {
                    window.open(data.LINK, '_blank');
                }
            });

            if (data.Status === 'Atualizado' && data.ChangedFields) {
                const changedFields = data.ChangedFields.split(',');
                const columnsMap = {
                    'UF': 0,
                    'CIDADE': 1,
                    'BAIRRO': 2,
                    'ENDERECO': 3,
                    'Status': 4,
                    'PRECO': 5,
                    'AVALIACAO': 6,
                    'DESCONTO': 7,
                    'AREA_PRIVATIVA': 8,
                    'AREA_DO_TERRENO': 9,
                    'TIPO': 11,
                    'MODALIDADE': 12,
                    'DATA_DISPUTA': 13,
                    'FGTS': 14,
                    'FINANCIAMENTO': 15
                };

                changedFields.forEach(fieldName => {
                    const trimmedField = fieldName.trim();
                    if (columnsMap.hasOwnProperty(trimmedField)) {
                        const colIndex = columnsMap[trimmedField];
                        $(row).find('td').eq(colIndex).addClass('cell-updated');
                    }
                });
            }
        },
        language: {
            url: '//cdn.datatables.net/plug-ins/1.13.7/i18n/pt-BR.json'
        },
        lengthChange: false,
        searching: false,
        pageLength: 25,
        order: [
            [5, 'asc']
        ],
        scrollX: true
    });

    function initializePriceSlider(minPrice, maxPrice) {
        if (priceSlider) {
            priceSlider.destroy();
        }

        const sliderElement = document.getElementById('price-range-slider');
        priceSlider = noUiSlider.create(sliderElement, {
            start: [minPrice, maxPrice],
            connect: true,
            range: {
                'min': minPrice,
                'max': maxPrice
            },
            format: {
                to: function(value) {
                    return Math.round(value);
                },
                from: function(value) {
                    return Number(value);
                }
            }
        });

        priceSlider.on('update', function(values, handle) {
            document.getElementById('price-min-value').textContent = formatCurrency(values[0]);
            document.getElementById('price-max-value').textContent = formatCurrency(values[1]);
        });
    }

    function updateFilters() {
        currentFilters = {
            status: $('#status-filter').val() || '',
            uf: $('#uf-filter').val() || '',
            cidade: $('#cidade-filter').val() || '',
            bairro: $('#bairro-filter').val() || '',
            tipo: $('#tipo-filter').val() || '',
            modalidade: $('#modalidade-filter').val() || '',
            fgts: $('#fgts-filter').val() || '',
            financiamento: $('#financiamento-filter').val() || '',
            data_inicio: $('#data-inicio-filter').val() || '',
            data_fim: $('#data-fim-filter').val() || ''
        };

        const precoMin = $('#preco-min-filter').val();
        const precoMax = $('#preco-max-filter').val();

        if (precoMin && precoMin !== '') {
            currentFilters.preco_min = parseFloat(precoMin);
        } else if (priceSlider) {
            const values = priceSlider.get();
            currentFilters.preco_min = parseFloat(values[0]);
        }

        if (precoMax && precoMax !== '') {
            currentFilters.preco_max = parseFloat(precoMax);
        } else if (priceSlider) {
            const values = priceSlider.get();
            currentFilters.preco_max = parseFloat(values[1]);
        }
    }

    $('#apply-filters').on('click', function() {

        updateFilters();
        table.ajax.reload();
    });

    $('#uf-filter').on('change', function() {
        const selectedUf = $(this).val();
        $('#cidade-filter').html('<option value="">Todas as Cidades</option>').val('');
        $('#bairro-filter').html('<option value="">Todos os Bairros</option>').val('');

        if (selectedUf) {
            $.get('/api/cidades_por_uf', {
                uf: selectedUf
            }, function(cidades) {
                cidades.forEach(cidade => {
                    $('#cidade-filter').append(`<option value="${cidade}">${cidade}</option>`);
                });
            });
        }
    });

    $('#cidade-filter').on('change', function() {
        const selectedCidade = $(this).val();
        const selectedUf = $('#uf-filter').val();
        $('#bairro-filter').html('<option value="">Todos os Bairros</option>').val('');

        if (selectedCidade) {
            $.get('/api/bairros_por_cidade', {
                cidade: selectedCidade,
                uf: selectedUf
            }, function(bairros) {
                bairros.forEach(bairro => {
                    $('#bairro-filter').append(`<option value="${bairro}">${bairro}</option>`);
                });
            });
        }
    });

    $('#start-scraping').on('click', function() {
        const selectedEstados = Array.from(document.querySelectorAll('#estados-pesquisa-container input:checked'))
            .map(cb => cb.value);

        if (selectedEstados.length === 0) {
            alert('Por favor, selecione pelo menos um estado.');
            return;
        }

        const modal = bootstrap.Modal.getInstance(document.getElementById('pesquisaModal'));
        modal.hide();

        progressBar.startScraping(selectedEstados);
    });

    function loadSummaryData() {
        $.get('/api/summary', function(data) {
            $('#summary-cards').html(`
                <div class="col-lg-2 col-md-4 col-sm-6 mb-3">
                    <div class="card kpi-card h-100">
                        <div class="card-body text-center">
                            <h5><i class="bi bi-house"></i> Total de Imóveis</h5>
                            <p>${data.stats.total_imoveis.toLocaleString('pt-BR')}</p>
                        </div>
                    </div>
                </div>
                <div class="col-lg-2 col-md-4 col-sm-6 mb-3">
                    <div class="card kpi-card h-100">
                        <div class="card-body text-center">
                            <h5><i class="bi bi-plus-circle"></i> Novos Imóveis</h5>
                            <p>${data.stats.novos_imoveis.toLocaleString('pt-BR')}</p>
                        </div>
                    </div>
                </div>
                <div class="col-lg-2 col-md-4 col-sm-6 mb-3">
                    <div class="card kpi-card h-100">
                        <div class="card-body text-center">
                            <h5><i class="bi bi-arrow-up-circle"></i> Atualizados</h5>
                            <p>${data.stats.atualizados.toLocaleString('pt-BR')}</p>
                        </div>
                    </div>
                </div>
                <div class="col-lg-2 col-md-4 col-sm-6 mb-3">
                    <div class="card kpi-card h-100">
                        <div class="card-body text-center">
                            <h5><i class="bi bi-x-circle"></i> Expirados</h5>
                            <p style="color: #e74c3c;">${data.stats.expirados.toLocaleString('pt-BR')}</p>
                        </div>
                    </div>
                </div>
                <div class="col-lg-4 col-md-8 col-sm-12 mb-3">
                    <div class="card kpi-card h-100">
                        <div class="card-body text-center">
                            <h5><i class="bi bi-check-circle"></i> Ativos</h5>
                            <p style="color: #2ecc71;">${data.stats.ativos.toLocaleString('pt-BR')}</p>
                        </div>
                    </div>
                </div>
            `);

            loadStatesSummary(data.uf_summary);
        });
    }

    function loadStatesSummary(ufSummary) {
        const container = $('#uf-summary-container').empty();

        if (!ufSummary || ufSummary.length === 0) {
            container.html('<p class="text-center text-muted">Nenhum dado disponível</p>');
            return;
        }

        const chunks = [
            ufSummary.slice(0, 9),
            ufSummary.slice(9, 18),
            ufSummary.slice(18, 27)
        ];

        chunks.forEach(chunk => {
            if (chunk.length > 0) {
                let tableHtml = `
                    <div class="col-md-4">
                        <table class="table table-dark table-sm">
                            <thead>
                                <tr>
                                    <th>UF</th>
                                    <th>Total</th>
                                    <th>Novos</th>
                                    <th>Atualizados</th>
                                    <th>Expirados</th>
                                </tr>
                            </thead>
                            <tbody>
                `;

                chunk.forEach(uf => {
                    tableHtml += `
                            <tr>
                                <td><strong>${uf.UF}</strong></td>
                                <td>${uf.Total.toLocaleString('pt-BR')}</td>
                                <td class="text-success">${(uf.Novos || 0).toLocaleString('pt-BR')}</td>
                                <td class="text-warning">${(uf.Atualizados || 0).toLocaleString('pt-BR')}</td>
                                <td class="text-danger">${(uf.Expirados || 0).toLocaleString('pt-BR')}</td>
                            </tr>
                    `;
                });

                tableHtml += '</tbody></table></div>';
                container.append(tableHtml);
            }
        });
    }

    function populateFilters() {
        $.get('/api/filters', function(data) {
            const filterMappings = {
                'uf': {
                    data: data.ufs,
                    placeholder: 'Todos os Estados'
                },
                'cidade': {
                    data: data.cidades,
                    placeholder: 'Todas as Cidades'
                },
                'bairro': {
                    data: data.bairros,
                    placeholder: 'Todos os Bairros'
                },
                'tipo': {
                    data: data.tipos,
                    placeholder: 'Todas as Tipologias'
                },
                'modalidade': {
                    data: data.modalidades,
                    placeholder: 'Todas as Modalidades'
                }
            };

            Object.keys(filterMappings).forEach(filterName => {
                const select = $(`#${filterName}-filter`);
                const mapping = filterMappings[filterName];
                select.html(`<option value="">${mapping.placeholder}</option>`);

                (mapping.data || []).forEach(option => {
                    select.append(`<option value="${option}">${option}</option>`);
                });
            });

            if (data.preco_range && data.preco_range.max > data.preco_range.min) {
                initializePriceSlider(data.preco_range.min, data.preco_range.max);
            } else {
                initializePriceSlider(0, 1000000);
            }
        });
    }

    function loadStatesForActions() {
        const allStates = ['AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'];

        $.get('/api/distinct_ufs', function(ufsFromDB) {
            const scrapingContainer = $('#estados-pesquisa-container');
            const exportContainer = $('#export-estados-container');

            scrapingContainer.empty();
            allStates.forEach(uf => {
                scrapingContainer.append(`
                    <div class="form-check">
                        <input class="form-check-input" type="checkbox" value="${uf}" id="scraping-${uf}">
                        <label class="form-check-label" for="scraping-${uf}">${uf}</label>
                    </div>
                `);
            });

            exportContainer.empty();
            if (ufsFromDB && ufsFromDB.length > 0) {
                ufsFromDB.forEach(uf => {
                    exportContainer.append(`
                        <div class="form-check">
                            <input class="form-check-input" type="checkbox" value="${uf}" id="export-${uf}">
                            <label class="form-check-label" for="export-${uf}">${uf}</label>
                        </div>
                    `);
                });
            } else {
                exportContainer.html('<p class="text-muted">Nenhum estado com dados para exportar.</p>');
            }
        });
    }

    function loadCheapProperties() {
        $.get('/api/imoveis_baratos', function(data) {
            const container = $('#vertical-carousel-inner').empty();

            if (data.length === 0) {
                container.html('<div class="text-center p-4"><p class="text-muted">Nenhuma oportunidade encontrada</p></div>');
                return;
            }

            let itemHtml = '';
            data.slice(0, 15).forEach(imovel => {
                itemHtml += `
                    <div class="mb-3">
                        <a href="${imovel.LINK}" target="_blank" class="text-decoration-none">
                            <div class="opportunity-card">
                                <h5>${formatCurrency(imovel.PRECO)}</h5>
                                <p><i class="bi bi-geo-alt"></i> ${imovel.CIDADE} - ${imovel.UF}</p>
                                <p><i class="bi bi-house"></i> ${imovel.TIPO || 'Imóvel'}</p>
                                <p><i class="bi bi-map"></i> ${imovel.BAIRRO || 'Bairro não informado'}</p>
                            </div>
                        </a>
                    </div>
                `;
            });

            container.html(itemHtml.repeat(3));
        });
    }

    $('#select-all-scraping-estados').on('change', function() {
        $('#estados-pesquisa-container input[type="checkbox"]').prop('checked', this.checked);
    });

    $('#select-all-export-estados').on('change', function() {
        $('#export-estados-container input[type="checkbox"]').prop('checked', this.checked);
    });

    $('#start-export').on('click', function() {
        const selectedEstados = Array.from(document.querySelectorAll('#export-estados-container input:checked'))
            .map(cb => cb.value);

        let downloadUrl = '/export/xlsx-hyperlink';
        if (selectedEstados.length > 0) {
            downloadUrl += '?estados=' + selectedEstados.join(',');
        }

        window.location.href = downloadUrl;

        const modal = bootstrap.Modal.getInstance(document.getElementById('exportModal'));
        modal.hide();
    });

    $('#start-upload').on('click', function() {
        const filesInput = document.getElementById('excel-files-input');
        const files = filesInput.files;
        const statusDiv = $('#upload-status');
        const uploadButton = $(this);

        if (files.length === 0) {
            statusDiv.html('<div class="alert alert-warning">Por favor, selecione pelo menos um arquivo.</div>');
            return;
        }

        const formData = new FormData();
        for (let i = 0; i < files.length; i++) {
            formData.append('files', files[i]);
        }

        statusDiv.html('<div class="d-flex align-items-center"><strong>Enviando e processando...</strong><div class="spinner-border ms-auto" role="status" aria-hidden="true"></div></div>');
        uploadButton.prop('disabled', true);

        $.ajax({
            url: '/upload_excel',
            type: 'POST',
            data: formData,
            processData: false,
            contentType: false,
            success: function(response) {
                let resultsHtml = `<div class="alert alert-success">${response.message}</div><ul>`;
                response.results.forEach(res => {
                    resultsHtml += `<li class="${res.success ? 'text-success' : 'text-danger'}"><strong>${res.file}:</strong> ${res.message}</li>`;
                });
                resultsHtml += '</ul>';
                statusDiv.html(resultsHtml);

                setTimeout(() => {
                    location.reload();
                }, 4000);
            },
            error: function(jqXHR, textStatus, errorThrown) {
                let errorMsg = 'Ocorreu um erro inesperado.';
                if (jqXHR.responseJSON && jqXHR.responseJSON.message) {
                    errorMsg = jqXHR.responseJSON.message;
                }
                statusDiv.html(`<div class="alert alert-danger">${errorMsg}</div>`);
                uploadButton.prop('disabled', false);
            }
        });
    });

    $('#clear-filters').on('click', function() {
        $('select, input[type="date"], input[type="number"]').val('');
        $('#status-filter').val('Ativos');
        $('#cidade-filter').html('<option value="">Todas as Cidades</option>');
        $('#bairro-filter').html('<option value="">Todos os Bairros</option>');

        if (priceSlider) {
            const range = priceSlider.options.range;
            priceSlider.set([range.min, range.max]);
        }

        updateFilters();
        table.ajax.reload();
    });

    updateFilters();
    loadSummaryData();
    populateFilters();
    loadStatesForActions();
    loadCheapProperties();
});