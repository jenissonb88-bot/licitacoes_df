<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <title>Painel Sniper PNCP</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        body { background-color: #f4f7f6; font-size: 0.82rem; }
        .table thead { background-color: #0d6efd; color: white; font-size: 0.75rem; text-transform: uppercase; }
        .badge-itens { background-color: #6c757d; color: white; font-weight: bold; }
        .paginacao-controles { display: flex; justify-content: center; align-items: center; gap: 15px; margin: 20px 0; }
        .modal-xl { max-width: 95%; }
        .table-vencedor { border-left: 5px solid #198754 !important; background-color: #f0fff4 !important; }
    </style>
</head>
<body>

<div class="container-fluid py-3">
    <div class="d-flex justify-content-between align-items-center mb-3">
        <h4 class="text-primary fw-bold"><i class="fas fa-crosshairs"></i> Sniper PNCP - Saúde</h4>
        <span class="badge bg-dark">Total: <span id="total_count">0</span></span>
    </div>

    <div class="card shadow-sm border-0 overflow-hidden">
        <table class="table table-hover align-middle mb-0">
            <thead>
                <tr>
                    <th>Publicado</th>
                    <th>Abertura (Prazo)</th>
                    <th>UF / Cidade</th>
                    <th>Órgão / UASG</th>
                    <th>Edital / Itens</th>
                    <th style="width: 25%">Objeto</th>
                    <th>Valor Est.</th>
                    <th class="text-center">Ações</th>
                </tr>
            </thead>
            <tbody id="tableBody"></tbody>
        </table>
    </div>

    <div class="paginacao-controles">
        <button id="btnAnterior" class="btn btn-sm btn-outline-primary" onclick="mudarPagina(-1)"><i class="fas fa-arrow-left"></i> Anterior</button>
        <span id="infoPagina" class="fw-bold"></span>
        <button id="btnProximo" class="btn btn-sm btn-outline-primary" onclick="mudarPagina(1)">Próximo <i class="fas fa-arrow-right"></i></button>
    </div>
</div>

<div class="modal fade" id="modalItens" tabindex="-1">
    <div class="modal-dialog modal-xl">
        <div class="modal-content">
            <div class="modal-header bg-primary text-white py-2">
                <h6 class="modal-title">Detalhamento de Itens e Resultados</h6>
                <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal"></button>
            </div>
            <div class="modal-body">
                <div id="loader" class="text-center d-none my-5">
                    <div class="spinner-border text-primary" role="status"></div>
                    <p class="mt-2 text-muted">Consultando API do PNCP...</p>
                </div>
                <div class="table-responsive">
                    <table class="table table-sm table-bordered">
                        <thead class="table-light">
                            <tr>
                                <th>#</th>
                                <th>Descrição do Item / Vencedor</th>
                                <th>Qtd</th>
                                <th>Unitário (4c)</th>
                                <th>Total</th>
                                <th>Status</th>
                            </tr>
                        </thead>
                        <tbody id="itensBody"></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>

<script src="dados/oportunidades.js"></script>
<script>
    const itensPorPagina = 15;
    let paginaAtual = 1;
    const fmt2 = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' });
    const fmt4 = new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', minimumFractionDigits: 4 });

    function renderTable() {
        const tbody = document.getElementById('tableBody');
        tbody.innerHTML = '';
        const inicio = (paginaAtual - 1) * itensPorPagina;
        const fim = inicio + itensPorPagina;
        const lista = dadosLicitacoes.slice(inicio, fim);

        lista.forEach(item => {
            const dtPub = new Date(item.data_pub).toLocaleDateString();
            const dtAbr = item.data_abertura ? new Date(item.data_abertura).toLocaleString() : 'Não inf.';
            const isFutura = item.data_abertura && new Date(item.data_abertura) > new Date();

            tbody.innerHTML += `
                <tr>
                    <td><small>${dtPub}</small></td>
                    <td><span class="badge ${isFutura ? 'bg-success' : 'bg-secondary'}">${dtAbr}</span></td>
                    <td><b>${item.uf}</b><br><small>${item.cidade}</small></td>
                    <td><div class="fw-bold text-truncate" style="max-width: 250px">${item.orgao}</div><small class="text-muted">UASG: ${item.uasg}</small></td>
                    <td><b>${item.numero}</b><br><span class="badge badge-itens mt-1">${item.quantidade_itens} itens</span></td>
                    <td><small>${item.objeto.substring(0, 100)}...</small></td>
                    <td class="fw-bold text-primary">${fmt2.format(item.valor_total)}</td>
                    <td class="text-center">
                        <button class="btn btn-sm btn-primary" onclick='verItens(${JSON.stringify(item.api_params)})'>Itens</button>
                        <a href="${item.link_pncp}" target="_blank" class="btn btn-sm btn-outline-secondary"><i class="fas fa-link"></i></a>
                    </td>
                </tr>`;
        });

        document.getElementById('total_count').innerText = dadosLicitacoes.length;
        document.getElementById('infoPagina').innerText = `Pág. ${paginaAtual} de ${Math.ceil(dadosLicitacoes.length / itensPorPagina)}`;
        document.getElementById('btnAnterior').disabled = (paginaAtual === 1);
        document.getElementById('btnProximo').disabled = (fim >= dadosLicitacoes.length);
    }

    function mudarPagina(v) { paginaAtual += v; renderTable(); window.scrollTo(0,0); }

    async function verItens(p) {
        const modal = new bootstrap.Modal(document.getElementById('modalItens'));
        const body = document.getElementById('itensBody');
        const loader = document.getElementById('loader');
        body.innerHTML = ''; loader.classList.remove('d-none'); modal.show();

        const base = `https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao/${p.cnpj}/${p.ano}/${p.seq}`;
        try {
            const [resI, resR] = await Promise.all([
                fetch(`${base}/itens?pagina=1&tamanhoPagina=100`),
                fetch(`${base}/resultados?pagina=1&tamanhoPagina=100`)
            ]);
            const itens = await resI.json();
            const results = resR.ok ? await resR.json() : [];
            const mapaV = {}; results.forEach(r => mapaV[r.numeroItem] = r);

            loader.classList.add('d-none');
            itens.forEach(i => {
                const v = mapaV[i.numeroItem];
                body.innerHTML += `
                    <tr class="${v ? 'table-vencedor' : ''}">
                        <td>${i.numeroItem}</td>
                        <td><b>${i.descricao}</b>${v ? `<br><small class="text-success">🏆 ${v.nomeRazaoSocialFornecedor}</small>` : ''}</td>
                        <td>${i.quantidade}</td>
                        <td>${v ? fmt4.format(v.valorUnitarioHomologado) : fmt4.format(i.valorUnitarioEstimado)}</td>
                        <td>${v ? fmt2.format(v.valorTotalHomologado) : fmt2.format(i.valorTotalEstimado)}</td>
                        <td><span class="badge ${v ? 'bg-success' : 'bg-secondary'}">${v ? 'Finalizado' : 'Aberto'}</span></td>
                    </tr>`;
            });
        } catch (e) { loader.innerHTML = 'Erro na API.'; }
    }

    window.onload = () => { if(typeof dadosLicitacoes !== 'undefined') renderTable(); };
</script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
