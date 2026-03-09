import requests
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURAÇÃO ---
UNIDADES = [
    {"sigla": "RT", "codigo": "158132"}, {"sigla": "AQ", "codigo": "158448"},
    {"sigla": "CG", "codigo": "158449"}, {"sigla": "CB", "codigo": "158450"},
    {"sigla": "CX", "codigo": "158451"}, {"sigla": "DR", "codigo": "155848"},
    {"sigla": "JD", "codigo": "155850"}, {"sigla": "NA", "codigo": "158452"},
    {"sigla": "NV", "codigo": "155849"}, {"sigla": "PP", "codigo": "158453"},
    {"sigla": "TL", "codigo": "158454"}
]

PASTA_DESTINO = "temp/contratos"
os.makedirs(PASTA_DESTINO, exist_ok=True)

# ======================================================
# 🛠️ FUNÇÕES DE APOIO
# ======================================================


def salvar_json_envelopado(nome_arquivo, url, dados, status="SUCESSO"):
    """Envelopa a resposta com metadados para garantir rastreabilidade."""
    caminho = os.path.join(PASTA_DESTINO, nome_arquivo)
    if status != "SUCESSO" and os.path.exists(caminho):
        return

    envelope = {
        "metadata": {
            "url_consultada": url,
            "data_extracao": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "status": status
        },
        "respostas": {"resultado": dados if dados is not None else []}
    }
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)

# ======================================================
# 📡 MÓDULOS DE EXTRAÇÃO
# ======================================================


def buscar_lista_contratos_por_ug(unidade):
    """Extrai a lista de contratos de uma UASG específica."""
    url = f"https://contratos.comprasnet.gov.br/api/contrato/ug/{unidade['codigo']}"
    try:
        res = requests.get(url, timeout=30)
        if res.status_code == 200:
            dados = res.json()
            if isinstance(dados, list):
                lista = dados
            elif "data" in dados:
                lista = dados["data"]
            elif "_embedded" in dados:
                lista = dados.get("_embedded", {}).get("contratos", [])
            else:
                lista = [dados]

            for c in lista:
                c['origem_sigla'] = unidade['sigla']
                c['origem_uasg'] = unidade['codigo']

            salvar_json_envelopado(
                f"contratos_uasg_{unidade['codigo']}.json", url, lista, "SUCESSO")
            return lista
    except:
        pass
    return []


def buscar_dados_filhos(args):
    """Módulo genérico para Itens e Responsáveis."""
    contrato_id, num_contrato, tipo = args
    url = f"https://contratos.comprasnet.gov.br/api/contrato/{contrato_id}/{tipo}"
    nome_arq = f"{tipo}_{contrato_id}.json"
    try:
        res = requests.get(url, timeout=30)
        if res.status_code == 200:
            dados = res.json()
            lista = dados if isinstance(dados, list) else [dados]
            for item in lista:
                item['id_contrato_origem'] = contrato_id
                item['numero_contrato_origem'] = num_contrato
            salvar_json_envelopado(nome_arq, url, lista, "SUCESSO")
            return "SUCCESS", len(lista)
    except:
        pass
    salvar_json_envelopado(nome_arq, url, None, "FALHA")
    return "FAILURE", 0

# ======================================================
# 🚀 ORQUESTRADOR PRINCIPAL
# ======================================================


def executar_esteira_contratos():
    total_falhas_global = 0
    total_itens_processados = 0
    total_responsaveis_processados = 0

    # --- FASE 1: CONTRATOS ---
    print(f"📡 Mapeando contratos das {len(UNIDADES)} UGs...")
    contratos_todos = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(
            buscar_lista_contratos_por_ug, u) for u in UNIDADES]
        for i, future in enumerate(as_completed(futures), 1):
            res = future.result()
            contratos_todos.extend(res)
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{timestamp}] 📦 Lote UGs: {i}/{len(UNIDADES)} | Contratos descobertos: {len(contratos_todos)}")

    total_c = len(contratos_todos)
    if total_c == 0:
        return

    print(
        f"\n🚀 INICIANDO | WORKERS: 15 | TOTAL CONTRATOS: {total_c}")

    # --- FASE 2: RESPONSÁVEIS E FASE 3: ITENS ---
    for tipo in ["responsaveis", "itens"]:
        print(f"\n📂 Processando {tipo.upper()}...")
        tarefas = [(c['id'], c.get('numero_contrato', 'S/N'), tipo)
                   for c in contratos_todos]

        sucesso, falha, contagem_entidades = 0, 0, 0
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(buscar_dados_filhos, t)
                       for t in tarefas]
            for i, future in enumerate(as_completed(futures), 1):
                status, qtd = future.result()
                contagem_entidades += qtd
                if status == "SUCCESS":
                    sucesso += 1
                else:
                    falha += 1

                if i % 100 == 0 or i == total_c:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    percent = (i / total_c) * 100
                    entidade_label = "Responsáveis" if tipo == "responsaveis" else "Itens"
                    print(
                        f"[{timestamp}] 📦 Lote: {i} | {entidade_label} extraídos: {contagem_entidades} | Falhas: {falha}")

        total_falhas_global += falha
        if tipo == "responsaveis":
            total_responsaveis_processados = contagem_entidades
        else:
            total_itens_processados = contagem_entidades

    print(f"\n{'='*60}")
    print(f"✅ FIM DO FLUXO.")
    print(f"📊 Total de Contratos: {total_c}")
    print(f"👥 Total de Responsáveis: {total_responsaveis_processados}")
    print(f"📦 Total de Itens: {total_itens_processados}")
    print(f"⚠️ Total de Falhas: {total_falhas_global}")
    print(f"{'='*60}")


if __name__ == "__main__":
    executar_esteira_contratos()
