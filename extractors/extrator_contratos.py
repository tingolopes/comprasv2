import requests
import os
import json
from datetime import datetime, timedelta
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

PASTA_DESTINO = "temp/temp_contratos"
os.makedirs(PASTA_DESTINO, exist_ok=True)

# Regras de validade
DIAS_VALIDADE = {
    "contratos": 1,
    "responsaveis": 1,
    "itens": 99
}

# ======================================================
# 🛠️ FUNÇÕES DE APOIO
# ======================================================


def caminho_arquivo(nome_arquivo):
    return os.path.join(PASTA_DESTINO, nome_arquivo)


def carregar_envelope(nome_arquivo):
    caminho = caminho_arquivo(nome_arquivo)
    if not os.path.exists(caminho):
        return None
    try:
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def cache_valido(nome_arquivo, dias_validade):
    """
    Cache é válido quando:
    - arquivo existe
    - metadata.status == SUCESSO
    - data_extracao está dentro da janela de validade
    """
    envelope = carregar_envelope(nome_arquivo)
    if not envelope:
        return False

    metadata = envelope.get("metadata", {})
    if metadata.get("status") != "SUCESSO":
        return False

    data_str = metadata.get("data_extracao")
    if not data_str:
        return False

    try:
        data_extracao = datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S")
    except Exception:
        return False

    limite = datetime.now() - timedelta(days=dias_validade)
    return data_extracao >= limite


def salvar_json_envelopado(nome_arquivo, url, dados, status="SUCESSO"):
    """Envelopa a resposta com metadados para garantir rastreabilidade."""
    caminho = caminho_arquivo(nome_arquivo)
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
    nome_arq = f"contratos_uasg_{unidade['codigo']}.json"

    # CONTRATOS: validade diária
    if cache_valido(nome_arq, DIAS_VALIDADE["contratos"]):
        envelope = carregar_envelope(nome_arq) or {}
        lista = envelope.get("respostas", {}).get("resultado", [])
        return "SKIP", lista

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

            salvar_json_envelopado(nome_arq, url, lista, "SUCESSO")
            return "SUCCESS", lista
    except Exception:
        pass

    salvar_json_envelopado(nome_arq, url, None, "FALHA")
    return "FAILURE", []


def buscar_dados_filhos(args):
    """Módulo genérico para Itens e Responsáveis."""
    contrato_id, num_contrato, tipo = args
    url = f"https://contratos.comprasnet.gov.br/api/contrato/{contrato_id}/{tipo}"
    nome_arq = f"{tipo}_{contrato_id}.json"

    # RESPONSÁVEIS: 1 dia | ITENS: 3 dias
    dias_validade = DIAS_VALIDADE["responsaveis"] if tipo == "responsaveis" else DIAS_VALIDADE["itens"]

    if cache_valido(nome_arq, dias_validade):
        return "SKIP", 0

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
    except Exception:
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
            status, res = future.result()
            contratos_todos.extend(res)
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(
                f"[{timestamp}] 📦 Lote UGs: {i}/{len(UNIDADES)} | Status: {status:<7} | Contratos acumulados: {len(contratos_todos)}"
            )

    # evita duplicidade de contratos quando há mistura de cache + atualização
    vistos = set()
    contratos_unicos = []
    for c in contratos_todos:
        chave = c.get("id")
        if chave and chave not in vistos:
            vistos.add(chave)
            contratos_unicos.append(c)

    contratos_todos = contratos_unicos
    total_c = len(contratos_todos)
    if total_c == 0:
        print("ℹ️ Nenhum contrato encontrado.")
        return

    print(f"\n🚀 INICIANDO | WORKERS: 15 | TOTAL CONTRATOS: {total_c}")

    # --- FASE 2: RESPONSÁVEIS E FASE 3: ITENS ---
    for tipo in ["responsaveis", "itens"]:
        print(f"\n📂 Processando {tipo.upper()}...")
        tarefas = [(c['id'], c.get('numero_contrato', 'S/N'), tipo)
                   for c in contratos_todos]

        sucesso, falha, skip, contagem_entidades = 0, 0, 0, 0
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(buscar_dados_filhos, t)
                       for t in tarefas]
            for i, future in enumerate(as_completed(futures), 1):
                status, qtd = future.result()
                contagem_entidades += qtd

                if status == "SUCCESS":
                    sucesso += 1
                elif status == "FAILURE":
                    falha += 1
                else:
                    skip += 1

                if i % 100 == 0 or i == total_c:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    percent = (i / total_c) * 100
                    entidade_label = "Responsáveis" if tipo == "responsaveis" else "Itens"
                    print(
                        f"[{timestamp}] 📦 Lote: {i}/{total_c} ({percent:.1f}%) | "
                        f"{entidade_label} extraídos: {contagem_entidades} | "
                        f"Sucesso: {sucesso} | Skip: {skip} | Falhas: {falha}"
                    )

        total_falhas_global += falha
        if tipo == "responsaveis":
            total_responsaveis_processados = contagem_entidades
        else:
            total_itens_processados = contagem_entidades

    print(f"\n{'='*60}")
    print("✅ FIM DO FLUXO.")
    print(f"📊 Total de Contratos: {total_c}")
    print(f"👥 Total de Responsáveis: {total_responsaveis_processados}")
    print(f"📦 Total de Itens: {total_itens_processados}")
    print(f"⚠️ Total de Falhas: {total_falhas_global}")
    print(f"{'='*60}")


if __name__ == "__main__":
    executar_esteira_contratos()
