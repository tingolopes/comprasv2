import requests
import os
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

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


def salvar_json_envelopado(nome_arquivo, url, dados, status="SUCESSO"):
    """Envelopa a resposta com metadados antes de salvar."""
    caminho = os.path.join(PASTA_DESTINO, nome_arquivo)

    # Trava de Segurança: Não sobrescreve cache bom com erro
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


def extrair_filhos(contrato_id, num_contrato, tipo):
    """Extrai itens ou responsáveis de um contrato específico."""
    url = f"https://contratos.comprasnet.gov.br/api/contrato/{contrato_id}/{tipo}"
    nome_arq = f"{tipo}_{contrato_id}.json"

    try:
        res = requests.get(url, timeout=30)
        if res.status_code == 200:
            dados = res.json()
            # Garante que seja lista e injeta IDs de origem
            lista = dados if isinstance(dados, list) else [dados]
            for item in lista:
                item['id_contrato_origem'] = contrato_id
                item['numero_contrato_origem'] = num_contrato

            salvar_json_envelopado(nome_arq, url, lista, "SUCESSO")
            return True
        else:
            salvar_json_envelopado(nome_arq, url, None,
                                   f"ERRO_{res.status_code}")
    except Exception as e:
        salvar_json_envelopado(nome_arq, url, None, f"FALHA_{str(e)}")
    return False


def extrair_contratos_ug(unidade):
    """Extrai a lista de contratos de uma UASG."""
    url = f"https://contratos.comprasnet.gov.br/api/contrato/ug/{unidade['codigo']}"
    nome_arq = f"contratos_uasg_{unidade['codigo']}.json"

    try:
        res = requests.get(url, timeout=30)
        if res.status_code == 200:
            dados = res.json()
            # Normaliza estrutura da API de contratos
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
            return lista
    except:
        pass
    return []


def executar_esteira_contratos():
    print(f"🚀 [1/3] Extraindo Listas de Contratos ({len(UNIDADES)} UGs)...")
    contratos_todos = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        resultados = list(executor.map(extrair_contratos_ug, UNIDADES))

    for r in resultados:
        contratos_todos.extend(r)

    if not contratos_todos:
        print("❌ Nenhum contrato localizado.")
        return

    # Prepara tarefas para Responsáveis e Itens
    tarefas_responsaveis = [(c['id'], c.get(
        'numero_contrato', 'S/N'), "responsaveis") for c in contratos_todos]
    tarefas_itens = [(c['id'], c.get('numero_contrato', 'S/N'), "itens")
                     for c in contratos_todos]

    print(
        f"🚀 [2/3] Extraindo Responsáveis para {len(contratos_todos)} contratos...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda p: extrair_filhos(*p), tarefas_responsaveis)

    print(f"🚀 [3/3] Extraindo Itens para {len(contratos_todos)} contratos...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda p: extrair_filhos(*p), tarefas_itens)

    print(f"✨ Extração finalizada. Arquivos salvos em {PASTA_DESTINO}")


if __name__ == "__main__":
    executar_esteira_contratos()
