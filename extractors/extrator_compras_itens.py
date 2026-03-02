import requests
import os
import time
import json
import sys
from datetime import datetime
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURAÇÃO PADRONIZADA ---
PASTAS = {
    "C_LEGADO": "temp/temp_compras_legado",
    "C_PNCP": "temp/temp_compras_14133",
    "I_LEGADO": "temp/temp_itens_legado_id",
    "I_PNCP": "temp/temp_itens_14133_id"
}
# Reduzido para 3 para manter o padrão de sucesso sem Erro 429
MAX_WORKERS = 3
BASE_URL = "https://dadosabertos.compras.gov.br"

for p in ["I_LEGADO", "I_PNCP"]:
    os.makedirs(PASTAS[p], exist_ok=True)


def carregar_json_seguro(caminho):
    """Escudo contra arquivos corrompidos ou NoneType."""
    if not os.path.exists(caminho):
        return {}
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            dados = json.load(f)
            return dados if dados is not None else {}
    except Exception as exc:
        print(f"⚠️ Erro ao carregar JSON {caminho}: {exc}")
        return {}


def verificar_sucesso(caminho):
    dados = carregar_json_seguro(caminho)
    status = dados.get("metadata", {}).get("status") == "SUCESSO"
    return status, dados


def salvar_json(caminho, url_base, params, conteudo, status="SUCESSO"):
    envelope = {
        "metadata": {
            "url_consultada": f"{url_base}?{urlencode(params)}",
            "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status
        },
        "respostas": conteudo
    }
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)


def processar_uma_tarefa(t):
    pagina = 1
    while True:
        nome_arq = f"{t['pasta']}/itens_{t['id']}_{t['sufixo']}_p{pagina}.json"
        sucesso, dados_cache = verificar_sucesso(nome_arq)

        if sucesso:
            # Se não for paginável ou já terminou as páginas
            if not t['paginavel'] or dados_cache.get("respostas", {}).get("paginasRestantes", 0) == 0:
                return f"⏭️ SKIP | {t['id']}"
            pagina += 1
            continue

        url_full = f"{BASE_URL}{t['path']}"
        params = t['params'].copy()
        if t['paginavel']:
            params.update({"pagina": pagina, "tamanhoPagina": 500})

        status = "FALHA"
        dados = None

        # BACKOFF: Tentativas contra Erro 429
        for tentativa in range(3):
            try:
                response = requests.get(url_full, params=params, timeout=30)
                if response.status_code == 200:
                    dados = response.json()
                    status = "SUCESSO"
                    break
                elif response.status_code == 429:
                    time.sleep(15 * (tentativa + 1))  # Espera progressiva
                else:
                    status = f"ERRO_{response.status_code}"
            except Exception as exc:
                print(f"⚠️ Tentativa {tentativa + 1} falhou para {url_full}: {exc}")
            time.sleep(2)

        salvar_json(nome_arq, url_full, params, dados, status)

        if status == "SUCESSO" and t['paginavel']:
            # Verifica se precisa de mais páginas
            pag_rest = dados.get('respostas', {}).get(
                'paginasRestantes', 0) if dados else 0
            if pag_rest > 0:
                pagina += 1
                continue

        return f"{'✅' if status == 'SUCESSO' else '❌'} {status} | {t['id']}"


def montar_fila():
    tarefas = []
    print("🔍 Mapeando arquivos para montar a fila de itens...")

    # Processamento Legado
    if os.path.exists(PASTAS["C_LEGADO"]):
        for arq in os.listdir(PASTAS["C_LEGADO"]):
            dados_c = carregar_json_seguro(
                os.path.join(PASTAS["C_LEGADO"], arq))
            url_orig = dados_c.get("metadata", {}).get("url_consultada", "")

            # PROTEÇÃO contra o erro de 'NoneType' que você teve
            respostas = dados_c.get("respostas", {})
            if not isinstance(respostas, dict):
                respostas = {}

            for c in respostas.get("resultado", []):
                id_c = c.get("id_compra") or c.get("idCompra")
                if not id_c:
                    continue

                tarefas.append({"id": id_c, "pasta": PASTAS["I_LEGADO"], "paginavel": False, "sufixo": "E2",
                                "path": "/modulo-legado/2.1_consultarItemLicitacao_Id", "params": {"id_compra": id_c}})

                if "3_consultarPregoes" in url_orig:
                    tarefas.append({"id": id_c, "pasta": PASTAS["I_LEGADO"], "paginavel": False, "sufixo": "E4",
                                    "path": "/modulo-legado/4.1_consultarItensPregoes_Id", "params": {"id_compra": id_c}})

    # Processamento PNCP
    if os.path.exists(PASTAS["C_PNCP"]):
        for arq in os.listdir(PASTAS["C_PNCP"]):
            dados_c = carregar_json_seguro(os.path.join(PASTAS["C_PNCP"], arq))
            respostas = dados_c.get("respostas", {})
            if not isinstance(respostas, dict):
                respostas = {}

            for c in respostas.get("resultado", []):
                id_c = c.get("idCompra") or c.get("id_compra")
                if id_c:
                    tarefas.append({"id": id_c, "pasta": PASTAS["I_PNCP"], "paginavel": True, "sufixo": "pncp",
                                    "path": "/modulo-contratacoes/2.1_consultarItensContratacoes_PNCP_14133_Id",
                                    "params": {"tipo": "idCompra", "codigo": id_c}})
    return tarefas


if __name__ == "__main__":
    lista = montar_fila()
    total = len(lista)
    concluidas = 0
    erros_count = 0

    print(
        f"\n🚀 INICIANDO TURBO BLINDADO | WORKERS: {MAX_WORKERS} | TOTAL: {total}\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        try:
            futures = {executor.submit(
                processar_uma_tarefa, t): t for t in lista}
            for future in as_completed(futures):
                concluidas += 1
                res = future.result()

                if "❌" in res or "FALHA" in res:
                    erros_count += 1

                perc = (concluidas / total) * 100
                ts = datetime.now().strftime('%H:%M:%S')
                print(
                    f"[{ts}] {res} | Progresso: {concluidas}/{total} ({perc:.1f}%) | Falhas: {erros_count}")

        except KeyboardInterrupt:
            print("\n🛑 INTERRUPÇÃO! Parando...")
            executor.shutdown(wait=False, cancel_futures=True)
            sys.exit(0)

    print(f"\n✅ FIM DO FLUXO. Total de falhas registradas: {erros_count}")
