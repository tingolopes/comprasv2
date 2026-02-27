import requests
import os
import time
import json
import sys
from datetime import datetime
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

# --- CONFIGURAÇÃO ---
UASGS = [
    {"sigla": "RT", "codigo": "158132"}
]

PASTA_ATAS = "temp/temp_atas_arp"
BASE_URL = "https://dadosabertos.compras.gov.br/modulo-arp/1_consultarARP"
ANOS = [2023, 2024, 2025, 2026]
MAX_WORKERS = 10

os.makedirs(PASTA_ATAS, exist_ok=True)


def verificar_sucesso(caminho):
    """Verifica se o arquivo já existe e se o status foi SUCESSO."""
    if not os.path.exists(caminho):
        return False, None
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            data = json.load(f)
            status = data.get("metadata", {}).get("status")
            return (status == "SUCESSO"), data
    except:
        return False, None


def salvar_dados(caminho, url_base, params, conteudo, status="SUCESSO"):
    """Salva o JSON com a URL consultada e metadados."""
    url_completa = f"{url_base}?{urlencode(params)}"
    envelope = {
        "metadata": {
            "url_consultada": url_completa,
            "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status
        },
        "respostas": conteudo
    }
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)


def processar_tarefa(t):
    """Função executada pelas threads para baixar uma página específica."""
    arquivo = f"{PASTA_ATAS}/atas_{t['sigla']}_{t['ano']}_p{t['pagina']}.json"

    sucesso, dados_cache = verificar_sucesso(arquivo)
    if sucesso:
        paginas_restantes = dados_cache.get(
            "respostas", {}).get("paginasRestantes", 0)
        return f"⏭️ SKIP | {t['sigla']} | {t['ano']} | Pág {t['pagina']}", paginas_restantes

    params = {
        "pagina": t['pagina'],
        "tamanhoPagina": 500,
        "codigoUnidadeGerenciadora": t['codigo_uasg'],
        "dataVigenciaInicialMin": f"{t['ano']}-01-01",
        "dataVigenciaInicialMax": f"{t['ano']}-12-31"
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        if response.status_code == 200:
            dados = response.json()
            salvar_dados(arquivo, BASE_URL, params, dados, "SUCESSO")
            paginas_restantes = dados.get('paginasRestantes', 0)
            return f"✅ DONE | {t['sigla']} | {t['ano']} | Pág {t['pagina']}", paginas_restantes
        else:
            status_erro = f"FALHA: Erro {response.status_code}"
            salvar_dados(arquivo, BASE_URL, params, None, status_erro)
            return f"❌ ERRO {response.status_code} | {t['sigla']} | {t['ano']}", 0
    except Exception as e:
        salvar_dados(arquivo, BASE_URL, params, None, f"FALHA: {str(e)}")
        return f"💥 FALHA | {t['sigla']} | {t['ano']} | {str(e)}", 0


def executar_parallel():
    # Prepara a fila inicial
    fila_inicial = []
    for unidade in UASGS:
        for ano in ANOS:
            fila_inicial.append({
                "sigla": unidade['sigla'],
                "codigo_uasg": unidade['codigo'],
                "ano": ano,
                "pagina": 1
            })

    total_tarefas = len(fila_inicial)
    concluidas = 0
    falhas_restantes = 0

    print(f"🚀 INICIANDO EXTRAÇÃO DE ATAS (TURBO {MAX_WORKERS} THREADS)")
    print(f"📊 CONSULTAS INICIAIS: {total_tarefas}\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(processar_tarefa, t)
                                   : t for t in fila_inicial}

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                t_info = futures.pop(future)
                concluidas += 1

                resultado, pag_rest = future.result()

                # Contabiliza falhas para o exit status
                if "❌" in resultado or "💥" in resultado:
                    falhas_restantes += 1

                # Log padronizado com percentual
                percentual = (concluidas / total_tarefas) * 100
                timestamp = datetime.now().strftime('%H:%M:%S')
                print(
                    f"[{timestamp}] {resultado} | {concluidas}/{total_tarefas} ({percentual:.1f}%)")

                # Se houver mais páginas, adiciona à fila e aumenta o total_tarefas
                if pag_rest > 0:
                    nova_tarefa = t_info.copy()
                    nova_tarefa['pagina'] += 1
                    futures[executor.submit(
                        processar_tarefa, nova_tarefa)] = nova_tarefa
                    total_tarefas += 1

    print(
        f"\n✅ Processo de Atas concluído em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")

    if falhas_restantes > 0:
        print(f"⚠️  Ciclo finalizado com {falhas_restantes} falhas pendentes.")
        sys.exit(1)
    else:
        print("🎉 TUDO CONCLUÍDO COM SUCESSO!")
        sys.exit(0)


if __name__ == "__main__":
    executar_parallel()
