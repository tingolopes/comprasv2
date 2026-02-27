import requests
import os
import time
import json
import sys
from datetime import datetime
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor  # Para o Nível Superior

# --- CONFIGURAÇÕES MANTIDAS ---
UASGS = [
    {"sigla": "RT", "codigo": "158132"}, {"sigla": "AQ", "codigo": "158448"},
    {"sigla": "CG", "codigo": "158449"}, {"sigla": "CB", "codigo": "158450"},
    {"sigla": "CX", "codigo": "158451"}, {"sigla": "DR", "codigo": "155848"},
    {"sigla": "JD", "codigo": "155850"}, {"sigla": "NA", "codigo": "158452"},
    {"sigla": "NV", "codigo": "155849"}, {"sigla": "PP", "codigo": "158453"},
    {"sigla": "TL", "codigo": "158454"}
]

CONFIG_APIS = {
    "LEGADO": {
        "base_url": "https://dadosabertos.compras.gov.br",
        "pasta": "temp/temp_compras_legado",
        "anos": list(range(2016, datetime.now().year + 1)),
        "uasgs": UASGS,
        "endpoints": [
            {"label": "outrasmodalidades", "path": "/modulo-legado/1_consultarLicitacao",
                "p_uasg": "uasg", "p_data": "data_publicacao"},
            {"label": "pregao", "path": "/modulo-legado/3_consultarPregoes",
                "p_uasg": "co_uasg", "p_data": "dt_data_edital"},
            {"label": "dispensa", "path": "/modulo-legado/5_consultarComprasSemLicitacao",
                "p_uasg": "co_uasg", "p_data": None}
        ]
    },
    "LEI14133": {
        "base_url": "https://dadosabertos.compras.gov.br",
        "pasta": "temp/temp_compras_14133",
        "anos": list(range(2021, datetime.now().year + 1)),
        "uasgs": [u for u in UASGS if u["sigla"] == "RT"],
        "modalidades": {3: "concorrencia", 5: "pregao", 6: "dispensa", 7: "inexigibilidade"},
        "path": "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133"
    }
}

# --- FUNÇÕES DE APOIO MANTIDAS ---


def verificar_sucesso_anterior(caminho):
    if not os.path.exists(caminho):
        return False, None
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            data = json.load(f)
            status = data.get("metadata", {}).get("status")
            return (status == "SUCESSO"), data
    except:
        return False, None


def deve_reverificar_pncp(dados_cache, dias_validade=7):
    respostas = dados_cache.get("respostas", {})
    resultados = respostas.get("resultado", [])
    if not resultados:
        return False
    status_finais = [3, 4, 5]
    eh_volatil = any(compra.get("situacaoCompraIdPncp")
                     not in status_finais for compra in resultados)
    if eh_volatil:
        data_ext_str = dados_cache.get("metadata", {}).get("data_extracao")
        data_extracao = datetime.strptime(data_ext_str, "%Y-%m-%d %H:%M:%S")
        if (datetime.now() - data_extracao).days >= dias_validade:
            return True
    return False


def salvar_dados(caminho, url_base, params, conteudo, status="SUCESSO"):
    url_consultada = f"{url_base}?{urlencode(params)}"
    envelope = {
        "metadata": {
            "url_consultada": url_consultada,
            "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": status
        },
        "respostas": conteudo
    }
    with open(caminho, 'w', encoding='utf-8') as f:
        json.dump(envelope, f, ensure_ascii=False, indent=4)

# --- MELHORIA: CONSULTA ROBUSTA COM BACKOFF EXPONENCIAL ---


def consultar_api_robusto(url, params):
    atraso = 2  # Começa com 2 segundos
    for tentativa in range(1, 3):  # 2 tentativas: 2s e 4s
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                dados = response.json()
                if isinstance(dados, dict) and "resultado" in dados:
                    return dados, "SUCESSO"
        except:
            pass
        time.sleep(atraso)
        atraso *= 2  # Backoff: 2s -> 4s
    return None, "FALHA"

# --- PROCESSADORES PARA THREADS ---


def processar_tarefa_legado(unidade, ano, enc, cfg):
    pagina = 1
    while True:
        arquivo = f"{cfg['pasta']}/{enc['label']}_{unidade['sigla']}_{ano}_p{pagina}.json"
        ja_existe, dados_cache = verificar_sucesso_anterior(arquivo)
        tstamp = datetime.now().strftime('%H:%M:%S')

        if ja_existe:
            respostas = dados_cache.get("respostas", {})
            if not respostas.get("resultado", []):
                break
            print(
                f"[{tstamp}] ⏭️ SKIP | {unidade['sigla']} | {enc['label'].upper():<15} | {ano}")
            if respostas.get('paginasRestantes', 0) > 0:
                pagina += 1
                continue
            else:
                break

        params = {"pagina": pagina, "tamanhoPagina": 500,
                  enc['p_uasg']: unidade['codigo']}
        if enc['label'] == "dispensa":
            params["dt_ano_aviso"] = ano
        else:
            params[f"{enc['p_data']}_inicial"] = f"{ano}-01-01"
            params[f"{enc['p_data']}_final"] = f"{ano}-12-31"

        dados, status = consultar_api_robusto(
            f"{cfg['base_url']}{enc['path']}", params)
        salvar_dados(
            arquivo, f"{cfg['base_url']}{enc['path']}", params, dados, status)

        if status == "SUCESSO":
            print(
                f"[{tstamp}] ✅ DONE | {unidade['sigla']} | {enc['label'].upper():<15} | {ano}")
            if dados.get('paginasRestantes', 0) > 0:
                pagina += 1
                continue
            else:
                break
        else:
            print(
                f"[{tstamp}] ❌ FAIL | {unidade['sigla']} | {enc['label'].upper():<15} | {ano}")
            return False
    return True


def processar_tarefa_14133(unidade, ano, cod_mod, nome_mod, cfg):
    pagina = 1
    while True:
        arquivo = f"{cfg['pasta']}/pncp_{unidade['sigla']}_{nome_mod}_{ano}_p{pagina}.json"
        ja_existe, dados_cache = verificar_sucesso_anterior(arquivo)
        tstamp = datetime.now().strftime('%H:%M:%S')

        if ja_existe:
            respostas = dados_cache.get("respostas", {})
            if not respostas.get("resultado", []) or not deve_reverificar_pncp(dados_cache):
                print(
                    f"[{tstamp}] ⏭️ SKIP | {unidade['sigla']} | PNCP-{nome_mod.upper():<10} | {ano}")
                if respostas.get('paginasRestantes', 0) > 0 and respostas.get("resultado", []):
                    pagina += 1
                    continue
                else:
                    break

        params = {"pagina": pagina, "tamanhoPagina": 500, "unidadeOrgaoCodigoUnidade": unidade['codigo'],
                  "dataPublicacaoPncpInicial": f"{ano}-01-01", "dataPublicacaoPncpFinal": f"{ano}-12-31", "codigoModalidade": cod_mod}

        dados, status = consultar_api_robusto(
            f"{cfg['base_url']}{cfg['path']}", params)
        salvar_dados(
            arquivo, f"{cfg['base_url']}{cfg['path']}", params, dados, status)

        if status == "SUCESSO":
            print(
                f"[{tstamp}] ✅ DONE | {unidade['sigla']} | PNCP-{nome_mod.upper():<10} | {ano}")
            if dados.get('paginasRestantes', 0) > 0:
                pagina += 1
                continue
            else:
                break
        else:
            print(
                f"[{tstamp}] ❌ FAIL | {unidade['sigla']} | PNCP-{nome_mod.upper():<10} | {ano}")
            return False
    return True

# --- MOTOR PRINCIPAL ---


def executar_extracao_completa():
    print(f"🚀 INICIANDO EXTRAÇÃO DE COMPRAS (MULTI-THREADING)... \n")
    falhas_totais = 0

    # MOTOR 1: LEGADO
    cfg_l = CONFIG_APIS["LEGADO"]
    os.makedirs(cfg_l["pasta"], exist_ok=True)
    # 3 UASGs por vez para evitar bloqueio
    with ThreadPoolExecutor(max_workers=3) as executor:
        futuros = []
        for unidade in cfg_l["uasgs"]:
            for ano in cfg_l["anos"]:
                for enc in cfg_l["endpoints"]:
                    futuros.append(executor.submit(
                        processar_tarefa_legado, unidade, ano, enc, cfg_l))
        for f in futuros:
            if not f.result():
                falhas_totais += 1

    # MOTOR 2: LEI 14133
    cfg_n = CONFIG_APIS["LEI14133"]
    os.makedirs(cfg_n["pasta"], exist_ok=True)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futuros = []
        for unidade in cfg_n["uasgs"]:
            for ano in cfg_n["anos"]:
                for cod_mod, nome_mod in cfg_n["modalidades"].items():
                    futuros.append(executor.submit(
                        processar_tarefa_14133, unidade, ano, cod_mod, nome_mod, cfg_n))
        for f in futuros:
            if not f.result():
                falhas_totais += 1

    sys.exit(1 if falhas_totais > 0 else 0)


if __name__ == "__main__":
    executar_extracao_completa()
