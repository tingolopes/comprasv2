import requests
import os
import time
import json
import sys
from datetime import datetime
from urllib.parse import urlencode, quote
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED

# --- CONFIGURAÇÃO ---
PASTAS = {
    "C_ATAS": "temp/temp_atas_arp",
    "I_ATAS": "temp/temp_atas_itens_id",
    "S_ATAS": "temp/temp_atas_saldos_id",
    "U_ATAS": "temp/temp_atas_unidades_id"
}
# Reduzido de 10 para 3 para evitar o erro 429 (Too Many Requests)
MAX_WORKERS = 3
BASE_URL = "https://dadosabertos.compras.gov.br"
DIAS_VALIDADE_SALDO = 3
LOG_INTERVALO_SKIP = 100

for p in PASTAS.values():
    os.makedirs(p, exist_ok=True)


def verificar_sucesso(caminho, forcar_atualizacao=False):
    if not os.path.exists(caminho):
        return False, None
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            data = json.load(f)
            status_ok = (data.get("metadata", {}).get("status") == "SUCESSO")
            if not status_ok:
                return False, None

            if forcar_atualizacao:
                data_ext_str = data.get("metadata", {}).get("data_extracao")
                data_extracao = datetime.strptime(
                    data_ext_str, "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - data_extracao).days >= DIAS_VALIDADE_SALDO:
                    return False, None
            return True, data
    except Exception as exc:
        print(f"⚠️ Erro ao validar cache {caminho}: {exc}")
        return False, None


def salvar_dados(caminho, url_base, params, conteudo, status="SUCESSO"):
    """
    Salva os dados com trava de segurança: 
    Não sobrescreve um cache de SUCESSO anterior se a tentativa atual falhou.
    """
    # 1. Trava de Segurança: Se a API falhou agora, mas já temos um arquivo bom de antes
    if status != "SUCESSO" and os.path.exists(caminho):
        # Verificamos se o arquivo existente era um SUCESSO
        try:
            with open(caminho, 'r', encoding='utf-8') as f:
                cache_antigo = json.load(f)
                if cache_antigo.get("metadata", {}).get("status") == "SUCESSO":
                    # REGRA DE OURO: Mantém o arquivo antigo para não perder os dados
                    return
        except:
            pass  # Se o arquivo antigo estiver corrompido, permite sobrescrever

    # 2. Lógica de salvamento normal
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


def processar_uma_tarefa(t):
    arquivo = f"{t['pasta']}/{t['sufixo']}_{t['id_limpo']}_p{t['pagina']}.json"

    # Identifica se é saldo para aplicar a validade de 3 dias
    is_saldo = (t['label'] == "SALDO   ")
    existe_arquivo = os.path.exists(arquivo)

    sucesso, dados_cache = verificar_sucesso(
        arquivo, forcar_atualizacao=is_saldo)

    if sucesso:
        pag_rest = dados_cache.get("respostas", {}).get("paginasRestantes", 0)
        return f"⏭️ SKIP | {t['label']} | ID: {t['id_referencia']}", pag_rest

    # Se chegou aqui, ou não existe, ou vai atualizar
    tipo_log = "🔄 UPDATE" if existe_arquivo else "🆕 NEW"

    url_full = f"{BASE_URL}{t['path']}"
    params = t['params'].copy()
    params.update({"pagina": t['pagina'], "tamanhoPagina": 500})
    headers = {'User-Agent': 'Mozilla/5.0'}

    # TENTATIVAS COM BACKOFF (Lógica para erro 429)
    for tentativa in range(3):
        try:
            if 'numeroAta' in params:
                encoded_ata = quote(params['numeroAta'], safe='')
                url_with_ata = f"{url_full}?numeroAta={encoded_ata}&" + \
                               urlencode(
                                   {k: v for k, v in params.items() if k != 'numeroAta'})
                response = requests.get(
                    url_with_ata, headers=headers, timeout=30)
            else:
                response = requests.get(
                    url_full, params=params, headers=headers, timeout=30)

            if response.status_code == 200:
                dados = response.json()
                salvar_dados(arquivo, url_full, params, dados, "SUCESSO")
                # RETORNA O TIPO_LOG PARA O CONSOLE
                return f"{tipo_log} | {t['label']} | ID: {t['id_referencia']}", dados.get('paginasRestantes', 0)

            elif response.status_code == 429:
                espera = (tentativa + 1) * 10  # Espera 10, 20... segundos
                time.sleep(espera)
                continue  # Tenta novamente

            else:
                salvar_dados(arquivo, url_full, params, None,
                             f"FALHA: {response.status_code}")
                return f"❌ ERRO {response.status_code} | {t['id_referencia']}", 0

        except Exception as e:
            if tentativa < 2:
                time.sleep(5)
                continue
            salvar_dados(arquivo, url_full, params, None, f"FALHA: {str(e)}")
            return f"💥 FALHA | {t['id_referencia']} | {str(e)}", 0

    return f"❌ LIMITE TENTATIVAS | {t['id_referencia']}", 0


def montar_fila():
    tarefas = []
    mapa_busca_itens = set()

    if os.path.exists(PASTAS["C_ATAS"]):
        for arq in os.listdir(PASTAS["C_ATAS"]):
            with open(os.path.join(PASTAS["C_ATAS"], arq), 'r', encoding='utf-8') as f:
                for ata in json.load(f).get("respostas", {}).get("resultado", []):
                    ctrl_pncp = ata.get("numeroControlePncpAta")
                    num_ata = ata.get("numeroAtaRegistroPreco")
                    uasg = ata.get("codigoUnidadeGerenciadora")
                    data_ini = ata.get("dataVigenciaInicial")

                    if not ctrl_pncp or not data_ini:
                        continue
                    id_limpo = ctrl_pncp.replace("/", "_").replace("-", "_")

                    chave_busca = f"{uasg}_{data_ini}"
                    if chave_busca not in mapa_busca_itens:
                        tarefas.append({
                            "id_referencia": chave_busca, "id_limpo": chave_busca, "pagina": 1,
                            "label": "BUSCA_ITENS", "sufixo": "busca_itens", "pasta": PASTAS["I_ATAS"],
                            "path": "/modulo-arp/2_consultarARPItem",
                            "params": {"codigoUnidadeGerenciadora": uasg, "dataVigenciaInicialMin": data_ini, "dataVigenciaInicialMax": data_ini}
                        })
                        mapa_busca_itens.add(chave_busca)

                    tarefas.append({
                        "id_referencia": num_ata, "id_limpo": id_limpo, "pagina": 1,
                        "label": "SALDO   ", "sufixo": "saldo_ata", "pasta": PASTAS["S_ATAS"],
                        "path": "/modulo-arp/4_consultarEmpenhosSaldoItem",
                        "params": {"numeroAta": num_ata, "unidadeGerenciadora": uasg}
                    })

    if os.path.exists(PASTAS["I_ATAS"]):
        for arq in os.listdir(PASTAS["I_ATAS"]):
            suc, dados_item = verificar_sucesso(
                os.path.join(PASTAS["I_ATAS"], arq))
            if suc:
                for item in dados_item.get("respostas", {}).get("resultado", []):
                    n_ata, u_ger, n_item = item.get("numeroAtaRegistroPreco"), item.get(
                        "codigoUnidadeGerenciadora"), item.get("numeroItem")
                    if n_ata and u_ger and n_item:
                        id_item_limpo = f"{n_ata.replace('/', '_')}_{u_ger}_{n_item}"
                        tarefas.append({
                            "id_referencia": f"{n_ata} It {n_item}", "id_limpo": id_item_limpo, "pagina": 1,
                            "label": "UNIDADE ", "sufixo": "unidade", "pasta": PASTAS["U_ATAS"],
                            "path": "/modulo-arp/3_consultarUnidadesItem",
                            "params": {"numeroAta": n_ata, "unidadeGerenciadora": u_ger, "numeroItem": n_item}
                        })
    return tarefas


if __name__ == "__main__":
    fila = montar_fila()
    total = len(fila)
    concluidas = 0
    falhas_contador = 0
    skips_contador = 0
    ultimo_log_skip = 0

    print(
        f"🚀 INICIANDO EXTRAÇÃO BLINDADA | WORKERS: {MAX_WORKERS} | TOTAL TAREFAS: {total}\n")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(processar_uma_tarefa, t): t for t in fila}

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                t = futures.pop(future)
                concluidas += 1
                res, pag_rest = future.result()

                if pag_rest > 0:
                    t_prox = t.copy()
                    t_prox['pagina'] += 1
                    futures[executor.submit(
                        processar_uma_tarefa, t_prox)] = t_prox
                    total += 1

                if "❌" in res or "💥" in res:
                    falhas_contador += 1
                elif "⏭️ SKIP" in res:
                    skips_contador += 1

                # Exibe o progresso e o contador de erros atual
                perc = (concluidas/total)*100

                deve_logar = (
                    "❌" in res
                    or "💥" in res
                    or "🆕 NEW" in res
                    or "🔄 UPDATE" in res
                    or concluidas == total
                )

                if "⏭️ SKIP" in res and not deve_logar:
                    if (skips_contador - ultimo_log_skip) >= LOG_INTERVALO_SKIP:
                        ultimo_log_skip = skips_contador
                        print(
                            f"[{datetime.now().strftime('%H:%M:%S')}] ⏭️ SKIPs acumulados: {skips_contador} | Progresso: {concluidas}/{total} ({perc:.1f}%) | Erros: {falhas_contador}")
                else:
                    print(
                        f"[{datetime.now().strftime('%H:%M:%S')}] {res} | Progresso: {concluidas}/{total} ({perc:.1f}%) | Erros: {falhas_contador}")

    print("\n" + "="*50)
    print(f"🏁 PROCESSO FINALIZADO!")
    print(f"   - Total Processado: {concluidas}")
    print(f"   - Sucessos/Skips: {concluidas - falhas_contador}")
    print(f"   - Falhas Totais: {falhas_contador}")
    print("="*50)

    if falhas_contador > 0:
        print(f"⚠️ Ciclo finalizado com {falhas_contador} falhas pendentes.")
        sys.exit(1)
    else:
        print("🎉 TUDO CONCLUÍDO COM SUCESSO!")
        sys.exit(0)
