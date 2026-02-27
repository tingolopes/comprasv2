import os
import json
import requests
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor  # Ativando Threads

# --- CONFIGURAÇÕES ---
PASTA_ORIGEM = "temp/temp_atas_itens_id"
PASTA_DESTINO = "temp/temp_itens_natureza_despesa"
BASE_URL = "https://dadosabertos.compras.gov.br/modulo-material/5_consultarMaterialNaturezaDespesa"

os.makedirs(PASTA_DESTINO, exist_ok=True)


def extrair_pdms_unicos():
    """Lê os itens das atas e extrai códigos PDM únicos."""
    pdms = set()
    if not os.path.exists(PASTA_ORIGEM):
        return pdms

    for arquivo in os.listdir(PASTA_ORIGEM):
        if arquivo.endswith(".json"):
            try:
                with open(os.path.join(PASTA_ORIGEM, arquivo), 'r', encoding='utf-8') as f:
                    dados = json.load(f)
                    itens = dados.get("respostas", {}).get("resultado", [])
                    if isinstance(itens, dict):
                        itens = [itens]
                    for item in itens:
                        pdm = item.get("codigoPdm")
                        if pdm and str(pdm).lower() != 'none':
                            pdms.add(int(pdm))
            except:
                continue
    return sorted(list(pdms))


def consultar_natureza_pdm(pdm):
    """Consulta a API com Backoff Exponencial e salvamento padrão."""
    arquivo_destino = os.path.join(PASTA_DESTINO, f"natureza_pdm_{pdm}.json")

    # Cache de Nível Superior
    if os.path.exists(arquivo_destino):
        return True

    params = {"pagina": 1, "codigoPdm": pdm}
    atraso = 4  # Segundos iniciais de espera em caso de falha
    tstamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for tentativa in range(1, 4):
        try:
            # Timeout estendido para 30s como padronizamos
            response = requests.get(BASE_URL, params=params, timeout=30)
            if response.status_code == 200:
                conteudo = response.json()
                envelope = {
                    "metadata": {
                        "url_consultada": f"{BASE_URL}?pagina=1&codigoPdm={pdm}",
                        "data_extracao": tstamp,
                        "status": "SUCESSO"
                    },
                    "respostas": {"resultado": conteudo.get("resultado", [])}
                }
                with open(arquivo_destino, 'w', encoding='utf-8') as f:
                    json.dump(envelope, f, ensure_ascii=False, indent=4)
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] ✅ PDM {pdm} | Sucesso")
                return True
        except:
            pass

        time.sleep(atraso)
        atraso *= 3  # Backoff: 4s -> 12s -> 36s

    print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ PDM {pdm} | Falhou após retries")
    return False


def executar_com_threads():
    pdms = extrair_pdms_unicos()
    total = len(pdms)
    print(f"🚀 Iniciando extração de {total} PDMs com Threads...\n")

    # Usamos max_workers=4 para ser rápido sem ser bloqueado pela API
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(consultar_natureza_pdm, pdms)

    print(f"\n✨ Processamento de naturezas finalizado.")


if __name__ == "__main__":
    executar_com_threads()
