import os
import json
from collections import Counter

# --- CONFIGURAÇÃO ---
PASTAS_MONITORADAS = ["temp/temp_atas_arp",
                      "temp/temp_atas_itens_id",
                      "temp/temp_atas_saldos_id",
                      "temp/temp_atas_unidades_id",
                      "temp/temp_compras_14133",
                      "temp/temp_compras_legado",
                      "temp/temp_itens_14133_id",
                      "temp/temp_itens_legado_id"]


def realizar_checkup():
    total_sucesso = 0
    total_falha = 0
    detalhes_falhas = []

    print(
        f"\n=== RELATÓRIO DE STATUS DA EXTRAÇÃO ({len(PASTAS_MONITORADAS)} PASTAS) ===\n")

    for pasta in PASTAS_MONITORADAS:
        if not os.path.exists(pasta):
            print(f"[!] Pasta não encontrada: {pasta}")
            continue

        arquivos = [f for f in os.listdir(pasta) if f.endswith('.json')]

        for nome_arq in arquivos:
            caminho = os.path.join(pasta, nome_arq)
            try:
                with open(caminho, 'r', encoding='utf-8') as f:
                    dados = json.load(f)
                    metadata = dados.get("metadata", {})
                    status = metadata.get("status", "DESCONHECIDO")
                    endpoint = metadata.get(
                        "url_consultada", "URL não registrada")

                    if status == "SUCESSO":
                        total_sucesso += 1
                    else:
                        total_falha += 1
                        detalhes_falhas.append({
                            "arquivo": nome_arq,
                            # Pega só a base da URL
                            "endpoint": endpoint.split('?')[0]
                        })
            except Exception as e:
                print(f"[!] Erro ao ler {nome_arq}: {e}")

    if detalhes_falhas:
        print("\n--- DETALHAMENTO DAS FALHAS ---")
        # Agrupa falhas por endpoint para facilitar a análise
        for falha in detalhes_falhas:
            print(f"Arquivo: {falha['arquivo']}")
            print(f"URL:     {falha['endpoint']}")
            print("-" * 30)

    # --- EXIBIÇÃO DOS RESULTADOS ---
    print(f"📊 TOTAL:    {total_sucesso + total_falha}")
    print(f"✅ SUCESSOS: {total_sucesso}")
    print(f"❌ FALHAS:   {total_falha}")


if __name__ == "__main__":
    realizar_checkup()
