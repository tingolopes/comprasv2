import os
import pandas as pd
from datetime import datetime

# --- CONFIGURAÇÃO ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASTA_DATA = os.path.join(BASE_DIR, "data")
ARQUIVO_META = os.path.join(PASTA_DATA, "banco_metadados_atualizacao.csv")


def gerar_auditoria_arquivos():
    print("🔍 Iniciando auditoria de modificação dos arquivos CSV...")

    if not os.path.exists(PASTA_DATA):
        print(f"❌ Erro: Pasta {PASTA_DATA} não encontrada.")
        return

    registros = []

    # Lista todos os arquivos .csv na pasta data
    arquivos_csv = [f for f in os.listdir(PASTA_DATA) if f.endswith(
        ".json") == False and f.endswith(".csv")]

    # Remove o próprio arquivo de metadados da lista para não dar loop
    if "banco_metadados_atualizacao.csv" in arquivos_csv:
        arquivos_csv.remove("banco_metadados_atualizacao.csv")

    for arquivo in arquivos_csv:
        caminho_completo = os.path.join(PASTA_DATA, arquivo)

        # Captura a data de modificação do sistema de arquivos (mtime)
        mtime = os.path.getmtime(caminho_completo)
        data_modificacao = datetime.fromtimestamp(
            mtime).strftime("%d/%m/%Y %H:%M:%S")

        registros.append({
            "tabela": arquivo.replace(".csv", ""),
            "ultima_atualizacao": data_modificacao,
            "tamanho_kb": round(os.path.getsize(caminho_completo) / 1024, 2)
        })

    if registros:
        df_meta = pd.DataFrame(registros)
        # Ordena por nome da tabela para ficar organizado no Power BI
        df_meta = df_meta.sort_values(by="tabela")

        df_meta.to_csv(ARQUIVO_META, index=False,
                       sep=';', encoding='utf-8-sig')
        print(
            f"✅ Auditoria finalizada! {len(registros)} arquivos mapeados em banco_metadados_atualizacao.csv")
    else:
        print("⚠️ Nenhum arquivo CSV encontrado para auditoria.")


if __name__ == "__main__":
    gerar_auditoria_arquivos()
