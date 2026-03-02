import os
import json
import pandas as pd
import re

# --- CONFIGURAÇÃO ---
PASTA_ORIGEM = "temp/temp_itens_natureza_despesa"
ARQUIVO_SAIDA_CSV = "data/banco_naturezas_despesa.csv"


def limpar_texto_pdm(texto):
    if not texto or str(texto).lower() == "null":
        return ""
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    texto = texto.replace(';', ',')
    return re.sub(r'\s+', ' ', texto).strip()


def consolidar_naturezas():
    print(
        f"🏗️ Consolidando Naturezas com classificação de Grupo em {PASTA_ORIGEM}...")

    if not os.path.exists(PASTA_ORIGEM):
        print(f"❌ Erro: Pasta {PASTA_ORIGEM} não encontrada.")
        return

    lista_final = []
    arquivos = [f for f in os.listdir(PASTA_ORIGEM) if f.endswith(".json")]

    for nome_arq in arquivos:
        try:
            with open(os.path.join(PASTA_ORIGEM, nome_arq), 'r', encoding='utf-8') as f:
                dados = json.load(f)
                naturezas = dados.get("respostas", {}).get("resultado", [])
                if isinstance(naturezas, dict):
                    naturezas = [naturezas]

                for nat in naturezas:
                    cod_nat = str(
                        nat.get("codigoNaturezaDespesa") or "").strip()

                    # --- VERIFICAÇÃO DE GRUPO (Nível Superior) ---
                    grupo = ""
                    if cod_nat.startswith("44"):
                        grupo = "permanente"
                    elif cod_nat.startswith("33"):
                        grupo = "consumo"

                    registro = {
                        "codigo_pdm": str(nat.get("codigoPdm") or "").strip(),
                        "codigo_natureza": cod_nat,
                        "nome_natureza": limpar_texto_pdm(nat.get("nomeNaturezaDespesa") or ""),
                        "grupo": grupo,  # Atributo de classificação contábil
                        "status_natureza": "Ativo" if nat.get("statusNaturezaDespesa") is True else "Inativo"
                    }
                    lista_final.append(registro)
        except Exception as exc:
            print(f"⚠️ Erro ao processar {nome_arq}: {exc}")
            continue

    if not lista_final:
        return

    df = pd.DataFrame(lista_final)
    # Remove duplicatas exatas para manter a base limpa
    df = df.drop_duplicates(subset=['codigo_pdm', 'codigo_natureza'])

    # Exportação robusta para Power BI
    df.to_csv(ARQUIVO_SAIDA_CSV, index=False, encoding='utf-8-sig', sep=';')
    print(
        f"✅ SUCESSO! Base com {len(df)} naturezas e classificação de grupo gerada.")


if __name__ == "__main__":
    consolidar_naturezas()
