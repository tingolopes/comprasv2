import os
import shutil
import json
import re

# Pasta de destino
DESTINO = "modelos"


def carregar_json_seguro(caminho):
    """Proteção contra arquivos corrompidos ou nulos."""
    try:
        with open(caminho, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as exc:
        print(f"⚠️ Erro ao carregar {caminho}: {exc}")
        return None


def possui_dados(dados):
    """Verifica se o JSON tem conteúdo útil (evita arquivos vazios)."""
    if not dados:
        return False
    # Verifica caminhos comuns de dados nas suas APIs (respostas/resultado ou direto no resultado)
    res = dados.get("respostas", {})
    if isinstance(res, dict):
        return len(res.get("resultado", [])) > 0
    elif isinstance(res, list):
        return len(res) > 0

    # Caso o resultado esteja na raiz (comum em alguns endpoints)
    if "resultado" in dados and isinstance(dados["resultado"], list):
        return len(dados["resultado"]) > 0

    return False


def coletar():
    if not os.path.exists(DESTINO):
        os.makedirs(DESTINO)
        print(f"✅ Pasta '{DESTINO}' criada.")

    print(f"🚀 Iniciando coleta dinâmica de amostras (Padrão temp/temp_)...")

    # Localiza todas as pastas que começam com 'temp/temp_' usando Regex
    diretorio_atual = os.getcwd()
    pastas_detectadas = [d for d in os.listdir(diretorio_atual)
                         if os.path.isdir(d) and re.match(r'^temp/temp_', d)]

    if not pastas_detectadas:
        print("⚠️ Nenhuma pasta começando com 'temp/temp_' foi encontrada.")
        return

    for pasta_origem in pastas_detectadas:
        # O rótulo será o nome da pasta em maiúsculas sem o prefixo temp/temp_ para ficar limpo
        rotulo = pasta_origem.replace("temp/temp_", "").upper()

        arquivos = [f for f in os.listdir(pasta_origem) if f.endswith('.json')]
        copiados = 0

        print(f"📂 Analisando pasta: {pasta_origem} (Rótulo: {rotulo})")

        for arq in arquivos:
            if copiados >= 2:
                break  # Limite de 2 amostras por pasta

            caminho_origem = os.path.join(pasta_origem, arq)
            dados = carregar_json_seguro(caminho_origem)

            # Só copia se o arquivo tiver resultados reais
            if possui_dados(dados):
                novo_nome = f"{rotulo}_{arq}"
                shutil.copy2(caminho_origem, os.path.join(DESTINO, novo_nome))
                print(f"   -> Amostra válida: {novo_nome}")
                copiados += 1

        if copiados == 0:
            print(
                f"   ℹ️ Nenhuma amostra com dados úteis encontrada em {pasta_origem}.")

    print(
        f"\n✨ Coleta dinâmica concluída! Amostras úteis salvas em '{DESTINO}'.")


if __name__ == "__main__":
    coletar()
