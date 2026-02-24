import os
import json
import pandas as pd
import re
from collections import defaultdict

# --- CONFIGURAÇÃO ---
PASTAS_ITENS = ["temp_itens_legado_id", "temp_itens_14133_id"]
ARQUIVO_SAIDA_ITENS = "banco_compras_itens.parquet"


def limpar_texto(texto):
    """Remove quebras de linha, tabulações e excesso de espaços."""
    if not texto or str(texto).lower() == "null":
        return ""
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()


def identificar_via(arq):
    """Identifica a via pelo sufixo no nome do arquivo."""
    if "_pncp_" in arq:
        return "PNCP"
    if "_E2_" in arq:
        return "LEG_E2"
    if "_E4_" in arq:
        return "LEG_E4"
    if "_E6_" in arq:
        return "LEG_E6"
    return "OUTRO"


def consolidar_itens():
    print("--- Iniciando Fusão Híbrida de Itens (Merge E2 + E4/E6 + PNCP) ---")

    mapa_fusao = defaultdict(dict)

    # 1. AGRUPAMENTO
    for pasta in PASTAS_ITENS:
        if not os.path.exists(pasta):
            continue
        for arq in os.listdir(pasta):
            if arq.endswith(".json"):
                via = identificar_via(arq)
                try:
                    with open(os.path.join(pasta, arq), 'r', encoding='utf-8') as f:
                        dados_json = json.load(f)
                        itens = dados_json.get(
                            "respostas", {}).get("resultado", [])
                        if isinstance(itens, dict):
                            itens = [itens]

                        for i in itens:
                            id_item = i.get("idCompraItem")
                            if id_item:
                                mapa_fusao[id_item][via] = i
                except:
                    continue

    # 2. FUSÃO (MERGE)
    lista_final = []
    print(f"Processando fusão de {len(mapa_fusao)} itens únicos...")

    for id_item, fontes in mapa_fusao.items():
        # Define a base de dados prioritária (b)
        base_key = next(
            (k for k in ["PNCP", "LEG_E4", "LEG_E6", "LEG_E2"] if k in fontes), "OUTRO")
        b = fontes[base_key]

        pncp = fontes.get("PNCP", {})
        e2 = fontes.get("LEG_E2", {})
        e4 = fontes.get("LEG_E4", {})
        e6 = fontes.get("LEG_E6", {})

        # DESCRIÇÃO: Prioridade E2
        desc_resumida = e2.get("nomeMaterial") or e2.get("nomeServico") or \
            pncp.get("descricaoResumida") or \
            e4.get("descricaoItem") or \
            e6.get("noServico") or e6.get("noMaterial") or ""

        # QUANTIDADE: Prioridade E6/E4
        qtd = e6.get("qtMaterialAlt") or e4.get("quantidadeItem") or \
            pncp.get("quantidade") or e2.get("quantidade") or 0

        # NÚMERO ITEM
        num_item = pncp.get("numeroItemPncp") or \
            e2.get("numeroItemLicitacao") or \
            e4.get("tbVwItensPregaoId", {}).get("coItem") or \
            e6.get("nuItemMaterial") or ""

        # SITUAÇÃO (HOMOLOGAÇÃO) - Corrigido 'base' para 'b'
        situacao = pncp.get("situacaoCompraItemNome") or \
            e4.get("situacaoItem") or \
            ("Homologado" if b.get("nomeFornecedor") or b.get("noFornecedorVencedor") or b.get(
                "fornecedorVencedor") or b.get("nomeVencedorPf") else "Pendente/Outro")

        reg = {
            "id_compra": b.get("idCompra"),
            "id_item": id_item,
            "num_item": num_item,
            "situacao": situacao,
            "descricao": limpar_texto(desc_resumida),
            "quantidade": qtd,
            "unidade": pncp.get("unidadeMedida") or e6.get("noUnidadeMedida") or e4.get("unidadeFornecimento") or e2.get("unidade") or "",
            "marca": limpar_texto(e6.get("noMarcaMaterial") or e4.get("noMarcaMaterial") or ""),
            "valor_estimado": pncp.get("valorUnitarioEstimado") or e4.get("valorEstimadoItem") or e6.get("vrEstimadoItem") or e2.get("valorEstimado") or 0,
            "valor_homologado": pncp.get("valorUnitarioResultado") or e4.get("valorHomologadoItem") or 0,
            "fornecedor_nome": limpar_texto(pncp.get("nomeFornecedor") or e6.get("noFornecedorVencedor") or e4.get("fornecedorVencedor") or e2.get("nomeVencedorPf") or e2.get("nomeFornecedor") or ""),
            "fornecedor_cnpj": pncp.get("codFornecedor") or e6.get("nuCnpjVencedor") or e2.get("cnpjFornecedor") or "",
            "origem_fusao": ",".join(fontes.keys())
        }
        lista_final.append(reg)

    # 3. EXPORTAÇÃO
    df = pd.DataFrame(lista_final)
    for col in ["quantidade", "valor_estimado", "valor_homologado"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df.to_parquet(ARQUIVO_SAIDA_ITENS, index=False,
                  encoding='utf-8-sig', sep=';')
    print(f"✅ Sucesso! {len(df)} itens unificados em '{ARQUIVO_SAIDA_ITENS}'.")


if __name__ == "__main__":
    consolidar_itens()
