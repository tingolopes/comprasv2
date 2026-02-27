import os
import json
import pandas as pd
import re
from collections import defaultdict

# ==============================================================================
# CONFIGURAÇÃO GERAL
# ==============================================================================

PASTAS_COMPRAS = ["temp/temp_compras_legado", "temp/temp_compras_14133"]
PASTAS_ITENS = ["temp/temp_itens_legado_id", "temp/temp_itens_14133_id"]

ARQUIVO_SAIDA_COMPRAS_CSV = "data/banco_compras.csv"
ARQUIVO_SAIDA_ITENS_CSV = "data/banco_compras_itens.csv"

MAPA_CODIGOS = {
    "5": "PREGÃO", "6": "DISPENSA DE LICITAÇÃO", "7": "INEXIGIBILIDADE",
    "1": "CONVITE", "2": "TOMADA DE PREÇOS", "3": "CONCORRÊNCIA"
}

UASGS = [
    {"sigla": "RT", "codigo": "158132"}, {"sigla": "AQ", "codigo": "158448"},
    {"sigla": "CG", "codigo": "158449"}, {"sigla": "CB", "codigo": "158450"},
    {"sigla": "CX", "codigo": "158451"}, {"sigla": "DR", "codigo": "155848"},
    {"sigla": "JD", "codigo": "155850"}, {"sigla": "NA", "codigo": "158452"},
    {"sigla": "NV", "codigo": "155849"}, {"sigla": "PP", "codigo": "158453"},
    {"sigla": "TL", "codigo": "158454"}
]

MAPA_SIGLAS = {u["codigo"]: u["sigla"] for u in UASGS}


# ==============================================================================
# FUNÇÕES UTILITÁRIAS COMPARTILHADAS
# ==============================================================================

def limpar_texto(texto):
    """Remove quebras de linha, tabulações e excesso de espaços."""
    if not texto or str(texto).lower() == "null":
        return ""
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()


# ==============================================================================
# MÓDULO 1 — BANCO DE COMPRAS (banco_compras.csv)
# ==============================================================================

def identificar_tipo_fonte_compra(nome_arquivo):
    if nome_arquivo.startswith("pncp"):
        return "PNCP"
    if "pregao" in nome_arquivo:
        return "LEG_E3"
    if "dispensa" in nome_arquivo or "inexigibilidade" in nome_arquivo:
        return "LEG_E5"
    if "outrasmodalidades" in nome_arquivo:
        return "LEG_E1"
    return "OUTRO"


def extrair_modalidade_dos_itens():
    """Extrai modalidades perdidas a partir dos arquivos de itens."""
    print("  [Compras] Extraindo modalidades perdidas dos arquivos de itens...")
    mapa_modalidades = {}
    for pasta in PASTAS_ITENS:
        if not os.path.exists(pasta):
            continue
        for arq in os.listdir(pasta):
            if arq.endswith(".json") and "_pncp_" not in arq:
                try:
                    with open(os.path.join(pasta, arq), 'r', encoding='utf-8') as f:
                        dados = json.load(f)
                        itens = dados.get("respostas", {}).get("resultado", [])
                        if isinstance(itens, dict):
                            itens = [itens]
                        for i in itens:
                            id_c = i.get("idCompra")
                            mod = i.get("nomeModalidade") or i.get(
                                "noModalidadeLicitacao")
                            if id_c and mod:
                                mapa_modalidades[id_c] = mod.upper()
                                break
                except Exception:
                    continue
    return mapa_modalidades


def gerar_banco_compras():
    """Gera o arquivo banco_compras.csv unificando todas as fontes de compras."""
    print("\n" + "=" * 60)
    print("MÓDULO 1 — Gerando banco_compras.csv")
    print("=" * 60)

    mapa_resgate_itens = extrair_modalidade_dos_itens()
    banco_ids = {}

    for pasta in PASTAS_COMPRAS:
        if not os.path.exists(pasta):
            print(f"  [AVISO] Pasta não encontrada: {pasta}")
            continue
        for arq in os.listdir(pasta):
            if arq.endswith(".json"):
                tipo = identificar_tipo_fonte_compra(arq)
                try:
                    with open(os.path.join(pasta, arq), 'r', encoding='utf-8') as f:
                        dados = json.load(f)
                        compras = dados.get(
                            "respostas", {}).get("resultado", [])
                        for c in compras:
                            id_c = c.get("idCompra") or c.get("id_compra")
                            if not id_c:
                                continue
                            if id_c not in banco_ids:
                                banco_ids[id_c] = {}
                            banco_ids[id_c][tipo] = c
                except Exception:
                    continue

    lista_final = []
    for id_c, fontes in banco_ids.items():
        master_key = next(
            (k for k in ["PNCP", "LEG_E3", "LEG_E5",
             "LEG_E1"] if k in fontes), "OUTRO"
        )
        m = fontes[master_key]

        # Busca global de responsáveis
        resp_decl = (fontes.get("LEG_E5", {}).get("no_responsavel_decl_disp") or
                     fontes.get("LEG_E1", {}).get("no_responsavel_decl_disp") or
                     fontes.get("LEG_E1", {}).get("nome_responsavel") or
                     m.get("no_responsavel_decl_disp") or "")

        cargo_decl = (fontes.get("LEG_E5", {}).get("no_cargo_resp_decl_disp") or
                      fontes.get("LEG_E1", {}).get("no_cargo_resp_decl_disp") or
                      fontes.get("LEG_E1", {}).get("funcao_responsavel") or
                      m.get("no_cargo_resp_decl_disp") or "")

        resp_ratif = (fontes.get("LEG_E5", {}).get("no_responsavel_ratificacao") or
                      fontes.get("LEG_E1", {}).get("no_responsavel_ratificacao") or
                      m.get("no_responsavel_ratificacao") or "")

        cargo_ratif = (fontes.get("LEG_E5", {}).get("no_cargo_resp_ratificacao") or
                       fontes.get("LEG_E1", {}).get("no_cargo_resp_ratificacao") or
                       m.get("no_cargo_resp_ratificacao") or "")

        # Lógica de modalidade
        modalidade_final = m.get("modalidadeNome") or m.get(
            "nome_modalidade") or "Outras"
        if modalidade_final == "Outras":
            if id_c in mapa_resgate_itens:
                modalidade_final = mapa_resgate_itens[id_c]
            else:
                cod_num = str(m.get("co_modalidade_licitacao") or "")
                if cod_num in MAPA_CODIGOS:
                    modalidade_final = MAPA_CODIGOS[cod_num]

        uasg_codigo = str(
            m.get("co_uasg") or m.get("uasg") or m.get(
                "unidadeOrgaoCodigoUnidade") or ""
        ).strip()

        registro = {
            "id_compra": id_c,
            "numero_controle_pncp": m.get("numeroControlePNCP") or "",
            "lei_14133": m.get("pertence14133", False) or (master_key == "PNCP"),
            "uasg": uasg_codigo,
            "sigla_campus": MAPA_SIGLAS.get(uasg_codigo, ""),
            "unidade_nome": m.get("no_ausg") or m.get("unidadeOrgaoNomeUnidade") or "",
            "modalidade": modalidade_final,
            "objeto": limpar_texto(
                m.get("objetoCompra") or m.get("tx_objeto") or m.get("objeto") or
                m.get("ds_objeto_licitacao") or m.get("ds_justificativa") or ""
            ),
            "responsavel_declaracao": resp_decl,
            "cargo_declaracao": limpar_texto(cargo_decl),
            "responsavel_ratificacao": limpar_texto(resp_ratif),
            "cargo_ratificacao": limpar_texto(cargo_ratif),
            "valor_estimado": m.get("valorTotalEstimado") or m.get("valorEstimadoTotal") or m.get("vr_estimado") or 0,
            "valor_homologado": m.get("valorTotalHomologado") or m.get("valorHomologadoTotal") or 0,
            "situacao": m.get("situacaoCompraNomePncp") or m.get("ds_situacao_pregao") or m.get("situacao_aviso") or "",
            "data_publicacao": (m.get("dataPublicacaoPncp") or m.get("dt_data_edital") or
                                m.get("data_publicacao") or m.get("dtDeclaracaoDispensa") or ""),
            "amparo_legal_nome": limpar_texto(m.get("amparoLegalNome") or m.get("ds_fundamento_legal") or ""),
            "origem_master": master_key
        }
        lista_final.append(registro)

    df = pd.DataFrame(lista_final)
    df.to_csv(ARQUIVO_SAIDA_COMPRAS_CSV, index=False,
              encoding='utf-8-sig', sep=';')
    print(f"  ✅ {len(df)} compras salvas em '{ARQUIVO_SAIDA_COMPRAS_CSV}'.")


# ==============================================================================
# MÓDULO 2 — BANCO DE ITENS (banco_compras_itens.csv)
# ==============================================================================

def identificar_via_item(arq):
    """Identifica a via pelo sufixo no nome do arquivo de itens."""
    if "_pncp_" in arq:
        return "PNCP"
    if "_E2_" in arq:
        return "LEG_E2"
    if "_E4_" in arq:
        return "LEG_E4"
    if "_E6_" in arq:
        return "LEG_E6"
    return "OUTRO"


def gerar_banco_itens():
    """Gera o arquivo banco_compras_itens.csv com fusão híbrida de itens."""
    print("\n" + "=" * 60)
    print("MÓDULO 2 — Gerando banco_compras_itens.csv")
    print("=" * 60)

    mapa_fusao = defaultdict(dict)

    for pasta in PASTAS_ITENS:
        if not os.path.exists(pasta):
            print(f"  [AVISO] Pasta não encontrada: {pasta}")
            continue
        for arq in os.listdir(pasta):
            if arq.endswith(".json"):
                via = identificar_via_item(arq)
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
                except Exception:
                    continue

    lista_final = []
    print(f"  Processando fusão de {len(mapa_fusao)} itens únicos...")

    for id_item, fontes in mapa_fusao.items():
        base_key = next(
            (k for k in ["PNCP", "LEG_E4", "LEG_E6",
             "LEG_E2"] if k in fontes), "OUTRO"
        )
        b = fontes[base_key]

        pncp = fontes.get("PNCP", {})
        e2 = fontes.get("LEG_E2", {})
        e4 = fontes.get("LEG_E4", {})
        e6 = fontes.get("LEG_E6", {})

        desc_resumida = (e2.get("nomeMaterial") or e2.get("nomeServico") or
                         pncp.get("descricaoResumida") or
                         e4.get("descricaoItem") or
                         e6.get("noServico") or e6.get("noMaterial") or "")

        qtd = (e6.get("qtMaterialAlt") or e4.get("quantidadeItem") or
               pncp.get("quantidade") or e2.get("quantidade") or 0)

        num_item = (pncp.get("numeroItemPncp") or
                    e2.get("numeroItemLicitacao") or
                    (e4.get("tbVwItensPregaoId") or {}).get("coItem") or
                    e6.get("nuItemMaterial") or "")

        situacao = (pncp.get("situacaoCompraItemNome") or
                    e4.get("situacaoItem") or
                    ("Homologado" if (b.get("nomeFornecedor") or b.get("noFornecedorVencedor") or
                                      b.get("fornecedorVencedor") or b.get("nomeVencedorPf"))
                     else "Pendente/Outro"))

        material_servico = (
            pncp.get("materialOuServicoNome") or
            e6.get("inMaterialServico") or
            ("material" if e2.get("codigoItemMaterial") is not None else "") or
            ("servico" if e2.get("codigoItemServico") is not None else "") or ""
        ).replace("Material", "material").replace("Serviço", "servico")

        reg = {
            "id_compra": b.get("idCompra"),
            "id_item": id_item,
            "num_item": num_item,
            "situacao": situacao,
            "material_servico": material_servico,
            "descricao": limpar_texto(desc_resumida),
            "quantidade": qtd,
            "unidade": (pncp.get("unidadeMedida") or e6.get("noUnidadeMedida") or
                        e4.get("unidadeFornecimento") or e2.get("unidade") or ""),
            "marca": limpar_texto(e6.get("noMarcaMaterial") or e4.get("noMarcaMaterial") or ""),
            "valor_estimado": (pncp.get("valorUnitarioEstimado") or e4.get("valorEstimadoItem") or
                               e6.get("vrEstimadoItem") or e2.get("valorEstimado") or 0),
            "valor_homologado": pncp.get("valorUnitarioResultado") or e4.get("valorHomologadoItem") or 0,
            "fornecedor_cnpj": (pncp.get("codFornecedor") or e6.get("nuCnpjVencedor") or
                                e2.get("cnpjFornecedor") or ""),
            "fornecedor_nome": limpar_texto(
                pncp.get("nomeFornecedor") or e6.get("noFornecedorVencedor") or
                e4.get("fornecedorVencedor") or e2.get("nomeVencedorPf") or
                e2.get("nomeFornecedor") or ""
            ),
            "origem_fusao": ",".join(fontes.keys())
        }
        lista_final.append(reg)

    df = pd.DataFrame(lista_final)
    for col in ["quantidade", "valor_estimado", "valor_homologado"]:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'uasg' in df.columns:
        df['uasg'] = df['uasg'].astype(str)
    if 'co_uasg' in df.columns:
        df['co_uasg'] = df['co_uasg'].astype(str)

    df.to_csv(ARQUIVO_SAIDA_ITENS_CSV, index=False,
              encoding='utf-8-sig', sep=';')
    print(f"  ✅ {len(df)} itens salvos em '{ARQUIVO_SAIDA_ITENS_CSV}'.")


# ==============================================================================
# PONTO DE ENTRADA
# ==============================================================================

if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════╗")
    print("║         GERADOR DE BANCOS DE COMPRAS — IFMS              ║")
    print("╚══════════════════════════════════════════════════════════╝")

    gerar_banco_compras()
    gerar_banco_itens()

    print("\n🏁 Processamento concluído! Arquivos gerados:")
    print(f"   → {ARQUIVO_SAIDA_COMPRAS_CSV}")
    print(f"   → {ARQUIVO_SAIDA_ITENS_CSV}")
