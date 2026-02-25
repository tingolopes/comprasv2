import os
import json
import pandas as pd
import re

# --- CONFIGURAÇÃO ---
PASTAS_COMPRAS = ["temp_compras_legado", "temp_compras_14133"]
PASTAS_ITENS = ["temp_itens_legado_id", "temp_itens_14133_id"]
ARQUIVO_SAIDA_CSV = "banco_compras.csv"

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


def limpar_texto(texto):
    if not texto or str(texto).lower() == "null":
        return ""
    texto = str(texto).replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    texto = re.sub(r'\s+', ' ', texto)
    return texto.strip()


def identificar_tipo_fonte(nome_arquivo):
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
    print("--- Mágica: Extraindo modalidades perdidas dos arquivos de itens ---")
    mapa_modalidades = {}
    for pasta in PASTAS_ITENS:
        if not os.path.exists(pasta):
            continue
        for arq in os.listdir(pasta):
            if arq.endswith(".json"):
                if "_pncp_" in arq:
                    continue
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
                except:
                    continue
    return mapa_modalidades


def unificar():
    mapa_resgate_itens = extrair_modalidade_dos_itens()
    print("--- Iniciando Unificação de Compras (V8 - Busca Global de Responsáveis) ---")
    banco_ids = {}

    for pasta in PASTAS_COMPRAS:
        if not os.path.exists(pasta):
            continue
        for arq in os.listdir(pasta):
            if arq.endswith(".json"):
                tipo = identificar_tipo_fonte(arq)
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
                except:
                    continue

    lista_final = []
    for id_c, fontes in banco_ids.items():
        # Define a fonte principal para valores e objeto
        master_key = "PNCP" if "PNCP" in fontes else (
            "LEG_E3" if "LEG_E3" in fontes else ("LEG_E5" if "LEG_E5" in fontes else "LEG_E1"))
        m = fontes[master_key]

        # --- BUSCA GLOBAL DE RESPONSÁVEIS (Independente da Master) ---
        # Tenta em E5, depois em E1, depois na Master
        resp_decl = fontes.get("LEG_E5", {}).get("no_responsavel_decl_disp") or \
            fontes.get("LEG_E1", {}).get("no_responsavel_decl_disp") or \
            fontes.get("LEG_E1", {}).get("nome_responsavel") or \
            m.get("no_responsavel_decl_disp") or ""

        cargo_decl = fontes.get("LEG_E5", {}).get("no_cargo_resp_decl_disp") or \
            fontes.get("LEG_E1", {}).get("no_cargo_resp_decl_disp") or \
            fontes.get("LEG_E1", {}).get("funcao_responsavel") or \
            m.get("no_cargo_resp_decl_disp") or ""

        resp_ratif = fontes.get("LEG_E5", {}).get("no_responsavel_ratificacao") or \
            fontes.get("LEG_E1", {}).get("no_responsavel_ratificacao") or \
            m.get("no_responsavel_ratificacao") or ""

        cargo_ratif = fontes.get("LEG_E5", {}).get("no_cargo_resp_ratificacao") or \
            fontes.get("LEG_E1", {}).get("no_cargo_resp_ratificacao") or \
            m.get("no_cargo_resp_ratificacao") or ""

        # --- LÓGICA DE MODALIDADE ---
        modalidade_final = m.get("modalidadeNome") or m.get(
            "nome_modalidade") or "Outras"
        if modalidade_final == "Outras":
            if id_c in mapa_resgate_itens:
                modalidade_final = mapa_resgate_itens[id_c]
            else:
                cod_num = str(m.get("co_modalidade_licitacao") or "")
                if cod_num in MAPA_CODIGOS:
                    modalidade_final = MAPA_CODIGOS[cod_num]

        # 1. Primeiro captura o código da UASG
        uasg_codigo = str(m.get("co_uasg") or m.get("uasg") or m.get(
            "unidadeOrgaoCodigoUnidade") or "").strip()

        # 2. Busca a sigla no nosso mapa (se não achar, deixa vazio ou '??')
        sigla_campus = MAPA_SIGLAS.get(uasg_codigo, "")

        registro = {
            "id_compra": id_c,
            "numero_controle_pncp": m.get("numeroControlePNCP") or "",
            "lei_14133": m.get("pertence14133", False) or (master_key == "PNCP"),
            "uasg": str(m.get("co_uasg") or m.get("uasg") or m.get("unidadeOrgaoCodigoUnidade") or "").strip(),
            "sigla_campus": sigla_campus,  # Coluna nova com a sigla do campus
            "unidade_nome": m.get("no_ausg") or m.get("unidadeOrgaoNomeUnidade") or "",
            "modalidade": modalidade_final,
            "objeto": limpar_texto(m.get("objetoCompra") or m.get("tx_objeto") or m.get("objeto") or m.get("ds_objeto_licitacao") or m.get("ds_justificativa") or ""),
            "responsavel_declaracao": resp_decl,
            "cargo_declaracao": limpar_texto(cargo_decl),
            "responsavel_ratificacao": limpar_texto(resp_ratif),
            "cargo_ratificacao": limpar_texto(cargo_ratif),
            "valor_estimado": m.get("valorTotalEstimado") or m.get("valorEstimadoTotal") or m.get("vr_estimado") or 0,
            "valor_homologado": m.get("valorTotalHomologado") or m.get("valorHomologadoTotal") or 0,
            "situacao": m.get("situacaoCompraNomePncp") or m.get("ds_situacao_pregao") or m.get("situacao_aviso") or "",
            "data_publicacao": m.get("dataPublicacaoPncp") or m.get("dt_data_edital") or m.get("data_publicacao") or m.get("dtDeclaracaoDispensa") or "",
            "amparo_legal_nome": limpar_texto(m.get("amparoLegalNome") or m.get("ds_fundamento_legal") or ""),
            "origem_master": master_key
        }
        lista_final.append(registro)

    df = pd.DataFrame(lista_final)
    # Salvamento padrão IFMS (CSV amigável ao Power BI)
    df.to_csv(ARQUIVO_SAIDA_CSV, index=False, encoding='utf-8-sig', sep=';')
    print(f"✅ Sucesso! {len(df)} compras unificadas no CSV.")


if __name__ == "__main__":
    unificar()
