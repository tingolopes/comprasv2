import os
import json
import pandas as pd
import re

# --- CONFIGURAÇÃO ---
PASTAS_COMPRAS = ["temp_compras_legado", "temp_compras_14133"]
PASTAS_ITENS = ["temp_itens_legado_id", "temp_itens_14133_id"]
ARQUIVO_SAIDA_CSV = "banco_compras.csv"

# Mapa de códigos para resgate quando o texto falhar
MAPA_CODIGOS = {
    "5": "PREGÃO",
    "6": "DISPENSA DE LICITAÇÃO",
    "7": "INEXIGIBILIDADE",
    "1": "CONVITE",
    "2": "TOMADA DE PREÇOS",
    "3": "CONCORRÊNCIA"
}


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
    """Varre os arquivos de itens para criar um de-para de ID_COMPRA -> MODALIDADE"""
    print("--- Mágica: Extraindo modalidades perdidas dos arquivos de itens ---")
    mapa_modalidades = {}

    for pasta in PASTAS_ITENS:
        if not os.path.exists(pasta):
            continue
        for arq in os.listdir(pasta):
            if arq.endswith(".json"):
                if "_pncp_" in arq:
                    continue
                if "_E4_" in arq:
                    partes = arq.split('_')
                    if len(partes) > 1:
                        mapa_modalidades[partes[1]] = "PREGÃO"
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
    print("--- Iniciando Unificação de Compras (V7 - Resgate por Código e Itens) ---")
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
        if "PNCP" in fontes:
            master = "PNCP"
        elif "LEG_E3" in fontes:
            master = "LEG_E3"
        elif "LEG_E5" in fontes:
            master = "LEG_E5"
        else:
            master = "LEG_E1"

        m = fontes[master]
        f_legado = fontes.get("LEG_E5") or fontes.get("LEG_E1") or {}

        # --- LÓGICA DE MODALIDADE (3 níveis de tentativa) ---
        modalidade_final = m.get("modalidadeNome") or m.get(
            "nome_modalidade") or "Outras"

        if modalidade_final == "Outras":
            # 1. Tenta resgate via Itens (Mapeamento Anterior)
            if id_c in mapa_resgate_itens:
                modalidade_final = mapa_resgate_itens[id_c]
            # 2. Tenta resgate via Código Numérico (Nova Descoberta!)
            else:
                cod_num = str(m.get("co_modalidade_licitacao") or "")
                if cod_num in MAPA_CODIGOS:
                    modalidade_final = MAPA_CODIGOS[cod_num]

        registro = {
            "id_compra": id_c,
            "numero_controle_pncp": m.get("numeroControlePNCP") or "",
            "lei_14133": m.get("pertence14133", False) or (master == "PNCP"),
            "uasg": m.get("co_uasg") or m.get("uasg") or m.get("unidadeOrgaoCodigoUnidade"),
            "unidade_nome": m.get("no_ausg") or m.get("unidadeOrgaoNomeUnidade"),
            "modalidade": modalidade_final,
            "objeto": limpar_texto(m.get("objetoCompra") or m.get("tx_objeto") or m.get("objeto") or f_legado.get("ds_objeto_licitacao") or ""),
            "responsavel_declaracao": limpar_texto(f_legado.get("no_responsavel_decl_disp") or ""),
            "cargo_declaracao": limpar_texto(f_legado.get("no_cargo_resp_decl_disp") or ""),
            "responsavel_ratificacao": limpar_texto(f_legado.get("no_responsavel_ratificacao") or ""),
            "cargo_ratificacao": limpar_texto(f_legado.get("no_cargo_resp_ratificacao") or ""),
            "valor_estimado": m.get("valorTotalEstimado") or m.get("valorEstimadoTotal") or 0,
            "valor_homologado": m.get("valorTotalHomologado") or m.get("valorHomologadoTotal") or 0,
            "situacao": m.get("situacaoCompraNomePncp") or m.get("ds_situacao_pregao") or m.get("situacao_aviso"),
            "data_publicacao": m.get("dataPublicacaoPncp") or m.get("dt_data_edital") or m.get("data_publicacao"),
            "amparo_legal_nome": limpar_texto(m.get("amparoLegalNome") or f_legado.get("ds_fundamento_legal") or ""),
            "origem_master": master
        }
        lista_final.append(registro)

    df = pd.DataFrame(lista_final)
    df.to_csv(ARQUIVO_SAIDA_CSV, index=False, encoding='utf-8-sig', sep=';')

    # Força UASG a ser string para não dar erro no PyArrow
    if 'uasg' in df.columns:
        df['uasg'] = df['uasg'].astype(str)
    if 'co_uasg' in df.columns:
        df['co_uasg'] = df['co_uasg'].astype(str)

    print(f"✅ Sucesso! {len(df)} compras unificadas.")


if __name__ == "__main__":
    unificar()
