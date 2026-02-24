import os
import json
import pandas as pd
import re
from urllib.parse import unquote

# Configuração de Pastas
PASTAS = {
    "ATAS": "temp_atas_arp",
    "ITENS": "temp_itens_atas_id",
    "SALDOS": "temp_saldos_atas_id",
    "UNIDADES": "temp_unidades_atas_id"
}


def limpar(t):
    if not t or str(t).lower() == "null":
        return ""
    return str(t).replace('\n', ' ').replace('\r', ' ').replace(';', ',').strip()


def extrair_da_url(url):
    try:
        match = re.search(r"numeroAta=([^&]+)", url)
        if match:
            return unquote(match.group(1))
    except:
        pass
    return None


def build():
    print("🚀 Iniciando Consolidação Robusta (Unificação Total)...")

    erros_detectados = 0
    total_arquivos = 0

    # --- 0. NOVO: BASE DE ATAS (CABEÇALHO) ---
    atas_header = []
    if os.path.exists(PASTAS["ATAS"]):
        arquivos_atas = [f for f in os.listdir(
            PASTAS["ATAS"]) if f.endswith(".json")]
        total_arquivos += len(arquivos_atas)
        for arq in arquivos_atas:
            with open(os.path.join(PASTAS["ATAS"], arq), 'r', encoding='utf-8') as f:
                envelope = json.load(f)
                if envelope.get("metadata", {}).get("status") != "SUCESSO":
                    erros_detectados += 1

                res = envelope.get("respostas", {})
                dados = res.get("resultado", []) if isinstance(
                    res, dict) else []
                for a in dados:
                    atas_header.append({
                        "id_compra": a.get("idCompra"),
                        "numero_ata": a.get("numeroAtaRegistroPreco"),
                        "codigo_uasg": a.get("codigoUnidadeGerenciadora"),
                        "nome_uasg": a.get("nomeUnidadeGerenciadora"),
                        "data_assinatura": a.get("dataAssinatura"),
                        "data_vigencia_inicio": a.get("dataVigenciaInicial"),
                        "data_vigencia_fim": a.get("dataVigenciaFinal"),
                        "valor_total_ata": a.get("valorTotal"),
                        "objeto": limpar(a.get("objeto")),
                        "modalidade": a.get("nomeModalidadeCompra"),
                        "link_pncp": a.get("linkAtaPNCP"),
                        "numero_controle_pncp": a.get("numeroControlePncpAta")
                    })

    if atas_header:
        pd.DataFrame(atas_header).drop_duplicates().to_csv(
            "banco_atas.csv", index=False, sep=';', encoding='utf-8-sig')

    # 1. BASE DE ITENS
    itens = []
    if os.path.exists(PASTAS["ITENS"]):
        arquivos = [f for f in os.listdir(
            PASTAS["ITENS"]) if f.endswith(".json")]
        total_arquivos += len(arquivos)
        for arq in arquivos:
            with open(os.path.join(PASTAS["ITENS"], arq), 'r', encoding='utf-8') as f:
                envelope = json.load(f)
                if envelope.get("metadata", {}).get("status") != "SUCESSO":
                    erros_detectados += 1

                res = envelope.get("respostas", {})
                dados = res.get("resultado", []) if isinstance(
                    res, dict) else []
                for i in dados:
                    itens.append({
                        "id_licitacao_pncp": i.get("numeroControlePncpCompra"),
                        "id_ata_pncp": i.get("numeroControlePncpAta"),
                        "numero_ata": i.get("numeroAtaRegistroPreco"),
                        "numero_item": i.get("numeroItem"),
                        "material_cod": i.get("codigoItem"),
                        "material_nome": limpar(i.get("descricaoItem")),
                        "fornecedor_cnpj": i.get("niFornecedor"),
                        "fornecedor_nome": limpar(i.get("nomeRazaoSocialFornecedor")),
                        "valor_unitario": i.get("valorUnitario"),
                        "qtd_homologada": i.get("quantidadeHomologadaItem"),
                        "valor_total_item": i.get("valorTotal")
                    })
    pd.DataFrame(itens).drop_duplicates().to_csv(
        "banco_atas_itens.csv", index=False, sep=';', encoding='utf-8-sig')

    # 2. BASE DE UNIDADES (Participação + Saldos)
    reservas = []
    if os.path.exists(PASTAS["UNIDADES"]):
        arquivos = [f for f in os.listdir(
            PASTAS["UNIDADES"]) if f.endswith(".json")]
        total_arquivos += len(arquivos)
        for arq in arquivos:
            with open(os.path.join(PASTAS["UNIDADES"], arq), 'r', encoding='utf-8') as f:
                envelope = json.load(f)
                if envelope.get("metadata", {}).get("status") != "SUCESSO":
                    erros_detectados += 1

                res = envelope.get("respostas", {})
                dados = res.get("resultado", []) if isinstance(
                    res, dict) else []
                for u in dados:
                    reservas.append({
                        "numero_ata": u.get("numeroAta"),
                        "numero_item": u.get("numeroItem"),
                        "codigo_unidade": str(u.get("codigoUnidade")).strip(),
                        "nome_unidade": u.get("nomeUnidade"),
                        "tipo_participacao": u.get("tipoUnidade"),
                        "qtd_reservada": u.get("quantidadeRegistrada"),
                        "saldo_remanejamento": u.get("saldoRemanejamentoEmpenho")
                    })
    df_res = pd.DataFrame(reservas).drop_duplicates()

    empenhos = []
    if os.path.exists(PASTAS["SALDOS"]):
        arquivos = [f for f in os.listdir(
            PASTAS["SALDOS"]) if f.endswith(".json")]
        total_arquivos += len(arquivos)
        for arq in arquivos:
            with open(os.path.join(PASTAS["SALDOS"], arq), 'r', encoding='utf-8') as f:
                envelope = json.load(f)
                if envelope.get("metadata", {}).get("status") != "SUCESSO":
                    erros_detectados += 1

                data = envelope
                num_ata_url = extrair_da_url(
                    data.get("metadata", {}).get("url_consultada", ""))
                res = data.get("respostas", {})
                dados = res.get("resultado", []) if isinstance(
                    res, dict) else []
                for s in dados:
                    uni_bruta = s.get("unidade", "")
                    partes = uni_bruta.split(" - ", 1)
                    cod_uni = partes[0].strip() if len(
                        partes) > 0 else uni_bruta
                    nome_uni = partes[1].strip() if len(partes) > 1 else ""

                    empenhos.append({
                        "numero_ata": num_ata_url,
                        "numero_item": s.get("numeroItem"),
                        "codigo_unidade": str(cod_uni).strip(),
                        "nome_unidade_alt": nome_uni,
                        "tipo_alt": s.get("tipo"),
                        "qtd_reservada_alt": s.get("quantidadeRegistrada"),
                        "qtd_empenhada": s.get("quantidadeEmpenhada"),
                        "saldo_empenho": s.get("saldoEmpenho"),
                        "data_atualizacao_saldo": s.get("dataHoraAtualizacao")
                    })
    df_emp = pd.DataFrame(empenhos).drop_duplicates()

    # --- FUSÃO INTELIGENTE ---
    if not df_res.empty or not df_emp.empty:
        df_merge = pd.merge(df_res, df_emp, on=[
                            "numero_ata", "numero_item", "codigo_unidade"], how="outer")

        # Preenchimento automático (Coalesce)
        df_merge["nome_unidade"] = df_merge["nome_unidade"].fillna(
            df_merge["nome_unidade_alt"])
        df_merge["tipo_participacao"] = df_merge["tipo_participacao"].fillna(
            df_merge["tipo_alt"])
        df_merge["qtd_reservada"] = df_merge["qtd_reservada"].fillna(
            df_merge["qtd_reservada_alt"])

        cols_drop = ["nome_unidade_alt", "tipo_alt", "qtd_reservada_alt"]
        df_merge = df_merge.drop(
            columns=[c for c in cols_drop if c in df_merge.columns])

        df_merge.to_csv("banco_atas_unidades_consolidado.csv",
                        index=False, sep=';', encoding='utf-8-sig')

    print("-" * 50)
    print(f"📊 RESUMO DA CONSOLIDAÇÃO:")
    print(f"   - Total de arquivos processados: {total_arquivos}")
    print(f"   - Erros/Falhas de API encontrados: {erros_detectados}")
    print(f"   - Atas registradas: {len(atas_header)}")
    print(
        f"   - Linhas no consolidado de unidades: {len(df_merge) if 'df_merge' in locals() else 0}")
    print(f"✅ Processo concluído.")
    print("-" * 50)


if __name__ == "__main__":
    build()
