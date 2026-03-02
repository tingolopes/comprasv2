import os
import json
import pandas as pd
import re
from urllib.parse import unquote

# ==============================================================================
# CONFIGURAÇÃO DE CAMINHOS (DINÂMICA)
# ==============================================================================

# Pega a raiz do projeto (um nível acima da pasta generators)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASTA_TEMP = os.path.join(BASE_DIR, "temp")
PASTA_DATA = os.path.join(BASE_DIR, "data")


def localizar_pastas():
    """Busca as subpastas de dados brutos dentro de 'temp'."""
    pastas = {"ATAS": "", "ITENS": "", "SALDOS": "", "UNIDADES": ""}

    if not os.path.exists(PASTA_TEMP):
        print(f"⚠️ Erro: Pasta 'temp' não encontrada em {PASTA_TEMP}")
        return pastas

    for d in os.listdir(PASTA_TEMP):
        caminho_completo = os.path.join(PASTA_TEMP, d)
        if not os.path.isdir(caminho_completo):
            continue

        # Mapeamento por palavra-chave no nome da pasta
        if "atas_arp" in d:
            pastas["ATAS"] = caminho_completo
        elif "atas_itens_id" in d:
            pastas["ITENS"] = caminho_completo
        elif "atas_saldos_id" in d:
            pastas["SALDOS"] = caminho_completo
        elif "atas_unidades_id" in d:
            pastas["UNIDADES"] = caminho_completo

    return pastas


PASTAS = localizar_pastas()


def limpar(t):
    if not t or str(t).lower() == "null":
        return ""
    return str(t).replace('\n', ' ').replace('\r', ' ').replace(';', ',').strip()


def extrair_da_url(url):
    try:
        match = re.search(r"numeroAta=([^&]+)", url)
        if match:
            return unquote(match.group(1))
    except Exception as exc:
        print(f"⚠️ Não foi possível extrair número da ata da URL: {exc}")
    return None


def salvar_bases(df, nome_base):
    if df.empty:
        print(f"⚠️ Pulo: {nome_base} está vazia.")
        return

    # Garante que a pasta 'data' exista
    os.makedirs(PASTA_DATA, exist_ok=True)

    # Extrai o nome do arquivo para salvar no diretório data correto
    nome_arquivo = os.path.basename(nome_base)
    caminho_final = os.path.join(PASTA_DATA, f"{nome_arquivo}.csv")

    df.to_csv(caminho_final, index=False, sep=';', encoding='utf-8-sig')
    print(f"   ✅ Base Salva: {caminho_final} ({len(df)} linhas)")


def build():
    print("🚀 Iniciando Consolidação Robusta (Restaurando Colunas)...")

    # --- 1. BASE DE ATAS ---
    atas_header = []
    if PASTAS["ATAS"] and os.path.exists(PASTAS["ATAS"]):
        for arq in [f for f in os.listdir(PASTAS["ATAS"]) if f.endswith(".json")]:
            with open(os.path.join(PASTAS["ATAS"], arq), 'r', encoding='utf-8') as f:
                res = json.load(f).get("respostas", {})
                for a in res.get("resultado", []) if isinstance(res, dict) else []:
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
                        "linkAtaPNCP": a.get("linkAtaPNCP"),
                        "linkCompraPNCP": a.get("linkCompraPNCP"),
                        "numero_controle_pncp": a.get("numeroControlePncpAta")
                    })
    salvar_bases(pd.DataFrame(atas_header).drop_duplicates(),
                 "data/banco_atas")

    # --- 2. BASE DE ITENS ---
    itens = []
    if PASTAS["ITENS"] and os.path.exists(PASTAS["ITENS"]):
        for arq in [f for f in os.listdir(PASTAS["ITENS"]) if f.endswith(".json")]:
            with open(os.path.join(PASTAS["ITENS"], arq), 'r', encoding='utf-8') as f:
                res = json.load(f).get("respostas", {})
                for i in res.get("resultado", []) if isinstance(res, dict) else []:
                    itens.append({
                        "id_licitacao_pncp": i.get("numeroControlePncpCompra"),
                        "id_ata_pncp": i.get("numeroControlePncpAta"),
                        "numero_ata": i.get("numeroAtaRegistroPreco"),
                        "numero_item": i.get("numeroItem"),
                        "material_cod": i.get("codigoItem"),
                        "material_servico": i.get("tipoItem"),
                        "codigo_pdm": i.get("codigoPdm"),
                        "fornecedor_cnpj": i.get("niFornecedor"),
                        "fornecedor_nome": limpar(i.get("nomeRazaoSocialFornecedor")),
                        "valor_unitario": i.get("valorUnitario"),
                        "qtd_homologada": i.get("quantidadeHomologadaItem"),
                        "valor_total_item": i.get("valorTotal"),
                        "material_nome": limpar(i.get("descricaoItem"))
                    })
    salvar_bases(pd.DataFrame(itens).drop_duplicates(),
                 "data/banco_atas_itens")

    # --- 3. BASE DE UNIDADES (CONSOLIDAÇÃO SALDO + PARTICIPAÇÃO) ---
    reservas = []
    if PASTAS["UNIDADES"] and os.path.exists(PASTAS["UNIDADES"]):
        for arq in [f for f in os.listdir(PASTAS["UNIDADES"]) if f.endswith(".json")]:
            with open(os.path.join(PASTAS["UNIDADES"], arq), 'r', encoding='utf-8') as f:
                res = json.load(f).get("respostas", {})
                for u in res.get("resultado", []) if isinstance(res, dict) else []:
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
    if PASTAS["SALDOS"] and os.path.exists(PASTAS["SALDOS"]):
        for arq in [f for f in os.listdir(PASTAS["SALDOS"]) if f.endswith(".json")]:
            with open(os.path.join(PASTAS["SALDOS"], arq), 'r', encoding='utf-8') as f:
                env = json.load(f)
                num_ata_url = extrair_da_url(
                    env.get("metadata", {}).get("url_consultada", ""))
                res = env.get("respostas", {})
                for s in res.get("resultado", []) if isinstance(res, dict) else []:
                    partes = s.get("unidade", "").split(" - ", 1)
                    empenhos.append({
                        "numero_ata": num_ata_url,
                        "numero_item": s.get("numeroItem"),
                        "codigo_unidade": str(partes[0]).strip(),
                        "nome_unidade_alt": partes[1].strip() if len(partes) > 1 else "",
                        "tipo_alt": s.get("tipo"),
                        "qtd_reservada_alt": s.get("quantidadeRegistrada"),
                        "qtd_empenhada": s.get("quantidadeEmpenhada"),
                        "saldo_empenho": s.get("saldoEmpenho"),
                        "data_atualizacao_saldo": s.get("dataHoraAtualizacao")
                    })
    df_emp = pd.DataFrame(empenhos).drop_duplicates()

    if not df_res.empty or not df_emp.empty:
        # Merge Full para não perder ninguém (quem tem saldo mas não está na lista de participantes e vice-versa)
        df_merge = pd.merge(df_res, df_emp, on=[
                            "numero_ata", "numero_item", "codigo_unidade"], how="outer")

        # Coalesce: Preenche os dados mestre usando a melhor fonte disponível
        df_merge["nome_unidade"] = df_merge["nome_unidade"].fillna(
            df_merge.get("nome_unidade_alt", ""))
        df_merge["tipo_participacao"] = df_merge["tipo_participacao"].fillna(
            df_merge.get("tipo_alt", "NÃO INFORMADO"))
        df_merge["qtd_reservada"] = df_merge["qtd_reservada"].fillna(
            df_merge.get("qtd_reservada_alt", 0))

        # Limpa colunas de auxílio e ordena para bater com seu padrão
        cols_final = ["numero_ata", "numero_item", "codigo_unidade", "nome_unidade", "tipo_participacao",
                      "qtd_reservada", "saldo_remanejamento", "qtd_empenhada", "saldo_empenho", "data_atualizacao_saldo"]
        df_final = df_merge[[c for c in cols_final if c in df_merge.columns]]
        salvar_bases(df_final, "data/banco_atas_saldos_unidadesParticipantes")

    print(f"\n✨ Bases sincronizadas com o padrão original. Tudo pronto para o GitHub!")


if __name__ == "__main__":
    build()
