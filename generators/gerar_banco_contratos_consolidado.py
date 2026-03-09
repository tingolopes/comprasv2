import os
import json
import pandas as pd

# --- CONFIGURAÇÃO DE CAMINHOS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASTA_TEMP = os.path.join(BASE_DIR, "temp", "contratos")
PASTA_DATA = os.path.join(BASE_DIR, "data")

os.makedirs(PASTA_DATA, exist_ok=True)


def capitalizar(texto):
    if not texto:
        return ""
    return str(texto).strip().capitalize()


def processar_contratos_completo():
    print("📊 Iniciando consolidação: Contratos, Responsáveis e Itens...")

    lista_contratos = []
    lista_responsaveis = []
    lista_itens = []

    if not os.path.exists(PASTA_TEMP):
        print(f"⚠️ Pasta {PASTA_TEMP} não encontrada.")
        return

    arquivos = [f for f in os.listdir(PASTA_TEMP) if f.endswith(".json")]

    for arq in arquivos:
        caminho = os.path.join(PASTA_TEMP, arq)
        try:
            with open(caminho, 'r', encoding='utf-8') as f:
                envelope = json.load(f)
                res = envelope.get("respostas", {}).get("resultado", [])
                if not isinstance(res, list):
                    res = [res]

                # --- 1. CONTRATOS ---
                if arq.startswith("contratos_uasg_"):
                    for c in res:
                        # Acessa o objeto fornecedor com segurança
                        fornecedor_obj = c.get("fornecedor", {})
                        # Aqui é onde o nome realmente está
                        nome_bruto = fornecedor_obj.get("nome", "")
                        lista_contratos.append({
                            "id_contrato": c.get("id"),
                            "origem_sigla": c.get("origem_sigla"),
                            "tipo": c.get("tipo"),
                            "modalidade": c.get("modalidade"),
                            "processo": c.get("processo"),
                            "numero": c.get("numero_contrato") or c.get("numero"),
                            "unidade_compra": c.get("unidade_compra"),
                            "codigo_modalidade": c.get("codigo_modalidade"),
                            "licitacao_numero": c.get("licitacao_numero"),
                            "fornecedor_nome": nome_bruto,
                            "link_responsaveis": f"https://contratos.comprasnet.gov.br/api/contrato/{c.get('id')}/responsaveis",
                            "prorrogavel": c.get("prorrogavel"),
                            "valor_parcela": c.get("valor_parcela"),
                            "vigencia_inicio": c.get("vigencia_inicio"),
                            "vigencia_fim": c.get("vigencia_fim")
                        })

                # --- 2. RESPONSÁVEIS ---
                elif arq.startswith("responsaveis_"):
                    for r in res:
                        lista_responsaveis.append({
                            "id_responsavel": r.get("id"),
                            "id_contrato": r.get("id_contrato_origem"),
                            "usuario": r.get("usuario", "").split("-")[-1].strip() if r.get("usuario") else "",
                            "funcao_id": r.get("funcao_id"),
                            "portaria": r.get("portaria"),
                            "data_inicio": r.get("data_inicio"),
                            "data_fim": r.get("data_fim"),
                            "situacao": r.get("situacao")
                        })

                # --- 3. ITENS (NOVO) ---
                elif arq.startswith("itens_"):
                    for i in res:
                        lista_itens.append({
                            "id_contrato": i.get("id_contrato_origem"),
                            "item_id": i.get("id"),
                            # Material ou Serviço
                            "tipo_material": i.get("tipo_id"),
                            "descricao": capitalizar(i.get("catmatseritem_id")),
                            "descricao_detalhada": capitalizar(i.get("descricao_complementar")),
                            "quantidade": i.get("quantidade"),
                            "valor_unitario": i.get("valorunitario"),
                            "valor_total": i.get("valortotal"),
                            "numero_item_compra": i.get("numero_item_compra")
                        })
        except Exception as e:
            print(f"⚠️ Erro ao processar {arq}: {e}")

    # --- SALVAMENTO ---
    salvar_csv(lista_contratos, "banco_contratos.csv")
    salvar_csv(lista_responsaveis, "banco_contratos_responsaveis.csv")
    salvar_csv(lista_itens, "banco_contratos_itens.csv")


def salvar_csv(lista, nome_arquivo):
    if lista:
        df = pd.DataFrame(lista).drop_duplicates()
        caminho = os.path.join(PASTA_DATA, nome_arquivo)
        df.to_csv(caminho, index=False, sep=';', encoding='utf-8-sig')
        print(f"✅ {nome_arquivo} gerado: {len(df)} linhas.")


if __name__ == "__main__":
    processar_contratos_completo()
