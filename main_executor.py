import os
import subprocess
import sys
import argparse
from datetime import datetime


def executar_script(nome_script):
    tstamp = datetime.now().strftime('%H:%M:%S')
    print(f"\n{'='*60}")
    print(f"[{tstamp}] 🚀 EXECUTANDO: {nome_script}")
    print(f"{'='*60}")

    try:
        resultado = subprocess.run([sys.executable, nome_script], check=False)
        return resultado.returncode == 0
    except Exception as e:
        print(f"❌ Erro ao chamar {nome_script}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Orquestrador da Esteira de Dados IFMS")

    # Define as opções de comando
    parser.add_argument("acao",
                        choices=["tudo", "extracao", "banco"],
                        help="Escolha o que executar: 'tudo', 'extracao' (apenas download) ou 'banco' (apenas consolidar)")

    args = parser.parse_args()
    inicio_total = datetime.now()

    # Listas de scripts por fase
    extracao = [
        # Atas
        "extractors/extrator_atas.py",
        "extractors/extrator_atas_itens_saldos_unidadesParticipantes.py"

        # Compras
        "extractors/extrator_compras.py",
        "extractors/extrator_compras_itens.py",

        # Natureza de Despesa
        "extractors/extrator_natureza_despesa.py",

        # Contratos
        "extractors/extrator_contratos.py",
    ]

    bancos = [
        "generators/gerar_banco_atas_consolidado.py",
        "generators/gerar_banco_compras_consolidado.py",
        "generators/gerar_banco_naturezas.py",
        "generators/gerar_banco_contratos_consolidado.py"
    ]

    # Lógica de Orquestração baseada no parâmetro
    if args.acao in ["tudo", "extracao"]:
        print("\n🛰️  FASE: EXTRAÇÃO INICIADA")
        for s in extracao:
            executar_script(s)

    if args.acao in ["tudo", "banco"]:
        print("\n🏗️  FASE: GERAÇÃO DE BANCOS INICIADA")
        for s in bancos:
            executar_script(s)

    duracao = datetime.now() - inicio_total
    print(f"\n✨ PROCESSO '{args.acao.upper()}' CONCLUÍDO EM: {duracao}")

    print("\n--- 🏁 Gerando Metadados ---")
    os.system("python generators/gerar_metadados.py")


if __name__ == "__main__":
    main()
