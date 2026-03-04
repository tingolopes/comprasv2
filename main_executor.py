import subprocess
import sys
import argparse
from datetime import datetime
from logging_utils import log_banner, log_event, log_section


def executar_script(nome_script):
    log_banner(f"EXECUTANDO: {nome_script}")

    try:
        resultado = subprocess.run([sys.executable, nome_script], check=False)
        log_event("INFO", "SCRIPT", f"Finalizado: {nome_script}", return_code=resultado.returncode)
        return resultado.returncode == 0
    except Exception as e:
        log_event("ERROR", "SCRIPT", f"Erro ao chamar {nome_script}: {e}")
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
        "extractors/extrator_atas.py",
        "extractors/extrator_compras.py",
        "extractors/extrator_compras_itens.py",
        "extractors/extrator_natureza_despesa.py",
        "extractors/extrator_atas_itens_saldos_unidadesParticipantes.py"
    ]

    bancos = [
        "generators/gerar_banco_atas_consolidado.py",
        "generators/gerar_banco_compras_consolidado.py",
        "generators/gerar_banco_naturezas.py",
    ]

    # Lógica de Orquestração baseada no parâmetro
    if args.acao in ["tudo", "extracao"]:
        log_section("FASE: EXTRAÇÃO INICIADA")
        for s in extracao:
            executar_script(s)

    if args.acao in ["tudo", "banco"]:
        log_section("FASE: GERAÇÃO DE BANCOS INICIADA")
        for s in bancos:
            executar_script(s)

    duracao = datetime.now() - inicio_total
    log_event("INFO", "EXECUTOR", f"Processo '{args.acao.upper()}' concluído", duracao=duracao)


if __name__ == "__main__":
    main()
