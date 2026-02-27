import json
import os
from pathlib import Path


def realizar_diagnostico_profundo():
    pastas_origem = [
        'temp/temp_atas_arp', 'temp/temp_compras_14133', 'temp/temp_compras_legado',
        'temp/temp_itens_14133_id', 'temp/temp_itens_atas_id', 'temp/temp_itens_legado_id',
        'temp/temp_saldos_atas_id', 'temp/temp_unidades_atas_id'
    ]

    arquivo_relatorio = Path('diagnostico_jsons_validados.txt')
    MAX_ARQUIVOS_COM_DADOS = 5  # Analisaremos até 5 arquivos REAIS com dados por pasta

    print("Iniciando varredura profunda por arquivos com conteúdo...")

    with open(arquivo_relatorio, 'w', encoding='utf-8') as f:
        f.write(
            "=== RELATÓRIO DE DIAGNÓSTICO PROFUNDO (APENAS ARQUIVOS COM RESULTADOS) ===\n\n")

        for nome_pasta in pastas_origem:
            pasta = Path(nome_pasta)
            if not pasta.exists():
                continue

            f.write(f"\n{'='*20} PASTA: {nome_pasta} {'='*20}\n")
            print(f"Analisando pasta: {nome_pasta}")

            arquivos_na_pasta = list(pasta.glob('*.json'))
            encontrados_nesta_pasta = 0

            for arquivo in arquivos_na_pasta:
                if encontrados_nesta_pasta >= MAX_ARQUIVOS_COM_DADOS:
                    break

                try:
                    with open(arquivo, 'r', encoding='utf-8') as j:
                        dados = json.load(j)
                        resultados = dados.get(
                            'respostas', {}).get('resultado', [])

                        # FILTRO CRÍTICO: Só processa se o resultado tiver algo dentro
                        if not resultados or len(resultados) == 0:
                            continue

                        encontrados_nesta_pasta += 1
                        exemplo_reg = resultados[0]
                        chaves = sorted(exemplo_reg.keys())

                        f.write(
                            f"\n[Arquivo {encontrados_nesta_pasta}]: {arquivo.name}\n")
                        f.write(
                            f"Total de registros neste arquivo: {len(resultados)}\n")
                        f.write(f"CHAVES ENCONTRADAS:\n")

                        for chave in chaves:
                            valor = exemplo_reg.get(chave)
                            # Mostra o valor para sabermos se o objeto está preenchido ou é 'null'
                            valor_str = str(valor)[
                                :120] + "..." if len(str(valor)) > 120 else str(valor)
                            f.write(f"    - {chave}: {valor_str}\n")

                        f.write("-" * 30 + "\n")

                except Exception as e:
                    continue  # Pula arquivos corrompidos

            if encontrados_nesta_pasta == 0:
                f.write(
                    "!!! AVISO: Nenhum arquivo com dados reais (resultado > 0) foi encontrado nesta pasta !!!\n")

    print(f"\nPronto! O novo diagnóstico está em: {arquivo_relatorio}")


if __name__ == "__main__":
    realizar_diagnostico_profundo()
