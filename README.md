📊 Esteira de Dados IFMS - Naviraí 🚀
Este projeto automatiza a extração, processamento e consolidação de dados de compras públicas e atas de registro de preços do IFMS, utilizando a API do PNCP (Lei 14.133) e sistemas legados.

🏗️ Arquitetura do Sistema
O projeto utiliza uma estrutura de Nível Superior para garantir performance e economia de API:

Data Lake (Bruto): Milhares de arquivos JSON armazenados na pasta temp/.

GitHub Actions Cache: Os arquivos brutos são preservados entre as execuções via cache (Linux-data-lake-v1), evitando downloads duplicados.

Data Warehouse (Consolidado): Bases em CSV geradas na pasta data/ para consumo direto no Power BI.

📁 Estrutura de Pastas
Plaintext
├── .github/workflows/  # Automação diária (GitHub Actions)
├── data/               # Bases CSV consolidadas (Power BI)
├── generators/         # Scripts de transformação e limpeza
├── temp/               # JSONs brutos (Data Lake - Ignorado pelo Git)
├── main_executor.py    # Orquestrador principal da esteira
└── .gitignore          # Proteção contra upload de arquivos pesados
⚙️ Como Funciona
Extração: O main_executor.py consulta a API e salva os JSONs em temp/.

Smart Skip: O sistema verifica se o arquivo já existe e se o status é SUCESSO antes de baixar.

Saldos: Itens com saldo empenhado são revisitados periodicamente com base na DIAS_VALIDADE_SALDO.

Consolidação: Os scripts em generators/ unem os dados de Atas, Itens e Unidades Participantes.

🚀 Execução
Para rodar a esteira completa localmente:

Bash
python main_executor.py tudo