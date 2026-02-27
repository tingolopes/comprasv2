# 📊 Esteira de Dados de Compras Públicas (IFMS)

Pipeline em Python para **extração, enriquecimento e consolidação** de dados de compras públicas e Atas de Registro de Preços (ARP), com foco em consumo no **Power BI**.

---

## ✨ Visão geral

Este repositório automatiza uma esteira que:

1. consulta APIs públicas de compras;
2. salva respostas brutas em JSON (camada *data lake*);
3. reaproveita cache para evitar chamadas desnecessárias;
4. consolida os dados em CSVs prontos para análise.

A execução é orquestrada por `main_executor.py`, que permite rodar extração, consolidação ou ambos os fluxos em sequência.

---

## 🧱 Arquitetura de dados

### 1) Data Lake (bruto)
- Armazenado em `temp/` (subpastas por domínio).
- Arquivos JSON com envelope padronizado:
  - `metadata.url_consultada`
  - `metadata.data_extracao`
  - `metadata.status`
  - `respostas`
- Estratégias de robustez:
  - *skip* quando o arquivo já existe com status `SUCESSO`;
  - retentativas/backoff em cenários de falha e limite de API (HTTP 429);
  - atualização periódica de saldos com janela de validade.

### 2) Data Warehouse (consolidado)
- Saída em `data/` para uso direto em BI:
  - `banco_atas.csv`
  - `banco_atas_itens.csv`
  - `banco_atas_saldos_unidadesParticipantes.csv`
  - `banco_compras.csv`
  - `banco_compras_itens.csv`
  - `banco_naturezas_despesa.csv`

---

## 📁 Estrutura do projeto

```text
.
├── .github/workflows/                      # Execução automática e reprocessamento manual
├── data/                                   # CSVs consolidados para análise
├── diagnostics/                            # Scripts utilitários de diagnóstico
├── docs/                                   # Documentação de referência
├── extractors/                             # Coleta e enriquecimento (JSON bruto)
├── generators/                             # Consolidação para CSV
├── main_executor.py                        # Orquestrador da esteira
└── README.md
```

---

## ⚙️ Requisitos

- Python **3.11+**
- Dependências:
  - `requests`
  - `pandas`
  - `pyarrow` (usado no fluxo de CI)

Instalação rápida:

```bash
pip install requests pandas pyarrow
```

---

## ▶️ Como executar

### Fluxo completo (extração + geração de bancos)

```bash
python main_executor.py tudo
```

### Apenas extração (download/enriquecimento JSON)

```bash
python main_executor.py extracao
```

### Apenas geração dos bancos CSV

```bash
python main_executor.py banco
```

---

## 🔄 Ordem da esteira

O orquestrador chama os scripts nesta sequência:

### Fase 1 — Extração
1. `extractors/extrator_atas.py`
2. `extractors/extrator_compras.py`
3. `extractors/extrator_compras_itens.py`
4. `extractors/extrator_natureza_despesa.py`
5. `extractors/extrator_atas_itens_saldos_unidadesParticipantes.py`

### Fase 2 — Consolidação
1. `generators/gerar_banco_atas_consolidado.py`
2. `generators/gerar_banco_compras_consolidado.py`
3. `generators/gerar_banco_naturezas.py`

---

## 📤 Saídas geradas

Após rodar o pipeline completo, os principais artefatos serão:

- **Dados brutos** em `temp/` (JSON versionado por data/hora de extração);
- **Bases consolidadas** em `data/*.csv` com separador `;` e codificação UTF-8 BOM (`utf-8-sig`), facilitando importação no Power BI.

---

## 🤖 Automação com GitHub Actions

A pasta `.github/workflows/` contém fluxos para:

- atualização diária automatizada;
- reprocessamento manual apenas dos bancos consolidados.

Também há estratégia de cache para preservar o *data lake* em `temp/` entre execuções do CI, reduzindo tempo e carga de API.

---

## 🛡️ Boas práticas do pipeline

- **Idempotência**: evita reprocessamento desnecessário quando o JSON já foi extraído com sucesso.
- **Resiliência**: utiliza tentativas e espera progressiva em chamadas de API.
- **Escalabilidade controlada**: uso de `ThreadPoolExecutor` com limites de *workers* por domínio.
- **Rastreabilidade**: toda saída bruta registra URL consultada e timestamp da extração.

---

## 🧪 Diagnóstico e suporte

Scripts de apoio disponíveis em `diagnostics/`:

- `diagnostico_jsons_brutos.py`
- `coletar_jsons_modelos.py`
- `verificar_integridade.py`
- `diagnostico_jsons_validados.txt` (relatório de referência)

Use-os para auditar integridade, estrutura e consistência dos JSONs antes de consolidar.

---

## 👤 Público-alvo

Este projeto é útil para equipes de:

- gestão e planejamento de compras;
- auditoria e conformidade;
- inteligência de dados e BI;
- apoio à tomada de decisão administrativa.

---

## 📌 Observações

- A pasta `temp/` pode crescer rapidamente (alto volume de JSON).
- Em ambientes com limitação de rede/API, priorize execução incremental.
- Se houver mudança de schema nas APIs, revise os extratores e geradores antes de publicar novos CSVs.

---

## 📬 Contato e evolução

Sugestões de melhoria são bem-vindas — especialmente para:

- novos indicadores analíticos;
- melhoria de qualidade de dados;
- otimização de performance e custo de execução.

Se quiser, posso também preparar uma versão deste README com:

- **diagrama da arquitetura**,
- **dicionário de dados inicial dos CSVs**,
- e **guia de integração com Power BI**.
