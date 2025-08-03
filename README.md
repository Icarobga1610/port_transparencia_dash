# Dashboard de Transparência Pública

Pipeline automatizado para análise de gastos públicos com IA para detecção de discrepâncias.

## Arquitetura

- **Ingestão**: Prefect Cloud + API Portal da Transparência
- **Dados**: MongoDB Atlas (raw → silver)
- **IA**: Maritaca AI (Sabiá-3) para detecção de fraudes
- **Dashboard**: Power BI Service (sem gateway)
- **Site**: Netlify/Vercel (embed + chat IA)

## Estrutura do projeto

```
port_transparencia_dash/
├── data_ingestion/         # Scripts de coleta e ingestão de dados
├── data_processing/        # Limpeza, transformação e enriquecimento dos dados
├── ai_detection/           # Modelos e pipelines de IA para detecção de discrepâncias
├── dashboard/              # Relatórios e arquivos do Power BI
├── web/                    # Código do site (embed do dashboard + chat IA)
├── configs/                # Configurações e credenciais (ex: Prefect, MongoDB)
├── docs/                   # Documentação adicional
└── README.md               # Este arquivo
```