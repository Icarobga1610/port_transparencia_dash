# Dashboard de Transparência Pública

Pipeline automatizado para análise de gastos públicos com IA para detecção de discrepâncias.

## Arquitetura

- **Ingestão**: Prefect Cloud + API Portal da Transparência
- **Dados**: MongoDB Atlas (raw → silver)
- **IA**: Maritaca AI (Sabiá-3) para detecção de fraudes
- **Dashboard**: Power BI Service (sem gateway)
- **Site**: Netlify/Vercel (embed + chat IA)

## Estrutura do projeto

