![Market Pulse Logo](./docs/market-pulse-header.png)
- [Overview](#overview)
  - [Problem Statement](#problem-statement)
  - [Purpose](#purpose)
  - [End User](#end-user)
  - [Data Sources](#data-sources)
- [Design \& Development](#design--development)
  - [Solution Architecture](#solution-architecture)
  - [Tech Stack](#tech-stack)
  - [Conceptual Data Model](#conceptual-data-model)
  - [Dashboard](#dashboard)
- [Challenges](#challenges)
- [Future Enhancements](#future-enhancements)

<br>

# Overview
## Problem Statement 
The average retail investor interested in stock market investing often faces an overwhelming amount of information, making it challenging to gain quick, actionable insights into a company's stock performance and public sentiment. This information overload can lead to decision paralysis or uninformed investment choices. 

## Purpose
The Market Pulse dashboard addresses this issue by providing a streamlined, user-friendly interface that offers immediate insight into a company's stock performance and public sentiment from online forums such as Reddit. This enables investors by providing them with a general sense of a company's performance relative to their industry and a guide to focus their more in-depth research.

## End User
The average retail investor who is interested in stock investing and is beginning
to gather information about a prospective company they may want to invest in.

## Data Sources
- DataHub.io API
- Polygon.io API
- Reddit API

# Design & Development

## Solution Architecture
![Solution Architecture Diagram](./docs/solution-architecture-v2.png)

## Tech Stack
- **Storage:** AWS S3, Delta Lake, Parquet
- **Data Processing:** Databricks, Hugging Face, Apache Spark, dbt
- **Data Visualization:** Databricks Dashboards
- **Orchestration:** Astronomer/Airflow
- **DevOps:** GitHub, GitHub Actions

## Conceptual Data Model
![Conceptual Data Model Diagram](./docs/conceptual-data-model.png)

## Dashboard
https://github.com/user-attachments/assets/1f82142e-505e-46d8-9a7a-5b431d633d1a

# Challenges
- The Reddit API has a rate limit of 100 queries per minute (QPM) per OAuth client ID for
their Free tier which caused issues with extracting Reddit posts data

# Future Enhancements
1. Source data from addtional social media networks (e.g. Twitter, Blue Sky, etc.) for more comprehensive sentiment analysis
2. Add posts data from additional stock/investing related subreddits
