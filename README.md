![Market Pulse Logo](./docs/market-pulse-header.png)

**Table of Contents**
- [1. Overview](#1-overview)
  - [1.1 Problem Statement](#11-problem-statement)
  - [1.2 Purpose](#12-purpose)
  - [1.3 End User](#13-end-user)
  - [1.4 Data Sources](#14-data-sources)
- [2 Design \& Development](#2-design--development)
  - [2.1 Solution Architecture](#21-solution-architecture)
  - [2.2 Tech Stack](#22-tech-stack)
  - [2.3  Conceptual Data Model](#23-onceptual-data-model)
  - [2.4 Dashboard](#24-dashboard)
- [3 Challenges](#3-challenges)
- [4 Future Enhancements](#4-future-enhancements)


<br>

# 1. Overview
## 1.1. Problem Statement 
The average retail investor interested in stock market investing often faces an overwhelming amount of information, making it challenging to gain quick, actionable insights into a company's stock performance and public sentiment. This information overload can lead to decision paralysis or uninformed investment choices. 

## 1.2. Purpose
The Market Pulse dashboard addresses this issue by providing a streamlined, user-friendly interface that offers immediate insight into a company's stock performance and public sentiment from online forums such as Reddit. This enables investors by providing them with a general sense of a company's performance relative to their industry and a guide to focus their more in-depth research.

## 1.3. End User
The average retail investor who is interested in stock investing and is beginning
to gather information about a prospective company they may want to invest in.

## 1.4. Data Sources
- [DataHub.io API](https://datahub.io/core/s-and-p-500-companies)
- [Polygon.io API](https://polygon.io/docs/rest/stocks/overview)
- [Reddit API](https://www.reddit.com/dev/api/oauth)

# 2. Design & Development

## 2.1. Solution Architecture
![Solution Architecture Diagram](./docs/solution-architecture-v2.png)

## 2.2. Tech Stack
- **Storage:** AWS S3, Delta Lake, Parquet
- **Data Processing:** Databricks, Hugging Face, Apache Spark, dbt Core
- **Data Visualization:** Databricks Dashboards
- **Orchestration:** Astronomer/Airflow
- **DevOps:** GitHub, GitHub Actions

## 2.3. Conceptual Data Model
![Conceptual Data Model Diagram](./docs/conceptual-data-model.png)

## 2.4. Dashboard
https://github.com/user-attachments/assets/1f82142e-505e-46d8-9a7a-5b431d633d1a

# 3. Challenges
1. **API Rate Limits:** The Reddit API has a rate limit of 100 queries per minute (QPM) per OAuth client ID for
their Free tier leading to the need to introduce defined wait periods to avoid reaching the rate limit
2. **Infrastructure Integrations:** When integrating Databricks with Astronomer and dbt, there were some difficulties because the Databricks instance was managed externally by the data bootcamp leading to additional research and prototyping being required to ensure proper connectivity.
3. **Data Visualization Limitations:** Using Databricks Dashboards was a simple and appealing choice for building the dashboard since it is integrated very well with the Databricks platfrom, but due to the limited data visual options this made it diffcult to present the data, particularly the sentiment analysis data, in a compelling way. For example, it would have been more impactful if it was possible to display the sentiment data on the same visual as the stock price data to observe that relationship more closely and easily.

# 4. Future Enhancements
1. **Additional Data Souces:** Integrate source data from addtional social media networks (e.g. Twitter, Blue Sky, etc.) for a more comprehensive and less biased sentiment analysis
2. **Additional Reddit Data:** Add posts data from additional stock/investing related subreddits. Currently, data was scraped from four of the top stock/investing subreddits but this could be expanded to the top ten stock/investing subreddits for better
3. **Alternative Data Visualization Application:** To improve the data visualization component of the solution,  a more comprehensive and fully-featured application could be considered such as Apache Superset or Tableau. This would offer more compelling visuals to choose from leading to more value being extracted from the data and a better user experience.