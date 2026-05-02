# Music-Analytics-Platform
PySpark Data Engineering Pipeline + Streamlit Interactive Dashboard

# 🎧 Music Analytics Platform  
### PySpark Data Engineering Pipeline + Streamlit Interactive Dashboard

![Python](https://img.shields.io/badge/Python-3.x-blue)
![PySpark](https://img.shields.io/badge/PySpark-BigData-orange)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![Status](https://img.shields.io/badge/Status-Active-success)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

---

##  Overview

Modern music platforms generate massive user interaction data — but raw data alone is useless without insights.

This project builds an **end-to-end data analytics system** that:
- Processes large-scale music listening data using **PySpark**
- Extracts meaningful behavioral insights
- Presents them through an **interactive Streamlit dashboard**

---

##  Problem Statement

Understanding user listening behavior is crucial for:
-  Personalization
-  Engagement growth
-  Content optimization

This project answers:
- What songs are trending?
- Who are the most active users?
- How do listening patterns evolve over time?
- Which genres dominate engagement?

---

---

##  Features

###  Data Engineering (PySpark)
- Null handling & schema enforcement
- Data deduplication
- Aggregations (top songs, users, trends)
- Window functions (ranking songs per artist)
- Genre enrichment using joins

###  Dashboard (Streamlit + Plotly)
- KPI metrics (Total Plays, Songs, Artists)
- Interactive filters (artist, color theme)
- Dynamic visualizations:
  - Pie & Donut charts
  - Trend lines
  - Bar charts

---

##  Tech Stack

| Layer | Tools |
|------|------|
| Data Processing | PySpark |
| Data Analysis | Python |
| Visualization | Matplotlib, Plotly |
| Dashboard | Streamlit |
| Environment | Jupyter / Local |

---

##  Dataset

The dataset simulates music streaming behavior:
- User IDs
- Song titles
- Artists
- Genres
- Timestamps
- Play counts

---

##  Data Pipeline Steps

1. **Data Loading**
   - Load raw dataset into PySpark DataFrame

2. **Data Cleaning**
   - Handle null values
   - Cast data types
   - Remove duplicates

3. **Transformations**
   - Normalize columns
   - Feature engineering

4. **Aggregations**
   - Top songs by play count
   - Most active users
   - Listening trends over time

5. **Window Functions**
   - Rank songs per artist

6. **Joins**
   - Combine genre metadata

7. **Export**
   - Save processed data for dashboard consumption

---

##  Dashboard Preview

The Streamlit dashboard includes:

-  **KPIs**
  - Total Plays
  - Unique Songs
  - Unique Artists

-  **Filters**
  - Artist selection
  - Custom color themes

-  **Charts**
  - Top Songs (Bar Chart)
  - Genre Distribution (Pie/Donut)
  - Listening Trends (Line Chart)

<img width="1860" height="620" alt="Screenshot 2026-05-02 135724" src="https://github.com/user-attachments/assets/85dbd2f1-458b-440e-9d58-e3cd5cf85f50" />
<img width="1860" height="620" alt="Screenshot 2026-05-02 135744" src="https://github.com/user-attachments/assets/1d3df5d6-d8f4-4027-97db-74163c997df2" />
<img width="1860" height="620" alt="Screenshot 2026-05-02 135807" src="https://github.com/user-attachments/assets/3cd9c868-d928-41d0-bd54-d86887d27cc9" />




---

## Author

**[Shahd Usama]**
Data Analyst | Python · SQL · Machine Learning

[![[LinkedIn](https://www.linkedin.com/in/shahdusama/)](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)]
[![[GitHub](https://github.com/ShahdUsama24)](https://img.shields.io/badge/GitHub-Follow-181717?style=flat&logo=github)]


*Data sources are publicly available on Kaggle. This project is for educational and portfolio purposes.*
