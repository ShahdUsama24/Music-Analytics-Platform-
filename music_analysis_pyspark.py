"""
================================================================================
  Music Listening Behaviour Analysis — PySpark Data Engineering Project
  Author  : [Your Name]
  Date    : 2024
  Tools   : PySpark 3.x · Python 3.10+ · Matplotlib
  Datasets: listening.csv · genre.csv
================================================================================

Project Goal:
  Analyse large-scale music streaming data to extract insights about
  - Top songs and artists globally
  - Most active listeners
  - Genre popularity and distribution
  - Monthly listening trends
  - User-level genre preferences

Portfolio Notes:
  - All DataFrames are cleaned and validated before analysis
  - Window functions demonstrate advanced SQL-equivalent patterns
  - Visualizations exported as high-resolution PNGs
  - Designed to run on a local machine (local[4]) or any Spark cluster
================================================================================
"""
# ── Install Dependencies ───────────────────────────────────────────────────────
# pip install pyspark matplotlib

# ── Imports ────────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick


# ==============================================================================
# TASK 1 — Project Introduction  (see module docstring above)
# ==============================================================================


# ==============================================================================
# TASK 2 — Initialize Spark, Load & Clean listening.csv
# ==============================================================================

# ── 2.1 · Initialize SparkSession ─────────────────────────────────────────────
spark = (SparkSession.builder
         .appName("MusicListeningAnalysis")
         .master("local[4]")           # 4 local CPU threads; swap for cluster URL
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN") # suppress INFO logs for cleaner notebooks
print(f"✓ Spark initialized  |  version: {spark.version}")


# ── 2.2 · Load the raw CSV ────────────────────────────────────────────────────
raw_df = (spark.read
          .option("header",      "true")
          .option("inferSchema", "true")
          .option("nullValue",   "N/A")   # treat literal "N/A" as null
          .csv("listening.csv"))

print(f"\nRaw rows loaded: {raw_df.count():,}")
raw_df.printSchema()
raw_df.show(5, truncate=False)


# ── 2.3 · Null audit ──────────────────────────────────────────────────────────
print("\nNull counts per column:")
null_counts = raw_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in raw_df.columns
])
null_counts.show()


# ── 2.4 · Clean the DataFrame ─────────────────────────────────────────────────
clean_df = (raw_df
    # Drop rows missing any critical column
    .dropna(subset=["user_id", "song_id", "play_count"])
    # Ensure play_count is integer (inferSchema may read as string)
    .withColumn("play_count", F.col("play_count").cast(IntegerType()))
    # Remove zero / negative play counts — data quality guard
    .filter(F.col("play_count") > 0)
    # Remove exact duplicate rows
    .dropDuplicates()
    # Trim leading/trailing whitespace from all string columns
    .withColumn("user_id",   F.trim(F.col("user_id")))
    .withColumn("song_id",   F.trim(F.col("song_id")))
    .withColumn("song_name", F.trim(F.col("song_name")))
    .withColumn("artist",    F.trim(F.col("artist")))
    # Parse timestamp string into a proper DateType for time-series analysis
    .withColumn("listen_date", F.to_date(F.col("timestamp"), "yyyy-MM-dd"))
)

# Cache in memory — avoids re-reading & re-cleaning CSV on every query
clean_df.cache()
clean_df.count()  # trigger materialisation

print(f"\n✓ Clean rows: {clean_df.count():,}")
clean_df.show(5, truncate=False)


# ==============================================================================
# TASK 3 — Queries: Basic → Intermediate → Advanced
# ==============================================================================

print("\n" + "="*60)
print("  TASK 3 — QUERIES")
print("="*60)


# ── Query 1 · Basic — Dataset summary stats ───────────────────────────────────
print("\n[Q1] Dataset Summary")
total_rows   = clean_df.count()
unique_users = clean_df.select("user_id").distinct().count()
unique_songs = clean_df.select("song_id").distinct().count()
print(f"  Total records : {total_rows:,}")
print(f"  Unique users  : {unique_users:,}")
print(f"  Unique songs  : {unique_songs:,}")


# ── Query 2 · Basic — Filter by artist name (partial, case-insensitive) ───────
print("\n[Q2] Plays Containing 'Taylor' in Artist Name")
artist_df = clean_df.filter(F.lower(F.col("artist")).contains("taylor"))
artist_df.select("song_name", "artist", "play_count").show(10)


# ── Query 3 · Basic — Songs played more than a threshold ──────────────────────
print("\n[Q3] Songs with play_count > 50")
heavy_plays = clean_df.filter(F.col("play_count") > 50)
heavy_plays.select("song_name", "artist", "play_count").show(10)


# ── Query 4 · Intermediate — Top 10 songs by total plays ─────────────────────
print("\n[Q4] Top 10 Most-Played Songs (Global)")
top_songs = (clean_df
    .groupBy("song_id", "song_name", "artist")
    .agg(F.sum("play_count").alias("total_plays"))
    .orderBy(F.desc("total_plays"))
    .limit(10))
top_songs.show(truncate=False)


# ── Query 5 · Intermediate — Most active users ────────────────────────────────
print("\n[Q5] Top 10 Most Active Users")
top_users = (clean_df
    .groupBy("user_id")
    .agg(
        F.sum("play_count").alias("total_plays"),
        F.countDistinct("song_id").alias("unique_songs_played")
    )
    .orderBy(F.desc("total_plays"))
    .limit(10))
top_users.show()


# ── Query 6 · Intermediate — Average plays per song per user ──────────────────
print("\n[Q6] Average Replay Rate per User (Top 10)")
avg_plays = (clean_df
    .groupBy("user_id")
    .agg(F.avg("play_count").alias("avg_plays_per_song"))
    .orderBy(F.desc("avg_plays_per_song"))
    .limit(10))
avg_plays.show()


# ── Query 7 · Intermediate — Top artists by total plays ───────────────────────
print("\n[Q7] Top 10 Artists by Total Plays")
top_artists = (clean_df
    .groupBy("artist")
    .agg(
        F.sum("play_count").alias("total_plays"),
        F.countDistinct("song_id").alias("songs_in_dataset")
    )
    .orderBy(F.desc("total_plays"))
    .limit(10))
top_artists.show(truncate=False)


# ── Query 8 · Advanced — Monthly listening trend ──────────────────────────────
print("\n[Q8] Monthly Listening Trend")
monthly_trend = (clean_df
    .withColumn("year_month", F.date_format(F.col("listen_date"), "yyyy-MM"))
    .groupBy("year_month")
    .agg(F.sum("play_count").alias("monthly_plays"))
    .orderBy("year_month"))
monthly_trend.show(20)


# ── Query 9 · Advanced — Window: rank songs within each artist ────────────────
print("\n[Q9] #1 Song per Artist (Window Function)")
artist_window = (Window
    .partitionBy("artist")
    .orderBy(F.desc("total_plays")))

ranked_songs = (clean_df
    .groupBy("artist", "song_name")
    .agg(F.sum("play_count").alias("total_plays"))
    .withColumn("rank_in_artist", F.rank().over(artist_window))
    .filter(F.col("rank_in_artist") == 1)
    .drop("rank_in_artist")
    .orderBy(F.desc("total_plays"))
    .limit(10))
ranked_songs.show(truncate=False)


# ==============================================================================
# TASK 4 — Load genre.csv, Join & Genre-Level Analysis
# ==============================================================================

print("\n" + "="*60)
print("  TASK 4 — GENRE ANALYSIS")
print("="*60)


# ── 4.1 · Load genre metadata ─────────────────────────────────────────────────
genre_df = (spark.read
            .option("header",      "true")
            .option("inferSchema", "true")
            .csv("genre.csv"))

print(f"\nGenre rows loaded: {genre_df.count():,}")
genre_df.printSchema()
genre_df.show(5)


# ── 4.2 · Left-join listening data with genre metadata ────────────────────────
# LEFT join: keep ALL listening records, even if a song has no genre entry.
# F.coalesce fills null genre values with the string "Unknown".
merged_df = (clean_df
    .join(genre_df, on="song_id", how="left")
    .withColumn("genre", F.coalesce(F.col("genre"), F.lit("Unknown"))))

merged_df.cache()
merged_df.count()  # materialise the join

print(f"\n✓ Merged rows: {merged_df.count():,}")
merged_df.show(5, truncate=False)


# ── 4.3 · Most popular genre by total plays ───────────────────────────────────
print("\n[Genre Q1] Genre Popularity")
genre_popularity = (merged_df
    .groupBy("genre")
    .agg(
        F.sum("play_count").alias("total_plays"),
        F.countDistinct("song_id").alias("unique_songs"),
        F.countDistinct("user_id").alias("unique_listeners")
    )
    .orderBy(F.desc("total_plays")))
genre_popularity.show()


# ── 4.4 · Genre percentage share ──────────────────────────────────────────────
print("\n[Genre Q2] Genre Share (%)")
total_plays_all = merged_df.agg(F.sum("play_count")).collect()[0][0]

genre_share = (genre_popularity
    .withColumn("pct_share",
                F.round((F.col("total_plays") / total_plays_all) * 100, 2)))
genre_share.show()


# ── 4.5 · Top genre per user (user → favourite genre) ─────────────────────────
print("\n[Genre Q3] Top Genre per User (Sample)")
user_genre_window = (Window
    .partitionBy("user_id")
    .orderBy(F.desc("plays_in_genre")))

user_top_genre = (merged_df
    .groupBy("user_id", "genre")
    .agg(F.sum("play_count").alias("plays_in_genre"))
    .withColumn("rk", F.rank().over(user_genre_window))
    .filter(F.col("rk") == 1)
    .drop("rk")
    .orderBy(F.desc("plays_in_genre")))
user_top_genre.show(10)


# ── 4.6 · Genre popularity by artist ─────────────────────────────────────────
print("\n[Genre Q4] Top Artists per Genre")
top_artist_per_genre_window = (Window
    .partitionBy("genre")
    .orderBy(F.desc("plays")))

top_artist_per_genre = (merged_df
    .groupBy("genre", "artist")
    .agg(F.sum("play_count").alias("plays"))
    .withColumn("rk", F.rank().over(top_artist_per_genre_window))
    .filter(F.col("rk") == 1)
    .drop("rk")
    .orderBy(F.desc("plays")))
top_artist_per_genre.show(truncate=False)


# ==============================================================================
# TASK 5 — Data Visualization (Matplotlib)
# ==============================================================================

print("\n" + "="*60)
print("  TASK 5 — VISUALIZATIONS")
print("="*60)

# Global Matplotlib settings
plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams.update({
    "font.family":       "DejaVu Sans",
    "axes.titlesize":    14,
    "axes.labelsize":    12,
    "xtick.labelsize":   10,
    "ytick.labelsize":   10,
    "figure.dpi":        100,
})
PALETTE = ["#3B82F6","#10B981","#F59E0B","#EF4444",
           "#8B5CF6","#EC4899","#14B8A6","#F97316","#6366F1","#84CC16"]


# ── Viz 1 · Top 10 Songs — Horizontal Bar ─────────────────────────────────────
top_songs_pd = top_songs.toPandas()
top_songs_pd["label"] = (
    top_songs_pd["song_name"].str[:28] + "  ·  " +
    top_songs_pd["artist"].str[:18]
)

fig, ax = plt.subplots(figsize=(11, 6))
bars = ax.barh(
    top_songs_pd["label"][::-1],
    top_songs_pd["total_plays"][::-1],
    color=PALETTE[:10],
    edgecolor="none",
    height=0.65,
)
for bar in bars:
    ax.text(bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2,
            f"{int(bar.get_width()):,}", va="center", fontsize=9, color="#555")

ax.set_xlabel("Total Plays")
ax.set_title("Top 10 Most-Played Songs", fontweight="bold", pad=12)
ax.xaxis.set_major_formatter(mtick.FuncFormatter(
    lambda x, _: f"{int(x/1e3)}K" if x < 1e6 else f"{x/1e6:.1f}M"))
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.savefig("top_songs.png", dpi=150, bbox_inches="tight")
print("  Saved: top_songs.png")
plt.show()


# ── Viz 2 · Genre Distribution — Pie + Bar ────────────────────────────────────
genre_pd = genre_share.toPandas().query("genre != 'Unknown'").head(8)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

# Pie chart — proportional view
wedges, texts, autotexts = ax1.pie(
    genre_pd["pct_share"],
    labels=genre_pd["genre"],
    autopct="%1.1f%%",
    colors=PALETTE[:len(genre_pd)],
    startangle=140,
    pctdistance=0.82,
    wedgeprops={"linewidth": 0.8, "edgecolor": "white"},
)
for at in autotexts:
    at.set_fontsize(9)
ax1.set_title("Genre Share (%)", fontweight="bold", pad=10)

# Bar chart — absolute view
ax2.bar(genre_pd["genre"], genre_pd["total_plays"],
        color=PALETTE[:len(genre_pd)], edgecolor="none", width=0.65)
ax2.set_title("Total Plays by Genre", fontweight="bold", pad=10)
ax2.set_xlabel("Genre")
ax2.set_ylabel("Total Plays")
ax2.tick_params(axis="x", rotation=30)
ax2.yaxis.set_major_formatter(mtick.FuncFormatter(
    lambda x, _: f"{x/1e6:.1f}M"))
ax2.spines[["top", "right"]].set_visible(False)

plt.suptitle("Music Genre Analysis", fontsize=15, fontweight="bold", y=1.01)
plt.tight_layout()
plt.savefig("genre_distribution.png", dpi=150, bbox_inches="tight")
print("  Saved: genre_distribution.png")
plt.show()


# ── Viz 3 · Monthly Listening Trend — Line Chart ──────────────────────────────
trend_pd = monthly_trend.toPandas()

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(trend_pd["year_month"], trend_pd["monthly_plays"],
        marker="o", linewidth=2.2, color="#3B82F6",
        markersize=5, markerfacecolor="white", markeredgewidth=2)
ax.fill_between(trend_pd["year_month"],
                trend_pd["monthly_plays"],
                alpha=0.08, color="#3B82F6")

ax.set_title("Monthly Listening Activity Over Time", fontweight="bold", pad=12)
ax.set_xlabel("Month")
ax.set_ylabel("Total Plays")
ax.tick_params(axis="x", rotation=45)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(
    lambda x, _: f"{x/1e3:.0f}K" if x < 1e6 else f"{x/1e6:.1f}M"))
ax.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
plt.savefig("monthly_trend.png", dpi=150, bbox_inches="tight")
print("  Saved: monthly_trend.png")
plt.show()


# ── Viz 4 · Top Users — Bar Chart ─────────────────────────────────────────────
top_users_pd = top_users.toPandas()

fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(range(len(top_users_pd)), top_users_pd["total_plays"],
       color=PALETTE[:10], edgecolor="none", width=0.65)

ax.set_xticks(range(len(top_users_pd)))
ax.set_xticklabels(top_users_pd["user_id"], rotation=30, ha="right")
ax.set_title("Top 10 Most Active Listeners", fontweight="bold", pad=12)
ax.set_ylabel("Total Plays")
ax.yaxis.set_major_formatter(mtick.FuncFormatter(
    lambda x, _: f"{x/1e3:.0f}K" if x < 1e6 else f"{x/1e6:.1f}M"))
ax.spines[["top", "right"]].set_visible(False)

plt.tight_layout()
plt.savefig("top_users.png", dpi=150, bbox_inches="tight")
print("  Saved: top_users.png")
plt.show()


# ==============================================================================
# CLEANUP
# ==============================================================================

# Unpersist cached DataFrames from memory
clean_df.unpersist()
merged_df.unpersist()

# Close the Spark session
spark.stop()
print("\n✓ Spark session terminated cleanly.")

"""
================================================================================
  REAL-WORLD IMPROVEMENTS & USE CASES
================================================================================

1. Schema Enforcement
   Replace inferSchema with an explicit StructType schema in production to
   prevent silent type mismatches and schema drift between CSV versions.

2. Delta Lake / Parquet
   Replace CSV with columnar formats (Parquet / Delta Lake) for 10-100x faster
   reads, predicate pushdown, and ACID transaction guarantees.

3. Recommendation Engine
   Feed user_top_genre and top_songs into PySpark MLlib's ALS collaborative
   filtering model to build a personalised song recommendation system.

4. Real-Time Streaming
   Swap spark.read.csv() for spark.readStream.format("kafka") to process
   live listening events from a Kafka broker — enabling real-time dashboards.

5. Data Quality Framework
   Integrate Great Expectations or Deequ (Amazon's PySpark data quality lib)
   to add automated data validation before every pipeline run.

6. Cloud Deployment
   Deploy this pipeline on AWS EMR, Google Dataproc, or Azure HDInsight
   to process datasets of billions of rows cost-efficiently.
================================================================================
"""
