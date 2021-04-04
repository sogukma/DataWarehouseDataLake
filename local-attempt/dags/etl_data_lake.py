import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import pandas as pd


ETL_DATA_DIRECTORY = "/opt/airflow/data"
NLTK_DATA_DIRECTORY = "/tmp"

logger = logging.getLogger(__name__)


def extract_reviews(city: str) -> pd.DataFrame:
    """
    Extract reviews from a CSV file.
    """
    reviews = pd.read_csv(
        f"{ETL_DATA_DIRECTORY}/{city}/reviews.csv",
        parse_dates=["date"],
        dtype={
            "reviewer_name": "string",
            "comments": "string",
        },
    )

    # replace NAs with empty strings
    reviews["comments"] = reviews["comments"].fillna("")

    return reviews


@dag(
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["example"],
)
def data_lake_etl():
    """
    This is a the data lake ETL pipeline.
    """
    @task()
    def extract() -> pd.DataFrame:
        """
        Extract the reviews from all CSV files and combine them to a single data frame.
        """
        reviews = pd.concat([
                extract_reviews("Boston"),
                extract_reviews("Seattle"),
            ],
            ignore_index=True,
        )

        logger.info(reviews.shape)
        logger.info(reviews.info())

        return reviews

    @task()
    def transform(raw_reviews: pd.DataFrame) -> pd.DataFrame:
        """
        Applies sentiment analysis on the reviews.
        """
        transformed_reviews = raw_reviews.copy()

        # prepare the sentiment analyzer
        nltk.download("vader_lexicon", download_dir=NLTK_DATA_DIRECTORY)
        nltk.data.path.append(NLTK_DATA_DIRECTORY)
        sentiment_analyzer = SentimentIntensityAnalyzer()

        sentiments = []
        for _, review in transformed_reviews["comments"].items():
            sentiment_compound = sentiment_analyzer.polarity_scores(review)["compound"]
    
            # decide sentiment as positive, negative and neutral
            if sentiment_compound >= 0.05:
                sentiment = "positive"
            elif sentiment_compound <= - 0.05:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            sentiments.append(sentiment)
            
        transformed_reviews["sentiment"] = sentiments
            
        return transformed_reviews

    @task()
    def load(transformed_reviews: pd.DataFrame) -> None:
        """
        Save the transformed reviews as a source for the data warehouse.
        """
        transformed_reviews.to_csv(
            f"{ETL_DATA_DIRECTORY}/transformed_reviews.csv",
            index=False,
        )

    raw_reviews = extract()
    transformed_reviews = transform(raw_reviews)
    load(transformed_reviews)


etl_data_lake_dag = data_lake_etl()
