# scripts/generate_fomc_sentiment.py
"""
Downloads FOMC meeting minutes and performs simple sentiment scoring.
Outputs: data/static/fomc_sentiment.csv
"""

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Create output directory
os.makedirs("data/static", exist_ok=True)

# Scrape FOMC minutes links
URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
soup = BeautifulSoup(requests.get(URL).text, "html.parser")
links = [a["href"] for a in soup.select("a[href*='monetarypolicy/fomcminutes']")]

# Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()
records = []

# Loop through links
for link in links:
    if not link.startswith("https"):
        link = f"https://www.federalreserve.gov{link}"

        # Fetch page and parse HTML
        response = requests.get(link)
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract text and title
        title = soup.title.get_text() if soup.title else ""
        text = soup.get_text()

        # Try to extract meeting date from the title
        date_str = None
        for word in title.split():
            if any(month in word for month in ["January", "February", "March", "April", "May", "June",
                                               "July", "August", "September", "October", "November", "December"]):
                date_str = " ".join(title.split()[-3:])
                break

        # Sentiment scoring
        score = analyzer.polarity_scores(text)

        # Metadata
        import datetime as dt

        word_count = len(text.split())
        retrieved_on = dt.datetime.now().strftime("%Y-%m-%d %H:%M")


    records.append({
        "url": link,
        "title": title,
        "date": date_str,
        "year": date_str.split()[-1] if date_str else None,
        "word_count": word_count,
        "sentiment_source": "VADER",
        "text_excerpt": text[:400],
        "retrieved_on": retrieved_on,
        **score
    })

# Save results
df = pd.DataFrame(records)
df["regime"] = df["compound"].apply(lambda x: "dovish" if x>0.2 else ("hawkish" if x<-0.2 else "neutral"))
df.to_csv("data/static/fomc_sentiment.csv", index=False)
print("✅ FOMC sentiment data saved to data/static/fomc_sentiment.csv")
