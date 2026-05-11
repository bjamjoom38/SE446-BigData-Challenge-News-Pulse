# SE446 Big Data Challenge: News Pulse

## Students

- Bakr Jamjoom
- Saud Dawood

## Project Summary

News Pulse is a simplified real-time big-data pipeline built with Python and PySpark. The project reads news headlines from a local RSS XML file, processes the records in simulated streaming batches, and prints useful aggregations to the console.

The goal is to demonstrate an end-to-end big-data workflow within a limited challenge environment, not to build a production-grade streaming system.

## What The Pipeline Does

1. Reads headline records from `data/newsfeed.xml`.
2. Converts each RSS item into a structured record with title, source, publication date, link, and ingestion time.
3. Simulates streaming by processing the records in small batches.
4. Uses PySpark DataFrames to process each batch.
5. Prints:
   - incoming headlines,
   - headline counts by source,
   - trending keywords per batch,
   - final cumulative summary.

## Project Structure

```text
.
├── main.py
├── requirements.txt
├── README.md
└── data/
    └── newsfeed.xml
```

## Requirements

Install the required packages:

```bash
pip install -r requirements.txt
```

## How To Run

Run the pipeline with:

```bash
python main.py
```

The script will load the RSS records, process them in simulated batches, and print the News Pulse results in the terminal.

## Notes

- The project uses simulated streaming batches instead of Kafka or a production stream source.
- This approach satisfies the challenge requirement for incremental or continuous-style processing.
- The local XML file is used so the demo is reproducible without depending on a live API.
