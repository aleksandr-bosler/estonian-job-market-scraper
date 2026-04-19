# Estonian Job Market Scraper

A multi-source ETL pipeline that collects, deduplicates, and merges job vacancies from three Estonian job portals: **cv.ee**, **cvkeskus.ee**, and **tootukassa.ee**.

## Background

Built as part of [**MKM-POL50 - "The future of work in Estonia: AI automation, skills, and work organization"**](https://www.etis.ee/Portal/Projects/Display/e49b9439-16d4-40a2-9ff6-6932acbef416) (2026–2027), a government-funded research project under the Estonian Ministry of Economic Affairs and Communications.

The project analyzes how AI and automation affect jobs, skills, and work organization in Estonia. This scraper is responsible for data collection: gathering job postings at scale for downstream NLP analysis - identifying which roles require AI skills, how frequently, and what specific competencies are in demand.

## Key Features

- Handles three different source structures: REST API, HTML pagination, and GraphQL
- OCR support for job listings published as images (Qwen3.5-9B via LM Studio)
- AI-powered deduplication using a local LLM (Qwen3.5-9B via LM Studio)
- Resume-friendly: all scraping steps save progress and can be interrupted and restarted

## Setup

```bash
pip install -r requirements.txt
```

For OCR and AI deduplication, [LM Studio](https://lmstudio.ai/) must be running locally with a Qwen model loaded on `http://localhost:1234`.

## Running the Pipeline

Scripts are meant to be run in order:

```
# 1. Collect job URLs
python src/collectors/cv_collect_urls.py
python src/collectors/cvkeskus_collect_urls.py

# 2. Scrape full job listings
python src/scrapers/cv_scrape.py
python src/scrapers/cvkeskus_scrape.py
python src/scrapers/tootukassa_scrape.py

# 3. (Optional) OCR for image-based listings
python src/ocr/cv_ocr.py
python src/ocr/cvkeskus_ocr.py

# 4. Deduplicate and merge cv.ee + cvkeskus.ee
python src/deduplication/compare_cross_site_duplicates.py
python src/deduplication/review_cross_site_duplicates.py
python src/merging/merge_cross_site_jobs.py

# 5. Deduplicate and merge with tootukassa.ee
python src/deduplication/compare_merged_vs_tootukassa_duplicates.py
python src/deduplication/review_merged_vs_tootukassa_duplicates.py
python src/merging/merge_with_tootukassa.py

# 6. Export to CSV
python src/export/cvkeskus_json_to_csv.py
python src/export/tootukassa_json_to_csv.py
python src/export/merged_jobs_with_tootukassa_to_csv.py
```

All scraping steps support resuming after interruption - progress is saved automatically.
