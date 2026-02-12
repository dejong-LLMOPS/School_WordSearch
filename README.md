# School Policy Web Scraper and Analysis System

A comprehensive system for scraping school and district websites, searching for policy-related terms ("restorative justice" and "race equity"), using Perplexity AI to contextualize findings, and generating detailed CSV reports with interactive map visualizations.

## Features

- **Web Scraping**: Automatically scrapes school and district websites with multi-level link following
- **Term Search**: Searches for policy-related terms with context extraction
- **AI Contextualization**: Uses Perplexity AI to provide contextual summaries of found terms
- **CSV Reports**: Generates detailed CSV files with all findings
- **Interactive Maps**: Dash/Plotly-based interactive visualizations showing results by state and district
- **Progress Tracking**: Resume capability to continue from where you left off
- **Rate Limiting**: Respectful scraping with configurable delays

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
   - Copy `.env.example` to `.env` (if it doesn't exist, create it)
   - Add your Perplexity API key:
   ```
   PERPLEXITY_API_KEY=your_api_key_here
   ```

## Usage

### Basic Usage

Process all North Carolina schools:
```bash
python main.py --state NC
```

### Testing with a Small Sample

Test with just 10 schools:
```bash
python main.py --state NC --max-schools 10
```

### Custom Search Terms

Search for different terms:
```bash
python main.py --state NC --terms "restorative justice" "equity" "diversity"
```

### Resume from Previous Run

By default, the system resumes from previous progress. To start fresh:
```bash
python main.py --state NC --no-resume
```

### View Interactive Map

After processing schools, launch the interactive map:
```bash
python map_app.py
```

Then open your browser to `http://127.0.0.1:8050`

## Project Structure

```
School_WordSearch/
├── config.py              # Configuration settings
├── main.py                # Main orchestrator
├── data_extractor.py      # Excel file reading and filtering
├── web_scraper.py         # Website scraping
├── term_searcher.py       # Term search and context extraction
├── ai_context.py          # Perplexity AI integration
├── csv_generator.py       # CSV report generation
├── map_app.py             # Interactive map visualization
├── utils/
│   ├── __init__.py
│   └── logging_config.py  # Logging setup
├── output/
│   ├── results.csv        # Generated CSV results
│   ├── progress.json      # Progress checkpoint
│   └── cache/             # Cached scraped pages
└── requirements.txt       # Python dependencies
```

## Configuration

Edit `config.py` to customize:
- Search terms
- Scraping depth and rate limits
- Timeout values
- Output paths

## Output

### CSV File (`output/results.csv`)

Contains columns:
- School Name
- District Name
- State
- School URL
- District URL
- Terms Found
- Page URLs Where Terms Found
- Context Snippets
- AI Summary
- Total Occurrences
- Pages With Terms
- Timestamp
- Scrape Status

### Interactive Map

The map visualization provides:
- State-level overview with percentage of schools with terms
- District-level drill-down
- School-level details with links
- Filtering by search term

## Notes

- The system respects rate limits with configurable delays between requests
- Scraped pages are cached to avoid re-scraping
- Progress is automatically saved and can be resumed
- The Perplexity API integration is flexible and will work with the standard API format

## Troubleshooting

### Perplexity API Issues

If AI contextualization isn't working:
1. Check that your API key is set in `.env`
2. Verify the API key is valid
3. Check API rate limits

### Scraping Errors

If many schools fail to scrape:
- Some websites may block automated access
- Check network connectivity
- Increase timeout values in `config.py`

### Memory Issues

For large datasets:
- Process in smaller batches using `--max-schools`
- Clear cache periodically from `output/cache/`

## License

This project is for internal use by State Policy Network.

