# 📝 Logs Directory

This directory contains execution logs and debugging information.

## Purpose

Logs are automatically generated during:
- Pipeline execution
- Data preprocessing
- Online simulation
- Evaluation runs

## Log Files

### Typical Structure

```
logs/
├── pipeline_YYYYMMDD_HHMMSS.log    # Main pipeline logs
├── enrichment_YYYYMMDD.log         # DBpedia/Wikidata queries
├── simulation_YYYYMMDD.log         # Online MAB simulation
└── errors.log                      # Error tracking
```

## Log Levels

- **DEBUG**: Detailed diagnostic information
- **INFO**: General informational messages
- **WARNING**: Warning messages (non-critical)
- **ERROR**: Error messages (critical issues)

## Usage

### Viewing Logs

```bash
# View latest pipeline log
tail -f logs/pipeline_*.log

# Search for errors
grep ERROR logs/*.log

# View specific date
cat logs/pipeline_20250121_*.log
```

### Log Rotation

Logs are automatically rotated:
- **Max size**: 10 MB per file
- **Retention**: 30 days
- **Compression**: Older logs are gzipped

## Configuration

Logging is configured in the pipeline scripts:

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
```

## Notes

- Logs are **not** version controlled (in `.gitignore`)
- Sensitive information is **redacted** automatically
- Logs are useful for **debugging** and **performance analysis**
