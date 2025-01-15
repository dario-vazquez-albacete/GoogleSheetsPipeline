# Modern ELT pipeline with Google Sheets, Google API, Prefect and dlt

## Description

This is a ELT pipeline which synchronizes data from Google Sheets to a Postgres database on a defined schedule. The pipeline runs two separate flows in separate docker containers mimmicking a distributed system.
* File tracking: Keeps track of file modifications
* File load: Performs the actual Extact-Load of Google Sheets to the Postgres database
Modified Google Sheets are sent to a Redis message queue and processed by the file load flow.

## Project structure

```
.
├── docker-compose.yml
├── Dockerfile
├── file-loading-flow.py
├── file-tracking-flow.py
├── Readme.md
├── requirements.txt
├── src
│   └── sources
│       ├── dlt_sources.py
│       ├── __init__.py
```

## Getting Started

### Dependencies

* Ubuntu server 22.04
* Python 3.10 or higher

### Installing

### Executing the pipeline

### Extending the pipeline


## Authors

Dario Vazquez

## References

