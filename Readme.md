# Modern ELT pipeline with Google Sheets, Google API, Prefect and dlt

## Description

This ELT pipeline synchronizes data from a given Google Sheets to a Postgres database using Google API on a defined schedule. The main idea of the project is to leverage the features of Python's Prefect orchestration tool and explore the possibilities of cloud infrastructure provided by Google to facilitate data management. It is designed with high data quality and flexibility in mind. The pipeline uses dlt (data load tool) for the actual Extract-Load (EL) steps and runs two independent Prefect flows in separate docker containers mimmicking a distributed system. One flow tracks Google Sheet modifications and keeps track of the required metadata. The second loads data to POstgres only when there has been a modification. A Redis queue is implemented as message broker to pass messages from one flow to the other.
This is a proof of concept pipeline which scales nicely due to its distributed nature. However in a real-world scenario, Prefect deployments in a dynamic infrastructure such as docker containers should take place in worker machines or in a kubernetes cluster. Since the pipeline is packed into a docker-compose.yml file it is not possible to deploy worker machines inside containers themsevles. But it would require minimal work to configure the pipeline for that scenario.

```mermaid
graph TD
    subgraph "Source"
        A[Google Sheets]
    end

    subgraph "Google APIs"
        B[Google Drive API]
        C[Google Sheets API]
    end

    subgraph "Docker Compose"
        G[Packages all components]
    subgraph "Containers"
        F1[Postgres Database]
        F2[Redis Message Broker]
    end
    subgraph "Prefect containers"
        D[Prefect server node]
        E1[File Tracking deployment]
        E2[File Loading deployment]
    end
    
    end



    A -->|Provides data & metadata| B
    A -->|Provides data & metadata| C

    B -->|Requests metadata| E1
    E1 -->|Stores metadata| F1
    E1 -->|Pushes file ids| F2

    F2 -->|Sends new file notifications| E2
    E2 -->|Requests Google Sheets data| C
    E2 -->|Loads sheet data| F1

    D -->|Deploys & manages| E1
    D -->|Deploys & manages| E2

    F1 -->|Stores all data and metadata| D
```

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

### Set up Google account

Before using the Google API or Google sheets it is necessary to have a google email and set up a Google cloud project which is for free. Then you create a service account to read files associated to your own email account.

* Create a new Google cloud project
* Create a service account and get the credentials in json format
* Create or upload the desired data to a Google sheet
* On the Google sheets select the range of cells with data and name it 'data': https://support.google.com/docs/answer/63175?hl=en&co=GENIE.Platform%3DDesktop
* Take note of the Google sheet name and the range name. These are the only two input paramters required for the pipeline
* Enable Google Drive API 
* Enable Google Sheets API
* Add the desired scopes (google.drive.readonly, google.sheets.readonly, etc)
* Grant the service account google drive and google sheets permissions to read the desired files

### Environment variables

Create a .env files with the following variables

```
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_database
POSTGRES_HOST=prefect-postgres
REDIS_HOST=prefect-redis
PREFECT_API_DATABASE_CONNECTION_URL="postgresql+asyncpg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@prefect-postgres:5432/${POSTGRES_DB}"
```

### Dependencies

* Ubuntu server 22.04
* Python 3.10 or higher
* Docker and docker compose

### Executing the pipeline

By default the pipeline is not scheduled

* Launch the pipeline with Docker compose
```
docker-compose up
```
* Once is up and running visit Prefect's UI on localhost port 4200 and check Prefect is working
* Update the Google sheet and run again the same deployment
* Check Prefect UI flow runs to monitor the proces
* Now you can access Postgres database using a client to rad the data which is synchronized every 10 minutes interval

### Customization

* You can create more google sheets and their respective named ranges
* Modify schedule interval of file tracking and loading flows by changing the cron argument in the *.py files


## Authors

Dario Vazquez

## References

* Integrate Google Drive API with Python: https://medium.com/the-team-of-future-learning/integrating-google-drive-api-with-python-a-step-by-step-guide-7811fcd16c44
* Using Google Drive API in Python with a service account: https://medium.com/@matheodaly.md/using-google-drive-api-with-python-and-a-service-account-d6ae1f6456c2
* Official Google Drive API documentation: https://developers.google.com/drive/api/quickstart/python
* Official Google Sheets API documentation: https://developers.google.com/sheets/api/guides/values#python

