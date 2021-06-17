# digital_mkt_analytics

Simple python Analytics application running data ingestion and ETLs to deliver a analytical database.

the application intents to ingest multiple data that are available on the internet, clean it and load it to a local postgres database. It also creates one big table containing all related data.

The datasources are:
- google_ads_media_costs.jsonl
-- Google ads costs
- facebook_ads_media_costs.jsonl
-- Facebook ads costs
- pageviews.txt
-- Ads accessed by users
- customer_leads_funnel.csv 
-- User's lead Analytical table 

## Installation
Use the package manager pip to install dependency packages listed on requirements.txt file. Remember that it needs to access the **artifactory** repository.
### Setting up the python environment:

```bash

$ pip install requirements.txt

```
Obs: I strongly advise you to use virtualenv to set up your environment.

### Setting up Database with Docker:
In order to configure the Postgres and PgAdmin services, you'll need to install a docker client on your machine.
After that, all you'll need to do is to run the `docker-compose.yaml` file, available at `digital_mkt_analytics\resources\conf`.
```bash

$ cd digital_mkt_analytics\resources\conf
$ docker-compose up

```

## Usage
You can run the application from the project's root by running the following command:
```bash

$ python src\etl.py

```

Remember to set up all required dependencies like postgresql and PgAdmin in order to get the application running.

Application's log will be written at logs\ directory.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
