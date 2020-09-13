# simple_etl_to_local
Simple python ETL application from JSON to Postgresql database.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install dependency packages listed on requirements.txt file.

```bash
$ pip install requirements.txt
```
Obs: I strongly advise you to use virtualenv to set up your environment.
## Usage

```bash
$ python etl.py
```
Remember to set up all required dependencies like postgresql and PgAdmin in order to get the application running.

Inside the conf/ directory, there's a docker compose file putting a postgres docker image online that can help you to set it up.

Application's log will be written at /logs directory.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.


## License
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0)
