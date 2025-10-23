# meteo-jobs


## Ideas and Roadmap

- Add Transform on Database(for now postgres) using dbt(?)
  It should properly perform data cleaning
- Add Visual
  to investigate
- Stations extract and load should be a end to end process
  meteos extract and load should be launch from another process
  investigate using Event based/Message Service
- implement connector to another database(BigQuery ?)
- continue to refactor using Typings(improve error handling)
- Improve tests and tests coverage
- Add proper logging
- investigate using gcp service and how to integrate them



### Install Python dependencies

In your workspace:

### Set up virtual environment
```
python3.12 -m venv venv
source venv/bin/activate
```
#### Install dependencies using pip
```
pip install -r requirements.txt
```
### Run Meteo EL
```
python3.12 start_EL_meteo.py --station {station_name}
```
### Run Station EL
```
python3.12 start_EL_station.py
```
### Build container
```
docker build -t .
```
### Run container
```
docker run -it --env-file .env --network meteo_jobs_default meteo_jobs
```
