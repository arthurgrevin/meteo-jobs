# meteo-jobs


### Install Python dependencies

In your workspace:

#### Set up virtual environment
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
