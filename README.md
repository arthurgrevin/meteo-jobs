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


### Build container
docker build -t .

### Run container
docker run -it --env-file .env --network meteo_jobs_default meteo_jobs
