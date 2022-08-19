# Ingestion into the Well Domain API

This repository is a working example of how to ingest data into the well domain
api (also called the well data layer).

In this example, we are ingesting well data from OSDU into the well domain api.

## Usage

```shell
# Install requirements using requirements.txt
pip install -r requirements.txt

# Set environment variables
export COGNITE_BASE_URL="https://api.cognitedata.com"
export COGNITE_PROJECT="my-cognite-project"
export COGNITE_CLIENT_ID="my client id"
export COGNITE_CLIENT_SECRET="my client secret"
export COGNITE_TENANT_ID='tenant id'
export COGNITE_TOKEN_SCOPES="${COGNITE_BASE_URL}/.default"
export COGNITE_TOKEN_URL="https://login.microsoftonline.com/${COGNITE_TENANT_ID}/oauth2/v2.0/token"

# Run the ingestion scripts
python 01-wells-and-wellbores.py
python 02-trajectories.py
python 03-depth-measurements.py
python 04-well-tops.py
```
