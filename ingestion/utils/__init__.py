import logging
import os
import re

from cognite.client import CogniteClient
from cognite.well_model import CogniteWellsClient
from cognite.well_model.wsfe import WellLogExtractorClient

log = logging.getLogger(__name__)


def clean_name(s):
    s = re.sub("[\r\n ]+", " ", s)
    s = re.sub("Wellbore|Well", "", s)
    s = re.sub("[- ]+$", "", s)
    return s


def clients():
    project = os.environ.get("COGNITE_PROJECT")
    if project is None:
        log.error("COGNITE_PROJECT environment variable is not set. Exiting...")
        exit(1)
    log.info(f"Cognite project is {project}.")

    # The EarlyRefreshTokenGenerator will automatically create a new token if
    # there is less than 15 minutes left on the token. Required for long-running
    # WSFE jobs. Reusing the same token generator for all three clients will
    # also reduce the number of times we have to reach out to microsoft to get a
    # fresh token.
    log.info("Creating clients")
    if os.environ.get("COGNITE_TOKEN"):
        token = os.environ.get("COGNITE_TOKEN")
    else:
        token = None
    client = CogniteClient(disable_pypi_version_check=True, token=token)
    wm = CogniteWellsClient(token=token)

    return client, wm


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
