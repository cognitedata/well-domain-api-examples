import logging
import math
from datetime import datetime
from typing import Dict, List

import coloredlogs
from cognite.client.data_classes import Asset, Sequence, SequenceData, SequenceList
from cognite.well_model.models import (
    DistanceUnitEnum,
    Lithostratigraphic,
    LithostratigraphicLevelEnum,
    SequenceSource,
    WellTopsIngestion,
    WellTopSurfaceIngestion,
)

from utils import clients, chunks
from utils.log_progress import log_progress

coloredlogs.install()
log = logging.getLogger(__name__)


def get_litho_unit(formation_name: str):
    lower = formation_name.lower()
    if "formation" in lower:
        return Lithostratigraphic(level=LithostratigraphicLevelEnum.formation)
    elif "group" in lower:
        return Lithostratigraphic(level=LithostratigraphicLevelEnum.group)
    elif "member" in lower:
        return Lithostratigraphic(level=LithostratigraphicLevelEnum.member)
    return None


def create_well_tops(
    sequence,
    data: SequenceData,
    assets_dict: Dict[int, Asset],
):
    records = data.to_pandas().to_dict("records")
    formations = []
    for wt in records:
        name = wt["STRAT_UNIT_NM"]
        base_measured_depth = wt["BASE_MD"]
        if math.isnan(base_measured_depth):
            base_measured_depth = None
        formation = WellTopSurfaceIngestion(
            name=name,
            top_measured_depth=wt["TOP_MD"],
            base_measured_depth=base_measured_depth,
            lithostratigraphic=get_litho_unit(name),
        )

        if math.isnan(formation.top_measured_depth):
            log.warning(
                f"top '{name}' in sequence '{sequence.external_id}' has "
                + "top_measured_depth set to NaN and is therefore ignored."
            )
            continue
        formations.append(formation)
    if len(formations) > 0:
        return WellTopsIngestion(
            wellbore_asset_external_id=assets_dict[sequence.asset_id].external_id,
            source=SequenceSource(
                sequence_external_id=sequence.external_id,
                source_name="OSDU",
            ),
            measured_depth_unit=DistanceUnitEnum.meter,
            tops=formations,
        )
    return None


def main():
    client, wdl = clients()

    log.info("Retrieving CDF sequences")
    sequences = client.sequences.list(limit=None)
    well_tops: List[Sequence] = SequenceList(
        [x for x in sequences if x.description == "Wellbore Marker"]
    )
    log.info(f"Found {len(sequences)} sequences and identified {len(well_tops)} well tops")
    print(well_tops.to_pandas())

    log.info("Retrieving CDF assets to be able to connect sequence.asset_id to asset.external_id")
    asset_ids = list({x.asset_id for x in well_tops})
    assets = client.assets.retrieve_multiple(ids=asset_ids)
    assets_dict = {x.id: x for x in assets}
    log.info(f"Found {len(assets)} assets")

    log.info("Retrieving well tops from WDL to prevent redundant work.")
    already_ingested = {x.source.sequence_external_id for x in wdl.well_tops.list(limit=None)}
    log.info(f"Found {len(already_ingested)} well tops already in WDL.")
    well_tops = [x for x in well_tops if x.external_id not in already_ingested]

    start_time = datetime.now()
    i = 0
    for wt_chunk in chunks(well_tops, 100):
        ingestions = []
        for wt in wt_chunk:
            i += 1
        progress_str = log_progress(start_time, i, len(well_tops))
        log.info(f"{progress_str} Creating well_tops ingestion for {wt.external_id}")
        data = client.sequences.data.retrieve(id=wt.id, start=0, end=None)
        ingestion = create_well_tops(wt, data, assets_dict)
        if ingestion is not None:
                ingestions.append(ingestion)
            try:
            wdl.well_tops.ingest(ingestions)
            except Exception as e:
                log.error("    Failed to ingest WDL depth measurements", exc_info=e)
                break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        exit(0)
