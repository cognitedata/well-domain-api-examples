import logging
from datetime import datetime
from typing import List, Optional, Tuple

import coloredlogs
from cognite.client.data_classes import Sequence, SequenceData, SequenceList
from cognite.well_model.models import (
    AngleUnitEnum,
    DepthIndexColumn,
    DistanceUnit,
    SequenceSource,
    TrajectoryIngestion,
    TrajectoryIngestionRow,
)

from utils import chunks, clients
from utils.log_progress import log_progress

coloredlogs.install()
log = logging.getLogger(__name__)

client, wm = clients()


def create_trajectory_ingestion(
    wellbore_asset_external_id: str,
    sequence: Sequence,
    data: SequenceData,
    md_column: DepthIndexColumn,
    inclination_column: Tuple[str, AngleUnitEnum],
    azimuth_column: Tuple[str, AngleUnitEnum],
    source_name: str,
    is_definitive: bool = False,
) -> Optional[TrajectoryIngestion]:
    inc_extid, inc_unit = inclination_column
    azim_extid, azim_unit = azimuth_column
    traj = TrajectoryIngestion(
        wellbore_asset_external_id=wellbore_asset_external_id,
        source=SequenceSource(sequence_external_id=sequence.external_id, source_name=source_name),
        measured_depth_unit=md_column.unit.unit,
        inclination_unit=inc_unit,
        azimuth_unit=azim_unit,
        is_definitive=is_definitive,
        rows=[],
    )

    def find_index(extid):
        index = next(
            (i for i, col in enumerate(sequence.columns) if col.get("externalId") == extid),
            None,
        )
        if index is None:
            raise Exception(
                f"Couldn't find column with externalId '{extid}' in sequence {sequence.external_id}"
            )
        return index

    md_index = find_index(md_column.column_external_id)
    inc_index = find_index(inc_extid)
    azim_index = find_index(azim_extid)

    for row_number, values in data.items():
        md = values[md_index] * md_column.unit.factor if values[md_index] else None
        inc = values[inc_index]
        azim = values[azim_index] % 360.0 if values[azim_index] else None
        if md is None or inc is None or azim is None:
            log.warning(
                f"Ignoring row {row_number} since one of the values are None: "
                + f"md={md}, inclination={inc}, azimuth={azim}"
            )
            continue
        row = TrajectoryIngestionRow(measured_depth=md, inclination=inc, azimuth=azim)
        traj.rows.append(row)
    if not traj.rows:
        log.warning("Can't ingest trajectory since it has no rows.")
        return None

    return traj


def main():
    log.info("Retrieving sequences...")
    sequences = client.sequences.list(limit=None)
    trajectories: List[Sequence] = SequenceList(
        [x for x in sequences if x.description == "Wellbore Trajectory"]
    )
    log.info(f"Found {len(sequences)} sequences and identified {len(trajectories)} trajectories")
    print(trajectories.to_pandas())

    asset_ids = [x.asset_id for x in trajectories]
    assets = client.assets.retrieve_multiple(ids=asset_ids)
    assets_dict = {x.id: x for x in assets}

    # There are a lot of trajectories, so we are "chunking" up the list of sequences
    # into eatable chunks.
    i = 0
    start_time = datetime.now()
    for trajectory_chunk in chunks(trajectories, 10):
        trajectory_ingestions = []
        for trajectory in trajectory_chunk:
            i += 1

            log.info(
                log_progress(start_time, i, len(trajectories))
                + f" Downloading sequence data for {trajectory.external_id}"
            )
            wellbore = assets_dict[trajectory.asset_id]
            data = client.sequences.data.retrieve(
                external_id=trajectory.external_id, start=0, end=None
            )
            ti = create_trajectory_ingestion(
                wellbore_asset_external_id=wellbore.external_id,
                sequence=trajectory,
                data=data,
                md_column=DepthIndexColumn(
                    unit=DistanceUnit(unit="foot"),
                    column_external_id="MeasuredDepth",
                    type="measured depth",
                ),
                inclination_column=("Inclination", AngleUnitEnum.degree),
                azimuth_column=("Azimuth", AngleUnitEnum.degree),
                source_name="OSDU",
                is_definitive=True,
            )
            if ti is not None:
                trajectory_ingestions.append(ti)

        log.info(f"Created {len(trajectory_ingestions)} trajectory ingestions")
        log.info(f"Ingesting {len(trajectory_ingestions)} trajectories...")
        try:
            wm.trajectories.ingest(trajectory_ingestions)
        except Exception as e:
            log.error("Failed to ingest trajectories", exc_info=e)

    log.info("DONE")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        exit(0)
