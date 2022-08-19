import logging
from collections import defaultdict
from datetime import date, datetime
from typing import List, Optional, Set

from cognite.client.data_classes import Asset
from cognite.well_model import CogniteWellsClient
from cognite.well_model.models import (
    AssetSource,
    Distance,
    WellboreIngestion,
    Wellhead,
    WellIngestion,
)

from utils import chunks, clean_name
from utils.measurements import parse_distance, parse_unit

log = logging.getLogger(__name__)


def _water_depth(well_asset: Asset) -> Optional[Distance]:
    assert well_asset.metadata is not None
    water_depth = well_asset.metadata.get("WELLHEAD_DEPTH")
    try:
        water_depth_float = float(water_depth)  # type: ignore
    except (ValueError, TypeError):
        log.warning(f"Couldn't parse water depth {water_depth} as a floating point value.")
        return None

    unit = well_asset.metadata.get("WELLHEAD_DEPTH_DSDSUNIT")
    if unit is None:
        return None
    parsed_unit = parse_unit(unit)
    if parsed_unit is None:
        return None

    if parsed_unit.factor is not None and parsed_unit.factor != 1.0:
        log.warning(f"Unit {unit} has a factor != 1.0, which isn't handled yet.")
        return None

    return Distance(value=water_depth_float, unit=parsed_unit.unit)


def _spud_date(well_asset: Asset) -> Optional[date]:
    assert well_asset.metadata is not None
    spud_date = well_asset.metadata.get("SPUD_DATE")
    if spud_date is not None:
        try:
            return datetime.fromisoformat(spud_date)
        except ValueError as e:
            log.warning(f"Spud date {spud_date} is not a datetime", exc_info=e)
            return None
    return None


def ingest_wells_and_wellbores_edm(
    wm: CogniteWellsClient, well_assets, wellbore_assets, datums: List[Asset]
):
    well_ingestions = []
    wellbore_ingestions = []

    well_matching_ids: Set[str] = set()

    datum_dict = {x.external_id: x for x in datums}

    # Create a dictionary from well.external_id to a list of wellbores for performance.
    wellbores_by_parent_external_id = defaultdict(lambda: [])
    for wb in wellbore_assets:
        wellbores_by_parent_external_id[wb.parent_external_id].append(wb)

    for well_asset in well_assets:
        wellbores = wellbores_by_parent_external_id[well_asset.external_id]

        operator = well_asset.metadata.get("WELL_OPERATOR")

        longitude = float(well_asset.metadata["GEO_LONGITUDE"])
        latitude = float(well_asset.metadata["GEO_LATITUDE"])

        description = well_asset.metadata.get("WELL_DESC")

        well_name = clean_name(well_asset.name)
        wi = WellIngestion(
            name=well_name,
            description=description,
            matching_id=well_name,
            source=AssetSource(
                asset_external_id=well_asset.external_id,
                source_name="EDM",
            ),
            wellhead=Wellhead(x=longitude, y=latitude, crs="EPSG:4326"),
            # The `type` field is usually used to differentate production and exploratation wells.
            type=None,
            water_depth=_water_depth(well_asset),
            operator=operator,
            spud_date=_spud_date(well_asset),
        )
        if wi.matching_id not in well_matching_ids:
            well_ingestions.append(wi)
            well_matching_ids.add(wi.matching_id)  # type: ignore
        else:
            continue

        for wellbore in wellbores:
            datum = None
            datum_key = wellbore.metadata.get("DRILLING_DATUM_ID_EDM")
            if datum_key is not None:
                datum_asset = datum_dict.get(datum_key + "||EDMDF")
                if datum_asset is not None:
                    assert datum_asset.metadata is not None
                    datum = parse_distance(
                        datum_asset.metadata.get("DATUM_ELEVATION"),
                        datum_asset.metadata.get("DATUM_ELEVATION_DSDUNIT"),
                    )

            wellbore_name = clean_name(wellbore.name)
            wbi = WellboreIngestion(
                name=wellbore_name,
                description=wellbore_name,
                matching_id=clean_name(wellbore.external_id),
                well_asset_external_id=well_asset.external_id,
                source=AssetSource(
                    asset_external_id=wellbore.external_id,
                    source_name="EDM",
                ),
                datum=datum,
            )
            wellbore_ingestions.append(wbi)

    log.info(f"Ingesting {len(well_ingestions)} wells and {len(wellbore_ingestions)} wellbores.")
    for wi_chunk in chunks(well_ingestions, 1000):
        wm.wells.ingest(wi_chunk)

    log.info("Ingesting wellbores")
    for wb_chunk in chunks(wellbore_ingestions, 1000):
        wm.wellbores.ingest(wb_chunk)
