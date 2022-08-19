import logging
from collections import defaultdict

import coloredlogs
from cognite.well_model.models import (
    AssetSource,
    Datum,
    Distance,
    DistanceUnitEnum,
    Source,
    WellboreIngestion,
    Wellhead,
    WellIngestion,
)

from edm_wells import ingest_wells_and_wellbores_edm
from utils import chunks, clean_name, clients

coloredlogs.install(milliseconds=True)
log = logging.getLogger(__name__)

client, wm = clients()


def setup_sources():
    osdu = Source(name="OSDU", description="The Open Subsurface Data Universe (OSDU) Data Platform")
    edm = Source(name="EDM", description="Engineering Data Management Services")
    diskos = Source(name="DISKOS", description="Diskos National Data Repository")
    wm.sources.ingest([osdu, edm, diskos])

    log.info(f"Sources: \n{wm.sources.list().to_pandas()}")

    priority = ["EDM", "OSDU", "DISKOS"]
    wm.wells.merge_rules.set(priority)
    wm.wellbores.merge_rules.set(priority)


def get_water_depth(well_asset):
    # Don't know how to do this currently. Wells in
    # OSDU_test/osdu_wks_master_data_Well_1_1 have a `VerticalMeasurements`
    # object that contains elevation. But I can't make sense of it. The value is
    # always in meters, but the value varies from over 200 to -5. So it's
    # unclear what it's actually measuring.
    return None
    return (Distance(value=float(well_asset.metadata["Waterdepthm"]), unit="meter"),)


def ingest_wells_and_wellbores_osdu(well_assets, wellbore_assets, source):
    well_ingestions = []
    wellbore_ingestions = []

    # Create a dictionary from well.external_id to a list of wellbores for performance.
    wellbores_by_parent_external_id = defaultdict(lambda: [])
    for wb in wellbore_assets:
        wellbores_by_parent_external_id[wb.parent_external_id].append(wb)

    for well_asset in well_assets:
        wellbores = wellbores_by_parent_external_id[well_asset.external_id]

        first_wellbore = wellbores[0] if wellbores else None
        operator = None
        if first_wellbore is not None:
            operator = first_wellbore.metadata.get("CurrentOperator")

        longitude = float(well_asset.metadata["Wgs84SpatialLocationX"])
        latitude = float(well_asset.metadata["Wgs84SpatialLocationY"])

        well_name = clean_name(well_asset.name)
        wi = WellIngestion(
            name=well_name,
            description=well_name,
            matching_id=well_name,
            source=AssetSource(
                asset_external_id=well_asset.external_id,
                source_name="OSDU",
            ),
            wellhead=Wellhead(x=longitude, y=latitude, crs="EPSG:4326"),
            # The `type` field is usually used to differentate production and exploratation wells.
            type=None,
            water_depth=get_water_depth(well_asset),
            operator=operator,
        )
        well_ingestions.append(wi)

        for wellbore in wellbores:
            datum_elevation = wellbore.metadata.get("VerticalMeasurement_Measured_From")
            datum_reference = wellbore.metadata.get("VerticalMeasurementType_Measured_From")
            if datum_elevation is not None and datum_reference is not None:
                datum = Datum(
                    value=float(datum_elevation),
                    unit=DistanceUnitEnum.meter,
                    reference=datum_reference,
                )

            wellbore_name = clean_name(wellbore.name)
            wbi = WellboreIngestion(
                name=wellbore_name,
                description=wellbore_name,
                matching_id=clean_name(wellbore.external_id),
                well_asset_external_id=well_asset.external_id,
                source=AssetSource(
                    asset_external_id=wellbore.external_id,
                    source_name="OSDU",
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


def main():
    log.info("Installing sources..")
    setup_sources()

    log.info("Retrieving well and wellbore assets from OSDU...")
    wells = client.assets.list(metadata={"FacilityType": "Well"}, limit=None)
    wellbores = client.assets.list(metadata={"FacilityTypeID": "Wellbore"}, limit=None)
    log.info("Ingesting wells and wellbores into WDL")
    ingest_wells_and_wellbores_osdu(wells, wellbores, "osdu")

    log.info("Retrieving well and wellbore assets from EDM...")
    wells = client.assets.list(metadata={"type": "Well"}, source="EDM", limit=None)
    wellbores = client.assets.list(metadata={"type": "Wellbore"}, source="EDM", limit=None)
    datums = client.assets.list(metadata={"type": "Datum"}, source="EDM", limit=None)
    log.info(
        f"Ingesting {len(wells)} wells, "
        + f"{len(wellbores)} wellbores, "
        + f"and {len(datums)} datums from EDM into WDL"
    )
    ingest_wells_and_wellbores_edm(wm, wells, wellbores, datums)

    log.info("Retrieving wells from WDL")
    wdl_wells = wm.wells.list(limit=None)
    print(wdl_wells.to_pandas())
    print(wdl_wells.wellbores().to_pandas())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("Processing failed", exc_info=e)
        exit(1)
