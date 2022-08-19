import logging
import re
from typing import Any, Callable, Dict, List, Optional

from cognite.well_model import CogniteWellsClient
from cognite.well_model.models import (
    DepthIndexColumn,
    DepthMeasurementColumn,
    Distance,
    DistanceUnit,
    DistanceUnitEnum,
    MnemonicMatch,
    MnemonicMatchGroup,
)

log = logging.getLogger(__name__)

_unit_map_by_unit = {
    DistanceUnitEnum.meter: ["m", "meter", "meters"],
    DistanceUnitEnum.inch: ["in", "inch", "inches"],
    DistanceUnitEnum.foot: ["feet", "foot", "ft"],
}
_unit_map = {vv: k for k, v in _unit_map_by_unit.items() for vv in v}
# Matches '0.1 in' or 'meter' or 'feet' or '23.34 ft'
_unit_regex = re.compile(r"(?P<factor>[+-]?([0-9]*[.])?[0-9]+)?\s*(?P<unit>\w+)")


def parse_unit(unit: str) -> Optional[DistanceUnit]:
    if unit == "mm":
        return DistanceUnit(unit=DistanceUnitEnum.meter, factor=0.001)
    if unit == "m rkb":
        # m rkb is meters based on rotary kelly bushin
        return DistanceUnit(unit=DistanceUnitEnum.meter)
    match = _unit_regex.fullmatch(unit)
    if match:
        unit_match = match.group("unit")
        factor_match = match.group("factor")
        if factor_match:
            factor = float(factor_match)
        else:
            factor = 1.0

        u = _unit_map.get(unit_match)
        if u is not None:
            return DistanceUnit(unit=u, factor=factor)
    return None


def parse_distance(value: Optional[str], unit: Optional[str]) -> Optional[Distance]:
    if value is None or unit is None:
        return None

    try:
        float_value = float(value)  # type: ignore
    except ValueError:
        return None

    parsed_unit = parse_unit(unit)
    if parsed_unit is None:
        return None

    if parsed_unit.factor is not None and parsed_unit.factor != 1.0:
        log.warning(f"Unit {unit} has a factor != 1.0, which isn't handled yet.")
        return None

    return Distance(value=float_value, unit=parsed_unit.unit)


def _parse(column):
    unit = column.get("metadata", {}).get("unit", "").lower()
    external_id = column.get("externalId", "").lower()
    desc = column.get("description", "").lower()
    return (external_id, desc, unit)


def measured_depth(column) -> Optional[DepthIndexColumn]:
    external_id, desc, unit = _parse(column)
    external_id = column.get("externalId", "")
    if external_id.lower() in ["md", "tdep", "depth", "dept"]:
        parsed_unit = parse_unit(unit)
        if parsed_unit is None:
            log.error(
                f"Failed to parse unit '{unit}' for column '{external_id}' with desc '{desc}'"
            )
            return None
        return DepthIndexColumn(
            column_external_id=external_id, unit=parsed_unit, type="measured depth"
        )
    return None


FindBestMatchCallable = Callable[
    [
        Dict[str, Any],
        List[MnemonicMatch],
    ],
    Optional[MnemonicMatch],
]


def _find_first_match(
    column: Dict[str, Any], matches: List[MnemonicMatch]
) -> Optional[MnemonicMatch]:
    return next((x for x in matches), None)


def measurement_columns(
    wells_client: CogniteWellsClient,
    columns: List[Dict[str, Any]],
    find_best_match: FindBestMatchCallable = _find_first_match,
) -> List[DepthMeasurementColumn]:
    """Uses the WDL mnemonics search to find measurement types

    Args:
        wells_client (CogniteWellsClient)
        columns (List[Dict[str, Any]]) list of sequence columns
        find_best_match (FindBestMatchCallable, optional): callback to select
            the best match when multiple matches are found

    Returns:
        Dict[str, Optional[DepthMeasurementColumn]]:

    """
    column_external_ids = [x["externalId"] for x in columns]
    response = wells_client.mnemonics.search(column_external_ids)
    output: List[DepthMeasurementColumn] = []
    for col, matches in zip(columns, response):
        assert isinstance(matches, MnemonicMatchGroup)
        match = find_best_match(col, matches.matches)
        if match is None:
            continue
        unit = col["metadata"].get("unit", "")
        measurement_column = DepthMeasurementColumn(
            measurement_type=match.measurement_type,
            unit=unit,
            column_external_id=matches.mnemonic,
        )
        output.append(measurement_column)
    return output
