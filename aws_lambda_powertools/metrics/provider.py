import datetime
import json
import logging
import numbers
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

from .exceptions import (
    MetricResolutionError,
    MetricUnitError,
    MetricValueError,
    SchemaValidationError,
)
from .types import MetricNameUnitResolution, MetricResolution, MetricSummary, MetricUnit

logger = logging.getLogger(__name__)


# the template of metrics providers
class MetricsProvider(ABC):
    MAX_METRICS = 100
    MAX_DIMENSIONS = 29
    # use single metric when recording coldstart
    Enable_Single_Metrics = True
    # enable run validation before sending out metrics
    Validate_Metrics = False

    @abstractmethod
    def add_metric(self, metrics, name, unit, value, resolution):
        pass

    # validate the format of metrics
    @abstractmethod
    def validate(self, metrics: MetricSummary) -> bool:
        pass

    # serialize for flushing
    @abstractmethod
    def serialize(self, metrics: MetricSummary):
        pass

    # flush serialized data to output
    @abstractmethod
    def flush(self, metrics):
        pass


class EMFProvider(MetricsProvider):
    def __init__(self):
        self._metric_unit_valid_options = list(MetricUnit.__members__)
        self._metric_units = [unit.value for unit in MetricUnit]
        return super().__init__()

    # generic add metrics function
    def add_metric(self, metrics, name, unit, value, resolution):
        if not isinstance(value, numbers.Number):
            raise MetricValueError(f"{value} is not a valid number")

        unit = self._extract_metric_unit_value(unit=unit)
        resolution = self._extract_metric_resolution_value(resolution=resolution)
        metrics["Unit"] = unit
        metrics["StorageResolution"] = resolution
        metrics["Value"].append(float(value))

        return metrics

    # validate the format of metrics
    def validate(self, metrics: MetricSummary) -> bool:
        if not self.Validate_Metrics:
            return True

        if len(metrics["Metrics"]) == 0:
            raise SchemaValidationError("Must contain at least one metric.")

        if metrics["Namespace"] is None:
            raise SchemaValidationError("Must contain a metric namespace.")

        return True

    # serialize for flushing
    def serialize(self, metrics: MetricSummary):
        if not self.validate(metrics):
            raise SchemaValidationError(f"{metrics} is not a valid metric")
        logger.debug(
            {"details": "Serializing metrics", "metrics": metrics["Metrics"], "dimensions": metrics["Dimensions"]}
        )

        # For standard resolution metrics, don't add StorageResolution field to avoid unnecessary ingestion of data into cloudwatch # noqa E501
        # Example: [ { "Name": "metric_name", "Unit": "Count"} ] # noqa E800
        #
        # In case using high-resolution metrics, add StorageResolution field
        # Example: [ { "Name": "metric_name", "Unit": "Count", "StorageResolution": 1 } ] # noqa E800
        metric_definition: List[MetricNameUnitResolution] = []
        metric_names_and_values: Dict[str, float] = {}  # { "metric_name": 1.0 }

        for metric_name in metrics["Metrics"]:
            metric: dict = metrics["Metrics"][metric_name]
            metric_value: int = metric.get("Value", 0)
            metric_unit: str = metric.get("Unit", "")
            metric_resolution: int = metric.get("StorageResolution", 60)

            metric_definition_data: MetricNameUnitResolution = {"Name": metric_name, "Unit": metric_unit}

            # high-resolution metrics
            if metric_resolution == 1:
                metric_definition_data["StorageResolution"] = metric_resolution

            metric_definition.append(metric_definition_data)

            metric_names_and_values.update({metric_name: metric_value})

        return {
            "_aws": {
                "Timestamp": int(datetime.datetime.now().timestamp() * 1000),  # epoch
                "CloudWatchMetrics": [
                    {
                        "Namespace": metrics["Namespace"],  # "test_namespace"
                        "Dimensions": [list(metrics["Dimensions"].keys())],  # [ "service" ]
                        "Metrics": metric_definition,
                    }
                ],
            },
            **metrics["Dimensions"],  # "service": "test_service"
            **metrics["Metadata"],  # "username": "test"
            **metric_names_and_values,  # "single_metric": 1.0
        }

    # flush serialized data to output
    def flush(self, metrics):
        print(json.dumps(metrics, separators=(",", ":")))

    def add_dimension(self, name: str, value: str):
        # Cast value to str according to EMF spec
        # Majority of values are expected to be string already, so
        # checking before casting improves performance in most cases
        return name, value if isinstance(value, str) else str(value)

    def add_metadata(self, key: str, value: Any):
        # Cast key to str according to EMF spec
        # Majority of keys are expected to be string already, so
        # checking before casting improves performance in most cases
        return key if isinstance(key, str) else str(key), value

    def _extract_metric_resolution_value(self, resolution: Union[int, MetricResolution]) -> int:
        """Return metric value from metric unit whether that's str or MetricResolution enum

        Parameters
        ----------
        unit : Union[int, MetricResolution]
            Metric resolution

        Returns
        -------
        int
            Metric resolution value must be 1 or 60

        Raises
        ------
        MetricResolutionError
            When metric resolution is not supported by CloudWatch
        """
        _metric_resolutions = [resolution.value for resolution in MetricResolution]
        if isinstance(resolution, MetricResolution):
            return resolution.value

        if isinstance(resolution, int) and resolution in _metric_resolutions:
            return resolution

        raise MetricResolutionError(
            f"Invalid metric resolution '{resolution}', expected either option: {_metric_resolutions}"
            # noqa: E501
        )

    def _extract_metric_unit_value(self, unit: Union[str, MetricUnit]) -> str:
        """Return metric value from metric unit whether that's str or MetricUnit enum

        Parameters
        ----------
        unit : Union[str, MetricUnit]
            Metric unit

        Returns
        -------
        str
            Metric unit value (e.g. "Seconds", "Count/Second")

        Raises
        ------
        MetricUnitError
            When metric unit is not supported by CloudWatch
        """

        if isinstance(unit, str):
            if unit in self._metric_unit_valid_options:
                unit = MetricUnit[unit].value

            if unit not in self._metric_units:
                raise MetricUnitError(
                    f"Invalid metric unit '{unit}', expected either option: {self._metric_unit_valid_options}"
                )

        if isinstance(unit, MetricUnit):
            unit = unit.value

        return unit
