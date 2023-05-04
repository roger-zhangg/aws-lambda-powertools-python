from abc import ABC, abstractmethod
from typing import Any, Dict, Tuple

from aws_lambda_powertools.metrics.types import MetricSummary


class MetricsProvider(ABC):
    """Class for metric provider template

    Use this template to create your own metric provider.

    """

    # MAX_METRICS controls how many metrics can be collected before auto-flushing
    MAX_METRICS = 100
    # MAX_DIMENSIONS controls how many dimension could be added
    MAX_DIMENSIONS = 29
    # use single metric when recording coldstart
    Enable_Single_Metrics = True
    # enable validation before sending out metrics. When enabled, validation will raise error if failed.
    Validate_Metrics = False

    # General add metric function. Should return combined metrics Dict
    @abstractmethod
    def add_metric(self, metrics, name, unit, value, resolution) -> Dict:
        pass

    # add logic for dimension name/value conversion and return (name,value)
    @abstractmethod
    def add_dimension(self, name: str, value: str) -> Tuple[str, str]:
        pass

    # add logic for metadata conversion and return (key,value)
    @abstractmethod
    def add_metadata(self, key: str, value: Any) -> Tuple[str, Any]:
        pass

    # validate the format of metrics
    @abstractmethod
    def validate(self, metrics: MetricSummary) -> bool:
        pass

    # serialize and return dict for flushing
    @abstractmethod
    def serialize(self, metrics: MetricSummary) -> Dict:
        pass

    # flush serialized data to output, or send to API directly
    @abstractmethod
    def flush(self, metrics):
        pass

    # clear_metric in provider when clear_metrics is called in metrics class
    @abstractmethod
    def clear_metrics(self):
        pass
