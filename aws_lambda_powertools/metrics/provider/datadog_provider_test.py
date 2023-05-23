from __future__ import annotations

import json
import logging
import numbers
import time
import warnings
from typing import Dict, List

from aws_lambda_powertools.metrics.exceptions import MetricValueError
from aws_lambda_powertools.metrics.provider import MetricsBase, MetricsProviderBase

logger = logging.getLogger(__name__)

# Check if using layer
try:
    from datadog import lambda_metric
except ImportError:
    lambda_metric = None


class DataDogProvider(MetricsProviderBase):
    """Class for datadog provider."""

    def __init__(self, namespace):
        self.metrics = []
        self.namespace = namespace
        return super().__init__()

    #  adding timestamp, tags. unit, resolution, name will not be used
    def add_metric(self, name, value, timestamp, tag: List = None):
        if not isinstance(value, numbers.Real):
            raise MetricValueError(f"{value} is not a valid number")
        if not timestamp:
            timestamp = time.time()
        self.metrics.append({"m": name, "v": float(value), "e": timestamp, "t": []})

    # serialize for flushing
    def serialize(self) -> Dict:
        # logic here is to add dimension and metadata to each metric's tag with "key:value" format
        extra_tags = []
        output_list = []

        for single_metric in self.metrics:
            output_list.append(
                {
                    "m": f"{self.namespace}.{single_metric['m']}",
                    "v": single_metric["v"],
                    "e": single_metric["e"],
                    "t": single_metric["t"] + extra_tags,
                }
            )

        return {"List": output_list}

    # flush serialized data to output
    def flush(self, metrics):
        # submit through datadog extension
        if lambda_metric:
            for metric_item in metrics.get("List"):
                lambda_metric(
                    metric_name=metric_item["m"],
                    value=metric_item["v"],
                    timestamp=metric_item["e"],
                    tags=metric_item["t"],
                )
        # flush to log with datadog format
        # https://github.com/DataDog/datadog-lambda-python/blob/main/datadog_lambda/metric.py#L77
        else:
            for metric_item in metrics.get("List"):
                print(json.dumps(metric_item, separators=(",", ":")))

    def clear(self):
        self.metrics = []


class DataDogMetrics(MetricsBase):
    """Class for datadog metrics."""

    def __init__(self, provider):
        self.provider = provider
        return super().__init__()

    def add_metric(self, name: str, value: float, timestamp: time, tags: List = None):
        self.provider.add_metric(name, value, timestamp, tags)

    def flush_metrics(self, raise_on_empty_metrics: bool = False) -> None:
        metrics = self.provider.serialize()
        if not metrics and raise_on_empty_metrics:
            warnings.warn(
                "No application metrics to publish. The cold-start metric may be published if enabled. "
                "If application metrics should never be empty, consider using 'raise_on_empty_metrics'",
                stacklevel=2,
            )
        self.provider.flush(metrics)
        self.provider.clear()
