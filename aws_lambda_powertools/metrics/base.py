import functools
import json
import logging
import os
import warnings
from collections import defaultdict
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Optional, Union

from provider import EMFProvider, MetricsProvider

from ..shared import constants
from ..shared.functions import resolve_env_var_choice
from .exceptions import SchemaValidationError
from .types import MetricResolution, MetricSummary, MetricUnit

logger = logging.getLogger(__name__)


is_cold_start = True


class MetricManager:
    """Base class for metric functionality (namespace, metric, dimension, serialization)

    MetricManager creates metrics asynchronously thanks to CloudWatch Embedded Metric Format (EMF).
    CloudWatch EMF can create up to 100 metrics per EMF object
    and metrics, dimensions, and namespace created via MetricManager
    will adhere to the schema, will be serialized and validated against EMF Schema.

    **Use `aws_lambda_powertools.metrics.metrics.Metrics` or
    `aws_lambda_powertools.metrics.metric.single_metric` to create EMF metrics.**

    Environment variables
    ---------------------
    POWERTOOLS_METRICS_NAMESPACE : str
        metric namespace to be set for all metrics
    POWERTOOLS_SERVICE_NAME : str
        service name used for default dimension

    Raises
    ------
    MetricUnitError
        When metric unit isn't supported by CloudWatch
    MetricResolutionError
        When metric resolution isn't supported by CloudWatch
    MetricValueError
        When metric value isn't a number
    SchemaValidationError
        When metric object fails EMF schema validation
    """

    def __init__(
        self,
        metric_set: Optional[Dict[str, Any]] = None,
        dimension_set: Optional[Dict] = None,
        namespace: Optional[str] = None,
        metadata_set: Optional[Dict[str, Any]] = None,
        service: Optional[str] = None,
        provider: MetricsProvider = None,
    ):
        self.metric_set = metric_set if metric_set is not None else {}
        self.dimension_set = dimension_set if dimension_set is not None else {}
        self.namespace = resolve_env_var_choice(choice=namespace, env=os.getenv(constants.METRICS_NAMESPACE_ENV))
        self.service = resolve_env_var_choice(choice=service, env=os.getenv(constants.SERVICE_NAME_ENV))
        self.metadata_set = metadata_set if metadata_set is not None else {}
        self.provider = provider if metadata_set is not None else EMFProvider()
        self._metric_units = [unit.value for unit in MetricUnit]
        self._metric_unit_valid_options = list(MetricUnit.__members__)

    # TODO refactor with provider
    def add_metric(
        self,
        name: str,
        unit: Union[MetricUnit, str],
        value: float,
        resolution: Union[MetricResolution, int] = 60,
    ) -> None:
        """Adds given metric

        Example
        -------
        **Add given metric using MetricUnit enum**

            metric.add_metric(name="BookingConfirmation", unit=MetricUnit.Count, value=1)

        **Add given metric using plain string as value unit**

            metric.add_metric(name="BookingConfirmation", unit="Count", value=1)

        **Add given metric with MetricResolution non default value**

            metric.add_metric(name="BookingConfirmation", unit="Count", value=1, resolution=MetricResolution.High)

        Parameters
        ----------
        name : str
            Metric name
        unit : Union[MetricUnit, str]
            `aws_lambda_powertools.helper.models.MetricUnit`
        value : float
            Metric value
        resolution : Union[MetricResolution, int]
            `aws_lambda_powertools.helper.models.MetricResolution`

        Raises
        ------
        MetricUnitError
            When metric unit is not supported by CloudWatch
        MetricResolutionError
            When metric resolution is not supported by CloudWatch
        """

        # Change to provider
        metric: Dict = self.metric_set.get(name, defaultdict(list))
        metric = self.provider.add_metric(metric, name, unit, value, resolution)
        logger.debug(f"Adding metric: {name} with {metric}")
        self.metric_set[name] = metric

        if len(self.metric_set) == self.provider.MAX_METRICS or len(metric["Value"]) == self.provider.MAX_METRICS:
            logger.debug(f"Exceeded maximum of {self.provider.MAX_METRICS} metrics - Publishing existing metric set")
            metric_summary: MetricSummary = {
                "Namespace": self.namespace,
                "Metrics": self.metric_set,
                "Dimensions": self.dimension_set,
                "Metadata": self.metadata_set,
            }
            metrics = self.provider.serialize(metric_summary)
            self.provider.flush(metrics)

            # clear metric set only as opposed to metrics and dimensions set
            # since we could have more than 100 metrics
            self.metric_set.clear()

    # TODO refactor with provider
    def serialize_metric_set(
        self, metrics: Optional[Dict] = None, dimensions: Optional[Dict] = None, metadata: Optional[Dict] = None
    ):
        """Serializes metric and dimensions set

        Parameters
        ----------
        metrics : Dict, optional
            Dictionary of metrics to serialize, by default None
        dimensions : Dict, optional
            Dictionary of dimensions to serialize, by default None
        metadata: Dict, optional
            Dictionary of metadata to serialize, by default None

        Example
        -------
        **Serialize metrics into EMF format**

            metrics = MetricManager()
            # ...add metrics, dimensions, namespace
            ret = metrics.serialize_metric_set()

        Returns
        -------
        Dict
            Serialized metrics following EMF specification

        Raises
        ------
        SchemaValidationError
            Raised when serialization fail schema validation
        """
        if metrics is None:  # pragma: no cover
            metrics = self.metric_set

        if dimensions is None:  # pragma: no cover
            dimensions = self.dimension_set

        if metadata is None:  # pragma: no cover
            metadata = self.metadata_set

        if self.service and not self.dimension_set.get("service"):
            # self.service won't be a float
            self.add_dimension(name="service", value=self.service)

        metric_summary: MetricSummary = {
            "Namespace": self.namespace,
            "Metrics": metrics,
            "Dimensions": dimensions,
            "Metadata": metadata,
        }
        metrics = self.provider.serialize(metric_summary)

        return metrics

    # TODO keep for compatibility
    def add_dimension(self, name: str, value: str) -> None:
        """Adds given dimension to all metrics

        Example
        -------
        **Add a metric dimensions**

            metric.add_dimension(name="operation", value="confirm_booking")

        Parameters
        ----------
        name : str
            Dimension name
        value : str
            Dimension value
        """

        logger.debug(f"Adding dimension: {name}:{value}")
        if len(self.dimension_set) == self.provider.MAX_DIMENSIONS:
            raise SchemaValidationError(
                f"Maximum number of dimensions exceeded"
                f" ({self.provider.MAX_DIMENSIONS}): Unable to add dimension {name}."
            )

        name, value = self.provider.add_dimension(name, value)

        self.dimension_set[name] = value

    # TODO refactor with provider
    def add_metadata(self, key: str, value: Any) -> None:
        """Adds high cardinal metadata for metrics object

        This will not be available during metrics visualization.
        Instead, this will be searchable through logs.

        If you're looking to add metadata to filter metrics, then
        use add_dimensions method.

        Example
        -------
        **Add metrics metadata**

            metric.add_metadata(key="booking_id", value="booking_id")

        Parameters
        ----------
        key : str
            Metadata key
        value : any
            Metadata value
        """

        key, value = self.provider.add_metadata(key, value)
        logger.debug(f"Adding metadata: {key}:{value}")
        self.metadata_set[key] = value

    def clear_metrics(self) -> None:
        logger.debug("Clearing out existing metric set from memory")
        self.metric_set.clear()
        self.dimension_set.clear()
        self.metadata_set.clear()

    def log_metrics(
        self,
        lambda_handler: Union[Callable[[Dict, Any], Any], Optional[Callable[[Dict, Any, Optional[Dict]], Any]]] = None,
        capture_cold_start_metric: bool = False,
        raise_on_empty_metrics: bool = False,
        default_dimensions: Optional[Dict[str, str]] = None,
    ):
        """Decorator to serialize and publish metrics at the end of a function execution.

        Be aware that the log_metrics **does call* the decorated function (e.g. lambda_handler).

        Example
        -------
        **Lambda function using tracer and metrics decorators**

            from aws_lambda_powertools import Metrics, Tracer

            metrics = Metrics(service="payment")
            tracer = Tracer(service="payment")

            @tracer.capture_lambda_handler
            @metrics.log_metrics
            def handler(event, context):
                    ...

        Parameters
        ----------
        lambda_handler : Callable[[Any, Any], Any], optional
            lambda function handler, by default None
        capture_cold_start_metric : bool, optional
            captures cold start metric, by default False
        raise_on_empty_metrics : bool, optional
            raise exception if no metrics are emitted, by default False
        default_dimensions: Dict[str, str], optional
            metric dimensions as key=value that will always be present

        Raises
        ------
        e
            Propagate error received
        """

        # If handler is None we've been called with parameters
        # Return a partial function with args filled
        if lambda_handler is None:
            logger.debug("Decorator called with parameters")
            return functools.partial(
                self.log_metrics,
                capture_cold_start_metric=capture_cold_start_metric,
                raise_on_empty_metrics=raise_on_empty_metrics,
                default_dimensions=default_dimensions,
            )

        @functools.wraps(lambda_handler)
        def decorate(event, context):
            try:
                if default_dimensions:
                    self.set_default_dimensions(**default_dimensions)
                response = lambda_handler(event, context)
                if capture_cold_start_metric:
                    self._add_cold_start_metric(context=context)
            finally:
                if not raise_on_empty_metrics and not self.metric_set:
                    warnings.warn(
                        "No application metrics to publish. The cold-start metric may be published if enabled. "
                        "If application metrics should never be empty, consider using 'raise_on_empty_metrics'",
                        stacklevel=2,
                    )
                else:
                    metric_summary: MetricSummary = {
                        "Namespace": self.namespace,
                        "Metrics": self.metric_set,
                        "Dimensions": self.dimension_set,
                        "Metadata": self.metadata_set,
                    }
                    metrics = self.provider.serialize(metric_summary)
                    self.clear_metrics()
                    self.provider.flush(metrics)

            return response

        return decorate

    def _add_cold_start_metric(self, context: Any) -> None:
        """Add cold start metric and function_name dimension

        Parameters
        ----------
        context : Any
            Lambda context
        """
        global is_cold_start
        if not is_cold_start:
            return

        logger.debug("Adding cold start metric and function_name dimension")
        # TODO try to simplify single metrics
        if self.provider.Enable_Single_Metrics:
            metric_set: Optional[Dict] = None
            dimension_set: Optional[Dict] = None
            metric_result: Any = None
            name = "ColdStart"
            try:
                metrics = self.provider.add_metric(metrics={}, name=name, unit=MetricUnit.Count, value=1, resolution=60)
                metric_set[name] = metrics

                dim_name, dim_value = self.provider.add_dimension(name="function_name", value=context.function_name)
                dimension_set[dim_name] = dim_value
                if self.service:
                    dim_name, dim_value = self.provider.add_dimension(name="service", value=self.service)
                    dimension_set[dim_name] = dim_value

                metric_summary: MetricSummary = {
                    "Namespace": self.namespace,
                    "Metrics": metric_set,
                    "Dimensions": dimension_set,
                }
                metric_result = self.provider.serialize(metric_summary)
            finally:
                self.provider.flush(metric_result)
        # provider can choose not to treat coldstart as a single metrics
        else:
            self.add_metric(name="ColdStart", unit=MetricUnit.Count, value=1, resolution=60)

        is_cold_start = False


class SingleMetric(MetricManager):
    """SingleMetric creates an EMF object with a single metric.

    EMF specification doesn't allow metrics with different dimensions.
    SingleMetric overrides MetricManager's add_metric method to do just that.

    Use `single_metric` when you need to create metrics with different dimensions,
    otherwise `aws_lambda_powertools.metrics.metrics.Metrics` is
    a more cost effective option

    Environment variables
    ---------------------
    POWERTOOLS_METRICS_NAMESPACE : str
        metric namespace

    Example
    -------
    **Creates cold start metric with function_version as dimension**

        import json
        from aws_lambda_powertools.metrics import single_metric, MetricUnit, MetricResolution
        metric = single_metric(namespace="ServerlessAirline")

        metric.add_metric(name="ColdStart", unit=MetricUnit.Count, value=1, resolution=MetricResolution.Standard)
        metric.add_dimension(name="function_version", value=47)

        print(json.dumps(metric.serialize_metric_set(), indent=4))

    Parameters
    ----------
    MetricManager : MetricManager
        Inherits from `aws_lambda_powertools.metrics.base.MetricManager`
    """

    def add_metric(
        self,
        name: str,
        unit: Union[MetricUnit, str],
        value: float,
        resolution: Union[MetricResolution, int] = 60,
    ) -> None:
        """Method to prevent more than one metric being created

        Parameters
        ----------
        name : str
            Metric name (e.g. BookingConfirmation)
        unit : MetricUnit
            Metric unit (e.g. "Seconds", MetricUnit.Seconds)
        value : float
            Metric value
        resolution : MetricResolution
            Metric resolution (e.g. 60, MetricResolution.Standard)
        """
        if len(self.metric_set) > 0:
            logger.debug(f"Metric {name} already set, skipping...")
            return
        return super().add_metric(name, unit, value, resolution)


@contextmanager
def single_metric(
    name: str,
    unit: MetricUnit,
    value: float,
    resolution: Union[MetricResolution, int] = 60,
    namespace: Optional[str] = None,
    default_dimensions: Optional[Dict[str, str]] = None,
) -> Generator[SingleMetric, None, None]:
    """Context manager to simplify creation of a single metric

    Example
    -------
    **Creates cold start metric with function_version as dimension**

        from aws_lambda_powertools import single_metric
        from aws_lambda_powertools.metrics import MetricUnit
        from aws_lambda_powertools.metrics import MetricResolution

        with single_metric(name="ColdStart", unit=MetricUnit.Count, value=1, resolution=MetricResolution.Standard, namespace="ServerlessAirline") as metric: # noqa E501
            metric.add_dimension(name="function_version", value="47")

    **Same as above but set namespace using environment variable**

        $ export POWERTOOLS_METRICS_NAMESPACE="ServerlessAirline"

        from aws_lambda_powertools import single_metric
        from aws_lambda_powertools.metrics import MetricUnit
        from aws_lambda_powertools.metrics import MetricResolution

        with single_metric(name="ColdStart", unit=MetricUnit.Count, value=1, resolution=MetricResolution.Standard) as metric: # noqa E501
            metric.add_dimension(name="function_version", value="47")

    Parameters
    ----------
    name : str
        Metric name
    unit : MetricUnit
        `aws_lambda_powertools.helper.models.MetricUnit`
    resolution : MetricResolution
        `aws_lambda_powertools.helper.models.MetricResolution`
    value : float
        Metric value
    namespace: str
        Namespace for metrics

    Yields
    -------
    SingleMetric
        SingleMetric class instance

    Raises
    ------
    MetricUnitError
        When metric metric isn't supported by CloudWatch
    MetricResolutionError
        When metric resolution isn't supported by CloudWatch
    MetricValueError
        When metric value isn't a number
    SchemaValidationError
        When metric object fails EMF schema validation
    """
    metric_set: Optional[Dict] = None
    try:
        metric: SingleMetric = SingleMetric(namespace=namespace)
        metric.add_metric(name=name, unit=unit, value=value, resolution=resolution)

        if default_dimensions:
            for dim_name, dim_value in default_dimensions.items():
                metric.add_dimension(name=dim_name, value=dim_value)

        yield metric
        metric_set = metric.serialize_metric_set()
    finally:
        print(json.dumps(metric_set, separators=(",", ":")))


def reset_cold_start_flag():
    global is_cold_start
    if not is_cold_start:
        is_cold_start = True
