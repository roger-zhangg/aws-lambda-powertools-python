from __future__ import annotations

from typing import Any, Dict

from aws_lambda_powertools.metrics.base import MetricManager
from aws_lambda_powertools.metrics.provider.base import MetricsProvider


class Metrics(MetricManager):
    """
    The default Metric Provider is CloudWatchEMFProvider

    Metrics create an object with up to the limit of metrics accepted by that Metrics provider.

    Use Metrics when you need to create multiple metrics that have
    dimensions in common (for example, service_name="payment").

    Metrics are stored in memory up to the limit that the Metrics provider accepts and are shared between
    all its instances. This means it can be safely instantiated outside
    from a Lambda function or anywhere else.

    A decorator (log_metrics) is provided so that the metrics are published at the end of its execution.
    If the metrics limit of the Metrics provider is reached in a given function execution,
    these metrics are serialized and published before adding a given metric
    to avoid metric truncation.

    Example
    -------
    **Creates a few metrics and publish at the end of a function execution**

        from aws_lambda_powertools import Metrics

        metrics = Metrics(namespace="ServerlessAirline", service="payment")

        @metrics.log_metrics(capture_cold_start_metric=True)
        def lambda_handler():
            metrics.add_metric(name="BookingConfirmation", unit="Count", value=1)
            metrics.add_dimension(name="function_version", value="$LATEST")

            return True

    Environment variables
    ---------------------
    POWERTOOLS_METRICS_NAMESPACE : str
        metric namespace
    POWERTOOLS_SERVICE_NAME : str
        service name used for default dimension

    Parameters
    ----------
    service : str, optional
        service name to be used as metric dimension, by default "service_undefined"
    namespace : str, optional
        Namespace for metrics
    provider: MetricsProvider, optional
        Metric provider, by default "CloudWatchEMFProvider"

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

    # NOTE: We use class attrs to share metrics data across instances
    # this allows customers to initialize Metrics() throughout their code base (and middlewares)
    # and not get caught by accident with metrics data loss, or data deduplication
    # e.g., m1 and m2 add metric ProductCreated, however m1 has 'version' dimension  but m2 doesn't
    # Result: ProductCreated is created twice as we now have 2 different EMF blobs
    _metrics: Dict[str, Any] = {}
    _dimensions: Dict[str, str] = {}
    _metadata: Dict[str, Any] = {}
    _default_dimensions: Dict[str, Any] = {}

    def __init__(
        self, service: str | None = None, namespace: str | None = None, provider: MetricsProvider | None = None
    ):
        self.metric_set = self._metrics
        self.metadata_set = self._metadata
        self.default_dimensions = self._default_dimensions
        self.dimension_set = self._dimensions
        self.dimension_set.update(**self._default_dimensions)
        return super().__init__(
            namespace=namespace,
            service=service,
            metric_set=self.metric_set,
            dimension_set=self.dimension_set,
            metadata_set=self.metadata_set,
            provider=provider,
        )

    def set_default_dimensions(self, **dimensions) -> None:
        """Persist dimensions across Lambda invocations

        Parameters
        ----------
        dimensions : Dict[str, Any], optional
            metric dimensions as key=value

        Example
        -------
        **Sets some default dimensions that will always be present across metrics and invocations**

            from aws_lambda_powertools import Metrics

            metrics = Metrics(namespace="ServerlessAirline", service="payment")
            metrics.set_default_dimensions(environment="demo", another="one")

            @metrics.log_metrics()
            def lambda_handler():
                return True
        """
        for name, value in dimensions.items():
            self.add_dimension(name, value)

        self.default_dimensions.update(**dimensions)

    def clear_default_dimensions(self) -> None:
        self.default_dimensions.clear()

    def clear_metrics(self) -> None:
        super().clear_metrics()
        # re-add default dimensions
        self.set_default_dimensions(**self.default_dimensions)


class EphemeralMetrics(MetricManager):
    """Non-singleton version of Metrics to not persist metrics across instances

    NOTE: This is useful when you want to:

    - Create metrics for distinct namespaces
    - Create the same metrics with different dimensions more than once
    """

    def __init__(
        self, service: str | None = None, namespace: str | None = None, provider: MetricsProvider | None = None
    ):
        super().__init__(namespace=namespace, service=service, provider=provider)
