import time

from aws_lambda_powertools.metrics.provider.datadog import DatadogMetrics, DatadogProvider
from aws_lambda_powertools.utilities.typing import LambdaContext

provider = DatadogProvider()
metrics = DatadogMetrics(provider=provider)


@metrics.log_metrics  # ensures metrics are flushed upon request completion/failure
def lambda_handler(event: dict, context: LambdaContext):
    metrics.add_metric(name="SuccessfulBooking", value=1, timestamp=int(time.time()))