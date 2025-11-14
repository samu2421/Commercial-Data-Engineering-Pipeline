"""
Prefect flows for the restaurant analytics pipeline.
"""

from .ingest_data import ingest_data_flow
from .transform_data import transform_data_flow
from .analytics import analytics_flow

__all__ = [
    'ingest_data_flow',
    'transform_data_flow',
    'analytics_flow'
]

