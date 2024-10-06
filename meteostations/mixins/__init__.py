"""Mixins module."""

from meteostations.mixins.auth import APIKeyHeaderMixin, APIKeyParamMixin
from meteostations.mixins.stations import AllStationsEndpointMixin
from meteostations.mixins.variables import (
    VariablesEndpointMixin,
    VariablesHardcodedMixin,
)
