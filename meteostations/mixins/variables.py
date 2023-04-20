"""Variables mixins."""
import pandas as pd
from better_abc import abstract_attribute


class VariablesEndpointMixin:
    """Variables endpoint mixin."""

    @abstract_attribute
    def _variables_endpoint(self):
        pass

    @property
    def variables_df(self) -> pd.DataFrame:
        """Variables dataframe."""
        try:
            return self._variables_df
        except AttributeError:
            response_json = self._get_json_from_url(self._variables_endpoint)
            self._variables_df = self._variables_df_from_json(response_json)
            return self._variables_df
