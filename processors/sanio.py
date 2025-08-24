from typing import Any

import pandas as pd


class BasicProcessor:
    def __init__():
        pass

    def process(self, input_data: Any) -> Any:
        # Implement your processing logic here
        df = pd.DataFrame(input_data.get("data"))
        # Perform operations on the DataFrame
        input_data.update({"data": df.to_json()})
        return input_data
