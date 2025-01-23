import itertools
import os

import numpy as np
import pandas as pd
import requests


def get_data_from_vannnett(wb_id, quality_element):
    """
    Fetches water quality data from the vann-nett service.

    Parameters
        wb_id: Str. The waterbody ID.
        quality_element: Str. The quality element to fetch. Must be one of ['ecological',
            'rbsp', 'swchemical'].

    Returns
        DataFrame of water quality data (if available) or None.
    """
    valid_elements = ["ecological", "rbsp", "swchemical"]
    if quality_element.lower() not in valid_elements:
        raise ValueError(
            "'quality_element' must be one of ['ecological', 'rbsp', 'swchemical']."
        )

    element_dict = {
        "ecological": "ecological",
        "rbsp": "RBSP",
        "swchemical": "swChemical",
    }
    quality_element = element_dict[quality_element.lower()]

    url = f"https://vann-nett.no/service/waterbodies/{wb_id}/qualityElements/{quality_element}"
    response = requests.get(url)
    if response.status_code != 200:
        response.raise_for_status()
    data = response.json()

    par_map = {
        "qualityElementType.parentId": "category",
        "qualityElementType.id": "element",
        "parameterType.text": "parameter",
        "status.text": "status",
        "eqr": "eqr",
        "neqr": "neqr",
        "value": "value",
        "threshold.refValue": "reference_value",
        "threshold.unit": "unit",
        "threshold.statusLimits": "status_limits",
        "yearFrom": "year_from",
        "yearTo": "year_to",
        "sampleCount": "sample_count",
        "otherSource": "source",
        "dataQuality.text": "data_quality",
    }
    par_cols = par_map.keys()
    df_list = []
    cat_data = pd.json_normalize(data)
    for cat_row in cat_data.itertuples():
        ele_data = pd.json_normalize(cat_row.qualityElements)
        for ele_row in ele_data.itertuples():
            par_df = pd.json_normalize(ele_row.parameters)
            if not par_df.empty:
                par_df = par_df[par_cols].rename(columns=par_map)
                par_df = par_df.dropna(axis="columns", how="all")
                df_list.append(par_df)

    if len(df_list) > 0:
        df = pd.concat(df_list, ignore_index=True)
        for col in par_map.values():
            if col not in df.columns:
                df[col] = np.nan
        df["waterbody_id"] = wb_id
        df = df[["waterbody_id"] + list(par_map.values())]
        return df
    else:
        return None


def get_wfd_class(boundary_str, value):
    """
    Determines the class name based on the given value and boundary string.

    Args
        boundary_str: Str. A semi-colon separated string defining class boundaries.
                      Example: "475.0;650.0;1075.0;1775.0"
        value: Float. The value to be classified.

    Returns
        Str. The class name corresponding to the given value. One of
        'High', 'Good', 'Moderate', 'Poor' or 'Bad'.
    """
    # Split the boundary string into a list of float values
    boundaries = list(map(float, boundary_str.split(";")))

    # Define the class names based on the boundaries
    class_names = ["High", "Good", "Moderate", "Poor", "Bad"]

    # Determine the class name based on the value
    if value < boundaries[0]:
        return class_names[0]
    elif boundaries[0] <= value < boundaries[1]:
        return class_names[1]
    elif boundaries[1] <= value < boundaries[2]:
        return class_names[2]
    elif boundaries[2] <= value < boundaries[3]:
        return class_names[3]
    else:
        return class_names[4]