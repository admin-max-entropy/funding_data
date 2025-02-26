from datetime import datetime
from bs4 import BeautifulSoup

import src.data_utils

start_date = datetime(2008, 1, 1)
file_name = "../data/H8_data.xml"

def __get_annotation_info(series):
    result = {}
    annotations = series.find("frb:Annotations")
    if annotations:
        for annotation in annotations.find_all("common:Annotation"):
            annotation_type = annotation.find("common:AnnotationType").text
            annotation_text = annotation.find("common:AnnotationText").text
            result[annotation_type] = annotation_text
    return result

def __read_h8_data():
    # Open and read the XML file
    with open(file_name, "r", encoding="utf-8") as file:
        xml_content = file.read()

    # Parse the XML content using BeautifulSoup
    soup = BeautifulSoup(xml_content, "xml")

    # Find the Long Description
    series_all = soup.find_all("kf:Series")

    series_set = {}

    for series in series_all:

        print(series.attrs)
        h8_unit = series.attrs["H8_UNITS"]
        if h8_unit != "LEVEL":
            continue

        series_set[series.attrs["SERIES_NAME"]] = {}
        observations = series.find_all("frb:Obs")
        annotations= __get_annotation_info(series)

        print(annotations)
        time_series = {}

        for observation in observations:
            period = observation.attrs["TIME_PERIOD"]
            period = datetime.strptime(period, "%Y-%m-%d")
            if period < start_date:
                continue
            time_series[period] = float(observation.attrs["OBS_VALUE"])
        series_set[series.attrs["SERIES_NAME"]] = time_series

    return series_set

series_set = __read_h8_data()
src.data_utils.update_h8_data(series_set)