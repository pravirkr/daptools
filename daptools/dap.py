import time
import urllib
import requests
import numpy as np
import pandas as pd
from astropy.time import Time
from rich.progress import track
from urllib3.util import Retry
from requests.adapters import HTTPAdapter

from daptools.filenames import MedusaPath


class DAPQuery(object):
    def __init__(self, mjd_gap=100, timeout=10):
        self.base_url = "https://data.csiro.au/dap/ws/v2/"
        self.atnf_endpoint = "domains/pulsarObservations/search"
        self.headers = {"Accept": "application/json"}
        self.mjd_gap = mjd_gap
        self.timeout = timeout

        self._setup_session()

    def query(self, mjdMax=None, mjdMin=None, **kwargs):
        if mjdMax is None:
            mjdMax = Time.now().mjd
        if mjdMin is None:
            mjdMin = 57754

        mjd_list = np.arange(mjdMax, mjdMin, -self.mjd_gap)
        json_res_list = []
        for jmjdMax, jmjdMin in track(
            zip(mjd_list, mjd_list[1:]),
            description="[red]Querying DAP...",
            total=len(mjd_list) - 1,
        ):
            queryParams = self._get_queryParams(
                mjdMax=jmjdMax,
                mjdMin=jmjdMin,
                **kwargs,
            )
            json_res = self.request(
                endpoint=self.atnf_endpoint,
                queryParams=queryParams,
            )
            json_res_list.append(json_res)
            time.sleep(1)

        df = pd.concat(
            [pd.DataFrame.from_dict(jres.get("files")) for jres in json_res_list]
        )
        df.sort_values(by="filename", ascending=False, inplace=True)
        df.reset_index(inplace=True, drop=True)
        return QueryDF(df)

    def close(self):
        self.session.close()

    def request(self, endpoint, queryParams):
        url = self.base_url + endpoint
        resp = self.session.get(
            url, headers=self.headers, params=queryParams, timeout=self.timeout
        )
        return resp.json()

    @staticmethod
    def check_for_errors(response, *args, **kwargs):
        response.raise_for_status()

    def _setup_session(self):
        retry_strategy = Retry(
            total=5,
            status_forcelist=[104, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.hooks["response"] = [self.check_for_errors]
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get_queryParams(self, **kwargs):
        queryParams = {
            "pulsarName": "",
            "projId": "",
            "mjdMin": "",
            "mjdMax": "",
            "observationMode": "All including calibration files",
            "backend": "",
            "frontend": "",
        }
        queryParams.update(
            (key, value) for key, value in kwargs.items() if key in queryParams
        )
        queryParams.update(
            {
                "p": 1,
                "rpp": 9999,
                "showFacets": True,
            }
        )
        return urllib.parse.urlencode(queryParams, quote_via=urllib.parse.quote)

    def _get_page(self, url):
        return int(urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["p"][0])


class QueryDF(object):
    def __init__(self, df):
        self.df = df


def split_file(filename):
    path = MedusaPath(filename)
    return path.mjd1, path.obs_id, path.file_index


def group_df(df, backend="Medusa", source="FRB"):
    df_backend = df.groupby("backend").get_group(backend)
    df_backend.reset_index(inplace=True, drop=True)
    src_list = list(df_backend.source.unique())
    gb = df_backend.groupby("source")
    if source == "FRB":
        source_list = [
            src for src in src_list if src.split("_")[-1] != "R" and src[0] != "J"
        ]
    elif source == "PSR":
        source_list = [
            src for src in src_list if src.split("_")[-1] != "R" and src[0] == "J"
        ]
    elif source == "CAL":
        source_list = [src for src in src_list if src.split("_")[-1] == "R"]
    else:
        raise ValueError(f"source type {source} not supported")
    df_source = pd.concat([gb.get_group(src) for src in source_list])
    df_source.reset_index(inplace=True, drop=True)

    df_source[["mjd1", "obs_id", "file_index"]] = pd.DataFrame(
        df_source.apply(lambda row: split_file(row.filename), axis=1).tolist(),
        index=df_source.index,
    )
    df_source["start_MJD"] = df_source.sttImjd + df_source.sttOffs

    drop_columns = [
        "dataCollectionId",
        "fileSize",
        "lastModified",
        "collection",
        "creationDate",
        "equinox",
        "frontend",
        "hdrver",
        "nrcvr",
        "obsMode",
        "observer",
        "telescope",
        "fdPoln",
        "startTime",
        "sttImjd",
        "sttLst",
        "sttOffs",
        "sttSmjd",
        "obs_id",
    ]
    agg_arg = {
        colname: "unique"
        for colname in df_source.columns.to_list()
        if colname not in drop_columns
    }
    agg_arg.update({"file_index": "count", "length": "sum"})
    group_df_source = df_source.groupby("obs_id").agg(agg_arg)
    unique_columns = [key for (key, value) in agg_arg.items() if value == "unique"]

    for colname in unique_columns:
        group_df_source[colname] = group_df_source[colname].map(lambda row: row[0])
    group_df_source.reset_index(inplace=True, drop=True)
    group_df_source["length"] = group_df_source["length"] / 1000 / 3600.0

    return group_df_source
