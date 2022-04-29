import urllib
import numpy as np
import pandas as pd

from astropy.time import Time
from rich.progress import track
from urllib3.util import Retry
from requests.adapters import HTTPAdapter
from requests_futures.sessions import FuturesSession
from concurrent.futures import as_completed

from daptools.filenames import MedusaPath, HIPSRPath


class DAPQuery(object):
    def __init__(self, mjd_gap=100, timeout=10, max_workers=10):
        self.base_url = "https://data.csiro.au/dap/ws/v2/"
        self.atnf_endpoint = "domains/pulsarObservations/search"
        self.headers = {"Accept": "application/json"}
        self.mjd_gap = mjd_gap
        self.timeout = timeout
        self.max_workers = max_workers

        self._setup_session()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def query(self, projId, mjdMax=None, mjdMin=None):
        if mjdMax is None:
            mjdMax = Time.now().mjd
        if mjdMin is None:
            mjdMin = 57754

        mjd_list = np.arange(mjdMax, mjdMin, -self.mjd_gap)
        queryParams_list = [
            self._get_queryParams(projId, mjdMax=jmjdMax, mjdMin=jmjdMin)
            for jmjdMax, jmjdMin in zip(mjd_list, mjd_list[1:])
        ]
        json_res_list = self.request_multi(queryParams_list)

        df_list = []
        for jres in json_res_list:
            last = jres.get("last")
            if last is not None:
                raise ValueError(f"Reduce mjd_gap. last is not None: {last}")
            files_info = jres.get("files")
            if files_info:
                mjd_df = pd.DataFrame.from_dict(files_info)
                df_list.append(mjd_df)
        df = pd.concat(df_list)
        df.sort_values(by="filename", ascending=False, inplace=True)
        df.reset_index(inplace=True, drop=True)
        return QueryDF(df)

    def close(self):
        self.session.close()

    def request(self, queryParams):
        url = self.base_url + self.atnf_endpoint
        future_resp = self.session.get(
            url, headers=self.headers, params=queryParams, timeout=self.timeout
        )
        resp = future_resp.result()
        return resp.data

    def request_multi(self, queryParams_list):
        url = self.base_url + self.atnf_endpoint
        result_list = queryParams_list.copy()
        futures = []
        for ipage, queryParams in enumerate(queryParams_list):
            future = self.session.get(url, headers=self.headers, params=queryParams)
            future.ipage = ipage
            futures.append(future)

        for comp_future in track(
            as_completed(futures),
            description="[red]Querying DAP...",
            total=len(queryParams_list),
        ):
            resp = comp_future.result()
            result_list[comp_future.ipage] = resp.data
        return result_list

    def _setup_session(self):
        retry_strategy = Retry(
            total=5,
            status_forcelist=[104, 429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = FuturesSession(max_workers=self.max_workers)
        self.session.hooks["response"] = [check_for_errors, response_hook]
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get_queryParams(self, projId, **kwargs):
        queryParams = {
            "projId": projId,
            "mjdMin": "",
            "mjdMax": "",
            "observationMode": "All including calibration files",
        }
        queryParams.update(
            (key, value) for key, value in kwargs.items() if key in queryParams
        )
        queryParams.update(
            {
                "p": 1,
                "rpp": 999,
                "showFacets": True,
            }
        )
        return urllib.parse.urlencode(queryParams, quote_via=urllib.parse.quote)

    def _get_page(self, url):
        return int(urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["p"][0])


class QueryDF(object):
    def __init__(self, df):
        self.df = df


def check_for_errors(response, *args, **kwargs):
    response.raise_for_status()


def response_hook(response, *args, **kwargs):
    # parse the json storing the result on the response object
    response.data = response.json()


def split_filename(filename, backend):
    types = {"Medusa": MedusaPath, "HIPSR_SRCH": HIPSRPath}
    path = types[backend](filename)
    return pd.Series((path.mjd1, path.obs_id, path.file_index))


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

    df_source[["mjd1", "obs_id", "file_index"]] = df_source.filename.apply(split_filename, args=(backend,))
    df_source["start_MJD"] = (
        df_source.sttImjd + (df_source.sttSmjd + df_source.sttOffs) / 86400
    )

    drop_columns = [
        "dataObjectId",
        "presignedLink",
        "thumbnails",
        "mountStatus",
        "fileSize",
        "lastModified",
        "collection",
        "creationDate",
        "equinox",
        "frontend",
        "filepath",
        "hdrver",
        "beconfig",
        "bitpix",
        "nrcvr",
        "obsMode",
        "embargoed",
        "telescope",
        "coordMethod",
        "fdPoln",
        "obsType",
        "startTime",
        "sttImjd",
        "sttLst",
        "sttOffs",
        "sttSmjd",
    ]

    agg_arg = {
        colname: "unique"
        for colname in df_source.columns.to_list()
        if colname not in drop_columns
    }
    agg_arg.update({"file_index": "count", "length": "sum", "start_MJD": "min"})
    group_df_source = df_source.groupby("obs_id").agg(agg_arg)
    unique_columns = [key for (key, value) in agg_arg.items() if value == "unique"]

    for colname in unique_columns:
        group_df_source[colname] = group_df_source[colname].map(lambda row: row[0])
    group_df_source.reset_index(inplace=True, drop=True)
    group_df_source["length"] = group_df_source["length"] / 1000
    group_df_source["length_hr"] = group_df_source["length"] / 3600.0

    return group_df_source
