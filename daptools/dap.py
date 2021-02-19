import os
import time
import urllib
import requests
import numpy as np
import pandas as pd
from astropy.time import Time
from rich.progress import track
from urllib3.util import Retry
from requests.adapters import HTTPAdapter


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
        return df

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


df_medusa = final_df.groupby("backend").get_group("Medusa")
df_medusa.reset_index(inplace=True, drop=True)
src_list = list(df_medusa.source.unique())
CAL_list = [src for src in src_list if src.split("_")[-1] == "R"]
FRB_list = [src for src in src_list if src.split("_")[-1] != "R" and src[0] != "J"]
PSR_list = [src for src in src_list if src.split("_")[-1] != "R" and src[0] == "J"]
gb = df_medusa.groupby("source")
df_frb = pd.concat([gb.get_group(x) for x in FRB_list])
df_frb.reset_index(inplace=True, drop=True)



def split_filepath(filepath, backend):
    basename, ext = os.path.splitext(filepath)
    if backend == "Medusa":
        if len(basename.split("_")) == 5:
            dformat, mjd1, mjd2, b_index, file_index = basename.split("_")
            obs_id = "_".join([mjd1, mjd2])
        elif len(basename.split("_")) == 4:
            dformat, mjd1, mjd2, file_index = basename.split("_")
            obs_id = "_".join([mjd1, mjd2])
        elif len(basename.split("_")) == 3:
            dformat, mjd1, mjd2 = basename.split("_")
            obs_id = "_".join([mjd1, mjd2])
            file_index = ""
    elif backend == "HIPSR_SRCH":
        if len(basename.split("_")) == 3:
            dformatmjd1, mjd2, file_index = basename.split("_")
            mjd1 = dformatmjd1.split("bpsr")[-1]
            obs_id = "_".join([mjd1, mjd2])
            file_index = file_index.split("beam")[-1]
    return [mjd1, obs_id, file_index]


dap_dp[["mjd1", "obs_id", "file_index"]] = pd.DataFrame(
    dap_dp.apply(lambda x: split_filepath(x.filename, x.backend), axis=1).tolist(),
    index=dap_dp.index,
)
dap_dp["start_MJD"] = dap_dp.sttImjd + dap_dp.sttOffs
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
    for colname in dap_dp.columns.to_list()
    if colname not in drop_columns
}
agg_arg.update({"file_index": "count", "length": "sum"})

group_dap_dp = dap_dp.groupby("obs_id").agg(agg_arg)

unique_columns = list(key for (key, value) in agg_arg.items() if value == "unique")
for colname in unique_columns:
    group_dap_dp[colname] = group_dap_dp[colname].map(lambda x: x[0])
group_dap_dp = group_dap_dp.reset_index()
group_dap_dp["length"] = group_dap_dp["length"] / 1000 / 3600.0


