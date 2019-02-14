import os
import logging
import threading

import pandas as pd

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from yodapy.datasources.datasource import DataSource


logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

logger = logging.getLogger(__name__)

CRED_JSON = os.path.join(os.path.dirname(
    __file__), 'infrastructure', 'cava-gsheet.json')
SCOPE = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


class CAVA(DataSource):

    def __init__(self):
        super().__init__()

        self._cava_arrays = None
        self._cava_sites = None
        self._cava_infrastructures = None
        self._cava_instruments = None
        self._cava_parameters = None

        self._cava_setup()

    @property
    def cava_arrays(self):
        return self._cava_arrays

    @property
    def cava_sites(self):
        return self._cava_sites

    @property
    def cava_infrastructures(self):
        return self._cava_infrastructures

    @property
    def cava_instruments(self):
        return self._cava_instruments

    @property
    def cava_parameters(self):
        return self._cava_parameters

    def _retrieve_gsheet(self):
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CRED_JSON,  # noqa
                                                                       SCOPE)
        gc = gspread.authorize(credentials)
        gsheet = gc.open('Reference_Designator_Chart')

        self._cava_arrays = self._get_df_from_gsheet(gsheet, 'Arrays')
        self._cava_sites = self._get_df_from_gsheet(gsheet, 'Sites')
        self._cava_infrastructures = self._get_df_from_gsheet(
            gsheet, 'Infrastructures')
        self._cava_instruments = self._get_df_from_gsheet(
            gsheet, 'Instruments')
        self._cava_parameters = self._get_df_from_gsheet(gsheet, 'Parameters')

    def _cava_setup(self):

        gthread = threading.Thread(name='retrieve-gsheet',
                                   target=self._retrieve_gsheet)
        gthread.setDaemon(True)

        gthread.start()

    def _get_df_from_gsheet(self, gsheet, worksheet_name):
        wksheet = gsheet.worksheet(worksheet_name)
        return pd.DataFrame.from_records(wksheet.get_all_records())
