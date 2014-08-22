"""
@package mi.dataset.driver.phsen_abcdef.dcl.driver
@file marine-integrations/mi/dataset/driver/phsen_abcdef/dcl/driver.py
@author Jeremy Amundson
@brief Driver for the phsen_abcdef_dcl
Release notes:

Initial Release
"""

__author__ = 'Jeremy Amundson'
__license__ = 'Apache 2.0'

import string

from mi.core.log import get_logger ; log = get_logger()

from mi.dataset.dataset_driver import SimpleDataSetDriver
from mi.dataset.parser.phsen_abcdef_dcl import PhsenAbcdefDclParser,
from mi.core.common import BaseEnum

class DataTypeKey(BaseEnum):

    FLORT_DJ_CSPP_RECOVERED = 'phsen_abcedf_dcl_recovered'
    FLORT_DJ_CSPP_TELEMETERED = 'phsen_abcedf_dcl_telemetered'

class PhsenAbcdefDclDataSetDriver(SimpleDataSetDriver):
    
    @classmethod
    def stream_config(cls):
        return [PhsenAbcdefDclParserDataParticle.type()]

    def _build_parser(self, parser_state, infile):
        """
        Build and return the parser
        """
        config = self._parser_config
        config.update({
            'particle_module': 'mi.dataset.parser.phsen_abcdef_dcl',
            'particle_class': 'PhsenAbcdefDclParserDataParticle'
        })
        log.debug("My Config: %s", config)
        self._parser = PhsenAbcdefDclParser(
            config,
            parser_state,
            infile,
            self._save_parser_state,
            self._data_callback,
            self._sample_exception_callback 
        )
        return self._parser

    def _build_harvester(self, driver_state):
        """
        Build and return the harvester
        """
        # *** Replace the following with harvester initialization ***
        self._harvester = None     
        return self._harvester
