#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_wc_hmr_cspp
@file marine-integrations/mi/dataset/parser/test/test_wc_hmr_cspp.py
@author Jeff Roy
@brief Test code for a wc_hmr_cspp data parser

wc_hmr_cspp is based on cspp_base.py
test_dosta_abcdjm_cspp.py fully tests all of the capabilities of the
base parser.  That level of testing is omitted from this test suite
"""

import os
import yaml
import numpy

from nose.plugins.attrib import attr

from mi.core.log import get_logger
log = get_logger()

from mi.idk.config import Config

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_driver import DataSetDriverConfigKeys

from mi.core.exceptions import RecoverableSampleException

from mi.dataset.parser.cspp_base import \
    StateKey, \
    METADATA_PARTICLE_CLASS_KEY, \
    DATA_PARTICLE_CLASS_KEY

from mi.dataset.parser.wc_hmr_cspp import \
    WcHmrCsppParser, \
    WcHmrEngRecoveredDataParticle, \
    WcHmrEngTelemeteredDataParticle, \
    WcHmrMetadataRecoveredDataParticle, \
    WcHmrMetadataTelemeteredDataParticle, \
    WcHmrDataTypeKey

RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi', 'dataset', 'driver', 'cspp_eng', 'cspp', 'resource')


@attr('UNIT', group='mi')
class WcHmrCsppParserUnitTestCase(ParserUnitTestCase):
    """
    wc_hmr_cspp Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Callback method to watch what comes in via the exception callback """
        self.exception_callback_value = exception

    def setUp(self):
        ParserUnitTestCase.setUp(self)
        self.config = {
            WcHmrDataTypeKey.WC_HMR_CSPP_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcHmrMetadataTelemeteredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcHmrEngTelemeteredDataParticle,
                }
            },
            WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    METADATA_PARTICLE_CLASS_KEY: WcHmrMetadataRecoveredDataParticle,
                    DATA_PARTICLE_CLASS_KEY: WcHmrEngRecoveredDataParticle,
                }
            },
        }

        # Define test data particles and their associated timestamps which will be
        # compared with returned results

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None
        self.exception_callback_value = None

    def particle_to_yml(self, particles, filename, mode='w'):
        """
        This is added as a testing helper, not actually as part of the parser tests. Since the same particles
        will be used for the driver test it is helpful to write them to .yml in the same form they need in the
        results.yml fids here.
        """
        # open write append, if you want to start from scratch manually delete this fid
        fid = open(os.path.join(RESOURCE_PATH, filename), mode)

        fid.write('header:\n')
        fid.write("    particle_object: 'MULTIPLE'\n")
        fid.write("    particle_type: 'MULTIPLE'\n")
        fid.write('data:\n')

        for i in range(0, len(particles)):
            particle_dict = particles[i].generate_dict()

            fid.write('  - _index: %d\n' % (i+1))

            fid.write('    particle_object: %s\n' % particles[i].__class__.__name__)
            fid.write('    particle_type: %s\n' % particle_dict.get('stream_name'))
            fid.write('    internal_timestamp: %f\n' % particle_dict.get('internal_timestamp'))

            for val in particle_dict.get('values'):
                if isinstance(val.get('value'), float):
                    fid.write('    %s: %16.16f\n' % (val.get('value_id'), val.get('value')))
                elif isinstance(val.get('value'), str):
                    fid.write("    %s: '%s'\n" % (val.get('value_id'), val.get('value')))
                else:
                    fid.write('    %s: %s\n' % (val.get('value_id'), val.get('value')))
        fid.close()

    def get_dict_from_yml(self, filename):
        """
        This utility routine loads the contents of a yml file
        into a dictionary
        """

        fid = open(os.path.join(RESOURCE_PATH, filename), 'r')
        result = yaml.load(fid)
        fid.close()

        return result

    def create_yml(self):
        """
        This utility creates a yml file
        Be sure to verify the results by eye before trusting!
        """

        fid = open(os.path.join(RESOURCE_PATH, '11079364_WC_HMR.txt'), 'r')

        stream_handle = fid
        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 None, stream_handle,
                                 self.state_callback, self.pub_callback,
                                 self.exception_callback)

        particles = parser.get_records(20)

        self.particle_to_yml(particles, '11079364_WC_HMR_recov.yml')
        fid.close()

    def assert_result(self, test, particle):
        """
        Suite of tests to run against each returned particle and expected
        results of the same.  The test parameter should be a dictionary
        that contains the keys to be tested in the particle
        the 'internal_timestamp' and 'position' keys are
        treated differently than others but can be verified if supplied
        """

        particle_dict = particle.generate_dict()

        #for efficiency turn the particle values list of dictionaries into a dictionary
        particle_values = {}
        for param in particle_dict.get('values'):
            particle_values[param['value_id']] = param['value']
            # log.debug('### building building particle values ###')
            # log.debug('value_id = %s', param['value_id'])
            # log.debug('value = %s', param['value'])

        # compare each key in the test to the data in the particle
        for key in test:
            test_data = test[key]

            #get the correct data to compare to the test
            if key == 'internal_timestamp':
                particle_data = particle.get_value('internal_timestamp')
                #the timestamp is in the header part of the particle
            elif key == 'position':
                particle_data = self.state_callback_value['position']
                #position corresponds to the position in the file
            else:
                particle_data = particle_values.get(key)
                #others are all part of the parsed values part of the particle

            # log.debug('*** assert result: test data key = %s', key)
            # log.debug('*** assert result: test data val = %s', test_data)
            # log.debug('*** assert result: part data val = %s', particle_data)

            if particle_data is None:
                #generally OK to ignore index keys in the test data, verify others

                log.warning("\nWarning: assert_result ignoring test key %s, does not exist in particle", key)
            else:
                if isinstance(test_data, float):

                    # slightly different test for these values as they are floats.
                    compare = numpy.abs(test_data - particle_data) <= 1e-5
                    # log.debug('*** assert result: compare = %s', compare)
                    self.assertTrue(compare)
                else:
                    # otherwise they are all ints and should be exactly equal
                    self.assertEqual(test_data, particle_data)

    def test_simple(self):
        """
        Read test data and pull out data particles
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_HMR.txt')
        stream_handle = open(file_path, 'r')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 None, stream_handle,
                                 self.state_callback, self.pub_callback,
                                 self.exception_callback)

        particles = parser.get_records(20)

        log.debug("*** test_simple Num particles %s", len(particles))

        # check the first particle, which should be the metadata particle (recovered)
        test_data = self.get_dict_from_yml('11079364_WC_HMR_recov.yml')
        #
        # # check all the values against expected results.
        #
        for i in range(len(particles)):

            self.assert_result(test_data['data'][i], particles[i])

        stream_handle.close()

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """
        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_HMR.txt')
        stream_handle = open(file_path, 'r')

        # Note: since the recovered and telemetered parser and particles are common
        # to each other, testing one is sufficient, will be completely tested
        # in driver tests

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_TELEMETERED),
                                 None, stream_handle,
                                 self.state_callback, self.pub_callback,
                                 self.exception_callback)

        # try to get 2000 particles, there are more data records in the file
        # so should get 2000 including the meta data
        particles = parser.get_records(2000)

        log.debug("*** test_get_many Num particles %s", len(particles))
        self.assertEqual(len(particles), 2000)

        stream_handle.close()

    def test_mid_state_start(self):
        """
        This test makes sure that we retrieve the correct particles upon starting with an offset state.
        """

        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_HMR.txt')
        stream_handle = open(file_path, 'r')

        # position 547 is the end of the 7th data record, which would have produced the
        # metadata particle and the first 7 engineering particles
        initial_state = {StateKey.POSITION: 549, StateKey.METADATA_EXTRACTED: True}

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 initial_state, stream_handle,
                                 self.state_callback, self.pub_callback,
                                 self.exception_callback)

        #expect to get the 8th and 9th engineering particles next
        particles = parser.get_records(2)

        log.debug("Num particles: %s", len(particles))

        self.assertTrue(len(particles) == 2)

        expected_results = self.get_dict_from_yml('11079364_WC_HMR_recov.yml')

        # skip the first 8 particles in the yml file due to mid state start
        offset = 8

        for i in range(len(particles)):
            self.assert_result(expected_results['data'][i + offset], particles[i])

        # now expect the state to be the end of the 9th data record and metadata sent
        the_new_state = {StateKey.POSITION: 627, StateKey.METADATA_EXTRACTED: True}
        log.debug("********** expected state: %s", the_new_state)
        log.debug("******** new parser state: %s", parser._state)
        self.assertTrue(parser._state == the_new_state)

        stream_handle.close()

    def test_set_state(self):
        """
        Test changing to a new state after initializing the parser and
        reading data, as if new data has been found and the state has
        changed
        """

        file_path = os.path.join(RESOURCE_PATH, '11079364_WC_HMR.txt')
        stream_handle = open(file_path, 'r')

        # 11079419_PPB_OCR_20.yml has the metadata and the first 19
        # engineering particles in it
        expected_results = self.get_dict_from_yml('11079364_WC_HMR_recov.yml')

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 None, stream_handle,
                                 self.state_callback, self.pub_callback,
                                 self.exception_callback)

        particles = parser.get_records(2)

        log.debug("Num particles: %s", len(particles))

        self.assertTrue(len(particles) == 2)

        for i in range(len(particles)):
            self.assert_result(expected_results['data'][i], particles[i])

        # position 945 is the byte at the start of the 18th data record
        new_state = {StateKey.POSITION: 945, StateKey.METADATA_EXTRACTED: True}

        parser.set_state(new_state)

        particles = parser.get_records(2)

        self.assertTrue(len(particles) == 2)

        # offset in the expected results
        offset = 18
        for i in range(len(particles)):
            self.assert_result(expected_results['data'][i + offset], particles[i])

        stream_handle.close()

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """

        # the first and 7th data record in this file are corrupted and will be ignored
        # we expect to get the metadata particle with the
        # timestamp from the 2nd data record and all of the valid engineering
        # data records

        file_path = os.path.join(RESOURCE_PATH, '11079364_BAD_WC_HMR.txt')
        stream_handle = open(file_path, 'r')

        log.info(self.exception_callback_value)

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 None, stream_handle,
                                 self.state_callback, self.pub_callback,
                                 self.exception_callback)

        # 18 particles
        particles = parser.get_records(18)

        expected_results = self.get_dict_from_yml('WC_HMR_bad_data_records.yml')

        self.assertTrue(len(particles) == 18)

        for i in range(len(particles)):
            self.assert_result(expected_results['data'][i], particles[i])

        self.assert_(isinstance(self.exception_callback_value, RecoverableSampleException))

        stream_handle.close()

    def test_extra_data(self):

        """
        Ensure that bad data is skipped when it exists.
        """

        # the first 2nd and 8th data record in this file are corrupted by adding additional
        # data values separated by tabs and will be ignored
        # we expect to get the metadata particle and only the valid
        # engineering data particles

        file_path = os.path.join(RESOURCE_PATH, '11079364_EXTRA_DATA_WC_HMR.txt')

        stream_handle = open(file_path, 'r')

        log.info(self.exception_callback_value)

        parser = WcHmrCsppParser(self.config.get(WcHmrDataTypeKey.WC_HMR_CSPP_RECOVERED),
                                 None, stream_handle,
                                 self.state_callback, self.pub_callback,
                                 self.exception_callback)

        particles = parser.get_records(18)

        self.assertTrue(self.exception_callback_value is not None)

        self.assert_(isinstance(self.exception_callback_value, RecoverableSampleException))

        # expect to see a recoverable sample exception in the log
        log.debug('TEST EXTRA DATA exception call back is %s', self.exception_callback_value)

        expected_results = self.get_dict_from_yml('WC_HMR_extra_data_values.yml')

        self.assertTrue(len(particles) == 18)

        # since the first two records were corrupted the first records received
        # should be metadata particle with the timestamp of the 3rd data row
        # and the engineering particle from the 3rd row

        for i in range(len(particles)):
            self.assert_result(expected_results['data'][i], particles[i])

        self.assert_(isinstance(self.exception_callback_value, RecoverableSampleException))

        stream_handle.close()
