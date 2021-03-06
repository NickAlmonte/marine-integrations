"""
@package mi.dataset.driver.moas.gl.parad.test.test_driver
@file marine-integrations/mi/dataset/driver/moas/gl/parad/test/test_driver.py
@author Nick Almonte
@brief Test cases for glider parad data

USAGE:
 Make tests verbose and provide stdout
   * From the IDK
       $ bin/dsa/test_driver
       $ bin/dsa/test_driver -i [-t testname]
       $ bin/dsa/test_driver -q [-t testname]
"""

__author__ = 'Nick Almonte'
__license__ = 'Apache 2.0'

import unittest

from nose.plugins.attrib import attr

from mi.core.log import get_logger ; log = get_logger()

from exceptions import Exception

from mi.idk.dataset.unit_test import DataSetTestCase
from mi.idk.dataset.unit_test import DataSetIntegrationTestCase
from mi.idk.dataset.unit_test import DataSetQualificationTestCase

from mi.idk.exceptions import SampleTimeout

from mi.dataset.dataset_driver import DataSourceConfigKey, DataSetDriverConfigKeys
from mi.dataset.dataset_driver import DriverParameter
from mi.dataset.driver.moas.gl.parad.driver import DataTypeKey
from mi.dataset.driver.moas.gl.parad.driver import PARADDataSetDriver

from mi.dataset.parser.glider import ParadTelemeteredDataParticle, ParadRecoveredDataParticle, DataParticleType
from pyon.agent.agent import ResourceAgentState

from interface.objects import ResourceAgentErrorEvent

TELEMETERED_TEST_DIR = '/tmp/paradTelemeteredTest'
RECOVERED_TEST_DIR = '/tmp/paradRecoveredTest'

DataSetTestCase.initialize(

    driver_module='mi.dataset.driver.moas.gl.parad.driver',
    driver_class="PARADDataSetDriver",
    agent_resource_id='123xyz',
    agent_name='Agent007',
    agent_packet_config=PARADDataSetDriver.stream_config(),
    startup_config={
        DataSourceConfigKey.RESOURCE_ID: 'parad',
        DataSourceConfigKey.HARVESTER:
        {
            DataTypeKey.PARAD_TELEMETERED:
            {
                DataSetDriverConfigKeys.DIRECTORY: TELEMETERED_TEST_DIR,
                DataSetDriverConfigKeys.STORAGE_DIRECTORY: '/tmp/stored_paradTelemeteredTest',
                DataSetDriverConfigKeys.PATTERN: '*.mrg',
                DataSetDriverConfigKeys.FREQUENCY: 1,
            },
            DataTypeKey.PARAD_RECOVERED:
            {
                DataSetDriverConfigKeys.DIRECTORY: RECOVERED_TEST_DIR,
                DataSetDriverConfigKeys.STORAGE_DIRECTORY: '/tmp/stored_paradRecoveredTest',
                DataSetDriverConfigKeys.PATTERN: '*.mrg',
                DataSetDriverConfigKeys.FREQUENCY: 1,
            }
        },
        DataSourceConfigKey.PARSER: {
            DataTypeKey.PARAD_TELEMETERED: {}, DataTypeKey.PARAD_RECOVERED: {}
        }
    }

)

###############################################################################
#                                UNIT TESTS                                   #
# Device specific unit tests are for                                          #
# testing device specific capabilities                                        #
###############################################################################
@attr('INT', group='mi')
class IntegrationTest(DataSetIntegrationTestCase):

    def test_get(self):
        """
        Test that we can get data from files.  Verify that the driver sampling
        can be started and stopped.
        """
        self.clear_sample_data()

        # Start sampling and watch for an exception
        self.driver.start_sampling()

        self.clear_async_data()
        self.create_sample_data_set_dir('single_glider_record.mrg',
                                        TELEMETERED_TEST_DIR,
                                        "unit_363_2013_245_6_6.mrg")
        self.assert_data(ParadTelemeteredDataParticle,
                         'single_parad_record.mrg.result.yml',
                         count=1, timeout=10)

        self.clear_async_data()
        self.create_sample_data_set_dir('multiple_glider_record.mrg',
                                        TELEMETERED_TEST_DIR,
                                        "unit_363_2013_245_7_6.mrg")
        self.assert_data(ParadTelemeteredDataParticle,
                         'multiple_parad_record.mrg.result.yml',
                         count=4, timeout=10)

        self.clear_async_data()
        self.create_sample_data_set_dir('single_glider_record.mrg',
                                        RECOVERED_TEST_DIR,
                                        "unit_363_2013_245_6_6_rec.mrg")
        self.assert_data(ParadRecoveredDataParticle,
                         'single_parad_record_recovered.mrg.result.yml',
                         count=1, timeout=10)

        self.clear_async_data()
        self.create_sample_data_set_dir('multiple_glider_record.mrg',
                                        RECOVERED_TEST_DIR,
                                        "unit_363_2013_245_7_6.mrg")
        self.assert_data(ParadRecoveredDataParticle,
                         'multiple_parad_record_recovered.mrg.result.yml',
                         count=4, timeout=10)


        log.debug("Start second file ingestion - Telemetered")
        # Verify sort order isn't ascii, but numeric
        self.clear_async_data()
        self.create_sample_data_set_dir('unit_247_2012_051_0_0-sciDataOnly.mrg',
                                        TELEMETERED_TEST_DIR,
                                        'unit_363_2013_245_10_6.mrg')
        self.assert_data(ParadTelemeteredDataParticle, count=115, timeout=30)

        log.debug("Start second file ingestion - Recovered")
        # Verify sort order isn't ascii, but numeric
        self.clear_async_data()
        self.create_sample_data_set_dir('unit_247_2012_051_0_0-sciDataOnly.mrg',
                                        RECOVERED_TEST_DIR,
                                        "unit_363_2013_245_10_6.mrg")
        self.assert_data(ParadRecoveredDataParticle, count=115, timeout=30)

    def test_stop_resume(self):
        """
        Test the ability to stop and restart the process
        """
        path_1 = self.create_sample_data_set_dir('single_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_6_8.mrg")
        path_2 = self.create_sample_data_set_dir('multiple_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_6_9.mrg")
        path_3 = self.create_sample_data_set_dir('single_glider_record.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_6_8.mrg")
        path_4 = self.create_sample_data_set_dir('multiple_glider_record.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_6_9.mrg")

        # Create and store the new driver state
        state = {
            DataTypeKey.PARAD_TELEMETERED: {
            'unit_363_2013_245_6_8.mrg': self.get_file_state(path_1, True, 9196),
            'unit_363_2013_245_6_9.mrg': self.get_file_state(path_2, False, 9196)
            },
            DataTypeKey.PARAD_RECOVERED: {
            'unit_363_2013_245_6_8.mrg': self.get_file_state(path_3, True, 9196),
            'unit_363_2013_245_6_9.mrg': self.get_file_state(path_4, False, 9196)
            }
        }
        self.driver = self._get_driver_object(memento=state)

        # create some data to parse
        self.clear_async_data()

        self.driver.start_sampling()

        # verify data is produced for telemetered particle
        self.assert_data(ParadTelemeteredDataParticle, 'merged_parad_record.mrg.result.yml', count=3, timeout=10)

        # verify data is produced for recovered particle
        self.assert_data(ParadRecoveredDataParticle, 'merged_parad_record_recovered.mrg.result.yml', count=3, timeout=10)

    def test_stop_start_ingest(self):
        """
        Test the ability to stop and restart sampling, and ingesting files in the correct order
        """
        # create some data to parse
        self.clear_async_data()

        self.driver.start_sampling()

        self.create_sample_data_set_dir('single_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_6_6.mrg")
        self.create_sample_data_set_dir('multiple_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_7_6.mrg")
        self.assert_data(ParadTelemeteredDataParticle, 'single_parad_record.mrg.result.yml', count=1, timeout=10)
        self.assert_file_ingested("unit_363_2013_245_6_6.mrg", DataTypeKey.PARAD_TELEMETERED)
        self.assert_file_not_ingested("unit_363_2013_245_7_6.mrg")

        self.driver.stop_sampling()
        self.driver.start_sampling()

        self.assert_data(ParadTelemeteredDataParticle, 'multiple_parad_record.mrg.result.yml', count=4, timeout=10)
        self.assert_file_ingested("unit_363_2013_245_7_6.mrg", DataTypeKey.PARAD_TELEMETERED)

        ####
        ## Repeat for Recovered Particle
        ####
        self.create_sample_data_set_dir('single_glider_record.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_6_6.mrg")
        self.create_sample_data_set_dir('multiple_glider_record.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_7_6.mrg")
        self.assert_data(ParadRecoveredDataParticle, 'single_parad_record_recovered.mrg.result.yml', count=1, timeout=10)
        self.assert_file_ingested("unit_363_2013_245_6_6.mrg", DataTypeKey.PARAD_RECOVERED)
        self.assert_file_not_ingested("unit_363_2013_245_7_6.mrg")

        self.driver.stop_sampling()
        self.driver.start_sampling()

        self.assert_data(ParadRecoveredDataParticle, 'multiple_parad_record_recovered.mrg.result.yml', count=4, timeout=10)
        self.assert_file_ingested("unit_363_2013_245_7_6.mrg", DataTypeKey.PARAD_RECOVERED)

    def test_bad_sample(self):
        """
        Test a bad sample.  To do this we set a state to the middle of a record
        """
        path_2 = self.create_sample_data_set_dir('multiple_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_6_9.mrg")
        path_4 = self.create_sample_data_set_dir('multiple_glider_record.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_6_9.mrg")

        # Create and store the new driver state
        state = {
            DataTypeKey.PARAD_TELEMETERED: {
             'unit_363_2013_245_6_9.mrg': self.get_file_state(path_2, False, 9196)
            },
            DataTypeKey.PARAD_RECOVERED: {
            'unit_363_2013_245_6_9.mrg': self.get_file_state(path_4, False, 9196)
            }
        }
        self.driver = self._get_driver_object(memento=state)

        # create some data to parse
        self.clear_async_data()

        self.driver.start_sampling()

        # verify data is produced for telemetered particle
        self.assert_data(ParadTelemeteredDataParticle, 'merged_parad_record.mrg.result.yml', count=3, timeout=10)

        # verify data is produced for recovered particle
        self.assert_data(ParadRecoveredDataParticle, 'merged_parad_record_recovered.mrg.result.yml', count=3, timeout=10)

    def test_sample_exception_telemetered(self):
        """
        test that a file is marked as parsed if it has a sample exception (which will happen with an empty file)
        """
        self.clear_async_data()

        config = self._driver_config()['startup_config']['harvester'][DataTypeKey.PARAD_TELEMETERED]['pattern']
        filename = config.replace("*", "foo")
        self.create_sample_data_set_dir(filename, TELEMETERED_TEST_DIR)

        # Start sampling and watch for an exception
        self.driver.start_sampling()
        # an event catches the sample exception
        self.assert_event('ResourceAgentErrorEvent')
        self.assert_file_ingested(filename, DataTypeKey.PARAD_TELEMETERED)

    def test_sample_exception_recovered(self):
        """
        test that a file is marked as parsed if it has a sample exception (which will happen with an empty file)
        """
        self.clear_async_data()

        config = self._driver_config()['startup_config']['harvester'][DataTypeKey.PARAD_RECOVERED]['pattern']
        filename = config.replace("*", "foo")
        self.create_sample_data_set_dir(filename, RECOVERED_TEST_DIR)

        # Start sampling and watch for an exception
        self.driver.start_sampling()
        # an event catches the sample exception
        self.assert_event('ResourceAgentErrorEvent')
        self.assert_file_ingested(filename, DataTypeKey.PARAD_RECOVERED)


###############################################################################
#                            QUALIFICATION TESTS                              #
# Device specific qualification tests are for                                 #
# testing device specific capabilities                                        #
###############################################################################
@attr('QUAL', group='mi')
class QualificationTest(DataSetQualificationTestCase):
    def setUp(self):
        super(QualificationTest, self).setUp()

    def test_publish_path(self):
        """
        Setup an agent/driver/harvester/parser and verify that data is
        published out the agent
        """
        self.create_sample_data_set_dir('single_glider_record.mrg', TELEMETERED_TEST_DIR, 'unit_363_2013_245_6_9.mrg')
        self.assert_initialize()

        # Verify we get one sample
        try:
            result = self.data_subscribers.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 1)
            log.debug("Telemetered RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'single_parad_record.mrg.result.yml')
        except Exception as e:
            log.error("Exception trapped: %s", e)
            self.fail("Sample timeout.")

        # Again for the recovered particle
        self.create_sample_data_set_dir('single_glider_record.mrg', RECOVERED_TEST_DIR, 'unit_363_2013_245_6_9.mrg')

        # Verify we get one sample
        try:
            result = self.data_subscribers.get_samples(DataParticleType.PARAD_M_GLIDER_RECOVERED, 1)
            log.debug("Recovered RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'single_parad_record_recovered.mrg.result.yml')
        except Exception as e:
            log.error("Exception trapped: %s", e)
            self.fail("Sample timeout.")

    def test_large_import(self):
        """
        There is a bug when activating an instrument go_active times out and
        there was speculation this was due to blocking behavior in the agent.
        https://jira.oceanobservatories.org/tasks/browse/OOIION-1284
        """
        self.create_sample_data_set_dir('unit_247_2012_051_0_0-sciDataOnly.mrg', TELEMETERED_TEST_DIR)
        self.assert_initialize()

        result1 = self.data_subscribers.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 115, 20)

        # again for recovered
        self.create_sample_data_set_dir('unit_247_2012_051_0_0-sciDataOnly.mrg', RECOVERED_TEST_DIR)
        result2 = self.data_subscribers.get_samples(DataParticleType.PARAD_M_GLIDER_RECOVERED, 115, 120)

    def test_stop_start(self):
        """
        Test the agents ability to start data flowing, stop, then restart
        at the correct spot.
        """
        log.info("## ## ## CONFIG: %s", self._agent_config())
        self.create_sample_data_set_dir('single_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_6_6.mrg")

        self.assert_initialize(final_state=ResourceAgentState.COMMAND)

        # Slow down processing to 1 per second to give us time to stop
        self.dataset_agent_client.set_resource({DriverParameter.RECORDS_PER_SECOND: 1})
        self.assert_start_sampling()

        try:
            # Read the first file and verify the data
            result = self.data_subscribers.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT)
            log.debug("## ## ## RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'single_parad_record.mrg.result.yml')
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 0)


            # Stop sampling: Telemetered
            self.create_sample_data_set_dir('multiple_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_7_6.mrg")
            # Now read the first three records of the second file then stop
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 1)
            log.debug("## ## ## Got result 1 %s", result)
            self.assert_stop_sampling()
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 0)

            # Restart sampling and ensure we get the last 3 records of the file
            self.assert_start_sampling()
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 3)
            log.debug("got result 2 %s", result)
            self.assert_data_values(result, 'merged_parad_record.mrg.result.yml')
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 0)


            #Stop sampling: Recovered
            self.create_sample_data_set_dir('multiple_glider_record.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_7_6.mrg")
            # Now read the first three records of the second file then stop
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_RECOVERED, 1)
            log.debug("got result 1 %s", result)
            self.assert_stop_sampling()
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_RECOVERED, 0)

            # Restart sampling and ensure we get the last 3 records of the file
            self.assert_start_sampling()
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_RECOVERED, 3)
            log.debug("got result 2 %s", result)
            self.assert_data_values(result, 'merged_parad_record_recovered.mrg.result.yml')
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_RECOVERED, 0)

        except SampleTimeout as e:
            log.error("Exception trapped: %s", e, exc_info=True)
            self.fail("Sample timeout.")

    def test_shutdown_restart(self):
        """
        Test the agents ability to completely stop, then restart
        at the correct spot.
        """
        log.info("## ## ## CONFIG: %s", self._agent_config())
        self.create_sample_data_set_dir('single_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_6_6.mrg")

        self.assert_initialize(final_state=ResourceAgentState.COMMAND)

        # Slow down processing to 1 per second to give us time to stop
        self.dataset_agent_client.set_resource({DriverParameter.RECORDS_PER_SECOND: 1})
        self.assert_start_sampling()

        try:
            # Read the first file and verify the data
            result = self.data_subscribers.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT)
            log.debug("## ## ## RESULT: %s", result)

            # Verify values
            self.assert_data_values(result, 'single_parad_record.mrg.result.yml')
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 0)


            # Restart sampling: Telemetered
            self.create_sample_data_set_dir('multiple_glider_record.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_7_6.mrg")
            # Now read the first three records of the second file then stop
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 1)
            log.debug("## ## ## Got result 1 %s", result)
            self.assert_stop_sampling()
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 0)

            # stop the agent
            self.stop_dataset_agent_client()
            # re-start the agent
            self.init_dataset_agent_client()
            # re-initialize
            self.assert_initialize(final_state=ResourceAgentState.COMMAND)

            # Slow down processing to 1 per second to give us time to stop
            self.dataset_agent_client.set_resource({DriverParameter.RECORDS_PER_SECOND: 1})

            # Restart sampling and ensure we get the last 3 records of the file
            self.assert_start_sampling()
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 3)
            log.debug("got result 2 %s", result)
            self.assert_data_values(result, 'merged_parad_record.mrg.result.yml')
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 0)


            # Restart sampling: Recovered
            self.create_sample_data_set_dir('multiple_glider_record.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_7_6.mrg")
            # Now read the first three records of the second file then stop
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_RECOVERED, 1)
            log.debug("got result 1 %s", result)
            self.assert_stop_sampling()
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_RECOVERED, 0)

            # stop the agent
            self.stop_dataset_agent_client()
            # re-start the agent
            self.init_dataset_agent_client()
            # re-initialize
            self.assert_initialize(final_state=ResourceAgentState.COMMAND)

            # Restart sampling and ensure we get the last 3 records of the file
            self.assert_start_sampling()
            result = self.get_samples(DataParticleType.PARAD_M_GLIDER_RECOVERED, 3)
            log.debug("got result 2 %s", result)
            self.assert_data_values(result, 'merged_parad_record_recovered.mrg.result.yml')
            self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_RECOVERED, 0)

        except SampleTimeout as e:
            log.error("Exception trapped: %s", e, exc_info=True)
            self.fail("Sample timeout.")


    def test_parser_exception(self):
        """
        Test an exception raised after the driver is started during
        record parsing.
        """
       # cause the error for telemetered
        self.clear_sample_data()
        self.create_sample_data_set_dir('non-input_file.mrg', TELEMETERED_TEST_DIR, "unit_363_2013_245_7_7.mrg")

        self.assert_initialize()

        self.event_subscribers.clear_events()
        self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_INSTRUMENT, 0)


        # Verify an event was raised and we are in our retry state
        self.assert_event_received(ResourceAgentErrorEvent, 40)
        self.assert_state_change(ResourceAgentState.STREAMING, 10)




        # # cause the same error for recovered
        self.event_subscribers.clear_events()
        self.clear_sample_data()
        self.create_sample_data_set_dir('non-input_file.mrg', RECOVERED_TEST_DIR, "unit_363_2013_245_7_8.mrg")

        self.assert_sample_queue_size(DataParticleType.PARAD_M_GLIDER_RECOVERED, 0)


        # Verify an event was raised and we are in our retry state
        self.assert_event_received(ResourceAgentErrorEvent, 40)
        self.assert_state_change(ResourceAgentState.STREAMING, 10)