#!/usr/bin/env python

"""
@package mi.dataset.parser.phsen_abcdef_dcl
@file marine-integrations/mi/dataset/parser/phsen_abcdef_dclpy
@author Nick Almonte
@brief Parser for the phsen_abcdef_dcl dataset driver
Release notes:

initial release
"""

__author__ = 'Nick Almonte'
__license__ = 'Apache 2.0'

import copy
import re

from functools import partial

from mi.core.log import get_logger
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle
from mi.core.exceptions import SampleException, DatasetParserException, UnexpectedDataException, RecoverableSampleException
from mi.dataset.dataset_parser import BufferLoadingParser
from mi.core.instrument.chunker import StringChunker
from mi.dataset.dataset_driver import DataSetDriverConfigKeys

METADATA_PARTICLE_CLASS_KEY = 'metadata_particle_class'
# The key for the data particle class
DATA_PARTICLE_CLASS_KEY = 'data_particle_class'

log = get_logger()


def _calculate_working_record_checksum(working_record):
    """
    Calculates the checksum of the argument ascii-hex string
    @retval int - modulo integer checksum value of argument ascii-hex string
    """
    log.debug("_calculate_working_record_checksum(): string_length is %s, working_record is %s",
              len(working_record), working_record)

    checksum = 0

    ## strip off the leading * and ID characters of the log line (3 characters) and
    ## strip off the trailing Checksum characters (2 characters)
    star_and_checksum_stripped_working_record = working_record[3:-2]

    working_record_length = len(star_and_checksum_stripped_working_record)

    log.debug("_calculate_working_record_checksum(): stripped working_record length is %s and now %s",
              working_record_length, star_and_checksum_stripped_working_record)

    for x in range(0, working_record_length, 2):
        value = star_and_checksum_stripped_working_record[x:x+2]
        checksum += int(value, 16)

    return checksum % 256


class DataParticleType(BaseEnum):
    """
    The data particle types that a phsen_abcdef_dcl parser may generate
    """
    METADATA_RECOVERED = 'phsen_abcdef_dcl_metadata_recovered'
    INSTRUMENT_RECOVERED = 'phsen_abcdef_dcl_instrument_recovered'
    METADATA_TELEMETERED = 'phsen_abcdef_dcl_metadata'
    INSTRUMENT_TELEMETERED = 'phsen_abcdef_dcl_instrument'


class StateKey(BaseEnum):
    POSITION = 'position'  # hold the current file position
    START_OF_DATA = 'start_of_data'


class PhsenAbcdefDclMetadataDataParticle(DataParticle):

    def _bin_to_bool(self, arg_int):
        """
        Returns False for 0 and True for 1, raises a recoverable exception for any other argument integer input
        """
        #log.debug("PhsenAbcdefDclMetadataDataParticle._bin_to_bool(): arg int= %s,", arg_int)

        if arg_int == 1:
            return True
        elif arg_int == 0:
            return False
        else:
            raise RecoverableSampleException("PhsenAbcdefDclMetadataDataParticle._bin_to_bool(): "
                                             "Argument int was neither 0 or 1")

    def _generate_particle(self):
        """
        Extracts PHSEN ABCDEF DCL Metadata data from raw_data.

        @returns result a list of dictionaries of particle data
        """
        ## extract the time from the raw_data touple
        dcl_controller_timestamp = self.raw_data[0]

        ## extract the working_record string from the raw data touple
        working_record = self.raw_data[1]

        # log.debug("PhsenAbcdefDclMetadataDataParticle._generate_particle(): dcl_controller_timestamp= %s, "
        #           "working_record= %s", dcl_controller_timestamp, working_record)
        #
        # log.debug("PhsenAbcdefDclMetadataDataParticle._generate_particle(): Data Length= %s, working_record Length= %s",
        #           int(working_record[3:5], 16), len(working_record))


        ## Per the IDD, voltage_battery data is optional and not guaranteed to be included in every CONTROL
        ## data record. Nominal size of a metadata string without the voltage_battery data is 39 (including the #).
        ## Voltage data adds 4 ascii characters to that, so raw_data greater than 41 contains voltage data,
        ## anything smaller does not.
        if len(working_record) >= 41:
            have_voltage_battery_data = True
        else:
            have_voltage_battery_data = False

        # log.debug("PhsenAbcdefDclMetadataDataParticle._generate_particle(): "
        #           "raw_data len= %s and contains: %s, have_voltage_battery_data= %s ",
        #           len(working_record), working_record, have_voltage_battery_data)

        ##
        ## Begin saving particle data
        ##
        unique_id_ascii_hex = working_record[1:3]
        ## convert 2 ascii (hex) chars to unsigned int
        unique_id_int = int(unique_id_ascii_hex, 16)

        record_type_ascii_hex = working_record[5:7]
        ## convert 2 ascii (hex) chars to unsigned int
        record_type_int = int(record_type_ascii_hex, 16)

        record_time_ascii_hex = working_record[7:15]
        ## convert 8 ascii (hex) chars to unsigned int
        record_time_int = int(record_time_ascii_hex, 16)

        ## TBD FLAGS
        flags_ascii_hex = working_record[15:19]
        ## convert 4 ascii (hex) chars to ...TBD
        flags_ascii_int = int(flags_ascii_hex, 16)
        binary_list = [ (flags_ascii_int >> x) & 0x1 for x in range(16)]
        # log.debug("PhsenAbcdefDclMetadataDataParticle._generate_particle(): binary_list= %s", binary_list)

        clock_active = self._bin_to_bool(binary_list[0])
        recording_active = self._bin_to_bool(binary_list[1])
        record_end_on_time = self._bin_to_bool(binary_list[2])
        record_memory_full = self._bin_to_bool(binary_list[3])
        record_end_on_error = self._bin_to_bool(binary_list[4])
        data_download_ok = self._bin_to_bool(binary_list[5])
        flash_memory_open = self._bin_to_bool(binary_list[6])
        battery_low_prestart = self._bin_to_bool(binary_list[7])
        battery_low_measurement = self._bin_to_bool(binary_list[8])
        battery_low_blank = self._bin_to_bool(binary_list[9])
        battery_low_external = self._bin_to_bool(binary_list[10])
        external_device1_fault = self._bin_to_bool(binary_list[11])
        external_device2_fault = self._bin_to_bool(binary_list[12])
        external_device3_fault = self._bin_to_bool(binary_list[13])
        flash_erased = self._bin_to_bool(binary_list[14])
        power_on_invalid = self._bin_to_bool(binary_list[15])

        # log.debug("hsenAbcdefDclMetadataDataParticle._generate_particle(): "
        #           "clock_active= %s, "
        #           "recording_active= %s, "
        #           "record_end_on_time= %s, "
        #           "record_memory_full= %s, "
        #           "record_end_on_error= %s, "
        #           "data_download_ok= %s, "
        #           "flash_memory_open= %s, "
        #           "battery_low_prestart= %s, "
        #           "battery_low_measurement= %s, "
        #           "battery_low_blank= %s, "
        #           "battery_low_external= %s, "
        #           "external_device1_fault= %s, "
        #           "external_device2_fault= %s, "
        #           "external_device3_fault= %s, "
        #           "flash_erased= %s, "
        #           "power_on_invalid= %s",
        #           clock_active,
        #           recording_active,
        #           record_end_on_time,
        #           record_memory_full,
        #           record_end_on_error,
        #           data_download_ok,
        #           flash_memory_open,
        #           battery_low_prestart,
        #           battery_low_measurement,
        #           battery_low_blank,
        #           battery_low_external,
        #           external_device1_fault,
        #           external_device2_fault,
        #           external_device3_fault,
        #           flash_erased,
        #           power_on_invalid)

        num_data_records_ascii_hex = working_record[19:25]
        ## convert 6 ascii (hex) chars to unsigned int
        num_data_records_int = int(num_data_records_ascii_hex, 16)

        num_error_records_ascii_hex = working_record[25:31]
        ## convert 6 ascii (hex) chars to unsigned int
        num_error_records_int = int(num_error_records_ascii_hex, 16)

        num_bytes_stored_ascii_hex = working_record[31:37]
        ## convert 6 ascii (hex) chars to unsigned int
        num_bytes_stored_int = int(num_bytes_stored_ascii_hex, 16)

        calculated_checksum = _calculate_working_record_checksum(working_record)

        ## Record may not have voltage data...
        if have_voltage_battery_data:
            voltage_battery_ascii_hex = working_record[37:41]
            ## convert 4 ascii (hex) chars to unsigned int
            voltage_battery_int = int(voltage_battery_ascii_hex, 16)

            passed_checksum_ascii_hex = working_record[41:43]
            ## convert 2 ascii (hex) chars to unsigned int
            passed_checksum_int = int(passed_checksum_ascii_hex, 16)

            ## Per IDD, if the calculated checksum does not match the checksum in the record,
            ## use a checksum of zero in the resultant particle
            if passed_checksum_int != calculated_checksum:
                checksum_final = 0
            else:
                checksum_final = passed_checksum_int
        else:
            voltage_battery_int = None

            passed_checksum_ascii_hex = working_record[37:39]
            ## convert 2 ascii (hex) chars to unsigned int
            passed_checksum_int = int(passed_checksum_ascii_hex, 16)

            ## Per IDD, if the calculated checksum does not match the checksum in the record,
            ## use a checksum of zero in the resultant particle
            if passed_checksum_int != calculated_checksum:
                checksum_final = 0
            else:
                checksum_final = passed_checksum_int

        log.debug("### ### ###PhsenAbcdefDclMetadataDataParticle._generate_particle(): "
                  "calculated_checksum= %s, passed_checksum_int= %s", calculated_checksum, passed_checksum_int)

        ## ASSEMBLE THE RESULTANT PARTICLE..
        resultant_particle = [{'value_id': 'dcl_controller_timestamp', 'value': dcl_controller_timestamp},
                              {'value_id': 'unique_id', 'value': unique_id_int},
                              {'value_id': 'record_type', 'value': record_type_int},
                              {'value_id': 'record_time', 'value': record_time_int},
                              {'value_id': 'clock_active', 'value': clock_active},
                              {'value_id': 'recording_active', 'value': recording_active},
                              {'value_id': 'record_end_on_time', 'value': record_end_on_time},
                              {'value_id': 'record_memory_full', 'value': record_memory_full},
                              {'value_id': 'record_end_on_error', 'value': record_end_on_error},
                              {'value_id': 'data_download_ok', 'value': data_download_ok},
                              {'value_id': 'flash_memory_open', 'value': flash_memory_open},
                              {'value_id': 'battery_low_prestart', 'value': battery_low_prestart},
                              {'value_id': 'battery_low_measurement', 'value': battery_low_measurement},
                              {'value_id': 'battery_low_blank', 'value': battery_low_blank},
                              {'value_id': 'battery_low_external', 'value': battery_low_external},
                              {'value_id': 'external_device1_fault', 'value': external_device1_fault},
                              {'value_id': 'external_device2_fault', 'value': external_device2_fault},
                              {'value_id': 'external_device3_fault', 'value': external_device3_fault},
                              {'value_id': 'flash_erased', 'value': flash_erased},
                              {'value_id': 'power_on_invalid', 'value': power_on_invalid},
                              {'value_id': 'num_data_records', 'value': num_data_records_int},
                              {'value_id': 'num_error_records', 'value': num_error_records_int},
                              {'value_id': 'num_bytes_stored', 'value': num_bytes_stored_int},
                              {'value_id': 'voltage_battery', 'value': voltage_battery_int},
                              {'value_id': 'passed_checksum', 'value': checksum_final}]

        return resultant_particle




class PhsenAbcdefDclInstrumentDataParticle(DataParticle):

    measurement_num_of_chars = 4

    def _create_light_measurements_array(self, working_record):
        """
        Creates a light measurement array from raw data for a PHSEN DCL Instrument record
        @returns list a list of light measurement values.  From the IDD: (an) array of 92 light measurements
                      (23 sets of 4 measurements)
        """
        light_measurements_list_int = []

        light_measurements_chunk = working_record[83:-14]
        light_measurements_ascii_hex = [light_measurements_chunk[i:i+self.measurement_num_of_chars]
                                        for i in range(0, len(light_measurements_chunk),
                                                     self.measurement_num_of_chars)]

        for ascii_hex_value in light_measurements_ascii_hex:
            light_measurements_list_int.append(int(ascii_hex_value, 16))

        # log.debug("PhsenAbcdefDclInstrumentDataParticle._create_light_measurements_array(): "
        #           "light_measurements_ascii_hex= %s", light_measurements_ascii_hex)
        # log.debug("PhsenAbcdefDclInstrumentDataParticle._create_light_measurements_array(): "
        #           "light_measurements_int= %s", light_measurements_int)

        return light_measurements_list_int

    def _create_reference_light_measurements_array(self, working_record):
        """
        Creates a reference light measurement array from raw data for a PHSEN DCL Instrument record
        @returns list a list of light measurement values.  From the IDD: (an) array of 16 measurements
                      (4 sets of 4 measurements)
        """
        reference_light_measurements_list_int = []

        reference_light_measurements_chunk = working_record[19:-382]
        reference_light_measurements_ascii_hex = [reference_light_measurements_chunk[i:i+self.measurement_num_of_chars]
                                                  for i in range(0, len(reference_light_measurements_chunk),
                                                                 self.measurement_num_of_chars)]

        for ascii_hex_value in reference_light_measurements_ascii_hex:
            reference_light_measurements_list_int.append(int(ascii_hex_value, 16))

        # log.debug("PhsenAbcdefDclInstrumentDataParticle._create_reference_light_measurements_array(): "
        #           "reference_light_measurements_ascii_hex= %s", reference_light_measurements_ascii_hex)
        # log.debug("PhsenAbcdefDclInstrumentDataParticle._create_reference_light_measurements_array(): "
        #           "reference_light_measurements_int= %s", reference_light_measurements_int)

        return reference_light_measurements_list_int

    def _generate_particle(self):
        """
        Extracts PHSEN ABCDEF DCL Instrument data from the raw_data touple.

        @returns result a list of dictionaries of particle data
        """
        ## extract the time from the raw_data touple
        dcl_controller_timestamp = self.raw_data[0]

        ## extract the working_record string from the raw data touple
        working_record = self.raw_data[1]

        # log.debug("PhsenAbcdefDclInstrumentDataParticle._generate_particle(): dcl_controller_timestamp= %s, "
        #           "working_record= %s", dcl_controller_timestamp, working_record)
        #
        # log.debug("PhsenAbcdefDclInstrumentDataParticle._generate_particle(): "
        #           "Data Length= %s, working_record Length= %s", int(working_record[3:5], 16), len(working_record))

        ##
        ## Begin saving particle data
        ##
        unique_id_ascii_hex = working_record[1:3]
        ## convert 2 ascii (hex) chars to unsigned int
        unique_id_int = int(unique_id_ascii_hex, 16)

        record_type_ascii_hex = working_record[5:7]
        ## convert 2 ascii (hex) chars to unsigned int
        record_type_int = int(record_type_ascii_hex, 16)

        record_time_ascii_hex = working_record[7:15]
        ## convert 8 ascii (hex) chars to unsigned int
        record_time_int = int(record_time_ascii_hex, 16)

        thermistor_start_ascii_hex = working_record[15:19]
        ## convert 4 ascii (hex) chars to unsigned int
        thermistor_start_int = int(thermistor_start_ascii_hex, 16)

        reference_light_measurements_list_int = self._create_reference_light_measurements_array(working_record)

        light_measurements_list_int = self._create_light_measurements_array(working_record)

        voltage_battery_ascii_hex = working_record[455:459]
        ## convert 4 ascii (hex) chars to unsigned int
        voltage_battery_int = int(voltage_battery_ascii_hex, 16)

        thermistor_end_ascii_hex = working_record[459:463]
        ## convert 4 ascii (hex) chars to unsigned int
        thermistor_end_int = int(thermistor_end_ascii_hex, 16)

        passed_checksum_ascii_hex = working_record[463:465]
        ## convert 2 ascii (hex) chars to unsigned int
        passed_checksum_int = int(passed_checksum_ascii_hex, 16)

        calculated_checksum = _calculate_working_record_checksum(working_record)

        # log.debug("### ### ###PhsenAbcdefDclInstrumentDataParticle._generate_particle(): "
        #           "calculated_checksum= %s, passed_checksum_int= %s", calculated_checksum, passed_checksum_int)

        ## Per IDD, if the calculated checksum does not match the checksum in the record,
        ## use a checksum of zero in the resultant particle
        if passed_checksum_int != calculated_checksum:
            checksum_final = 0
        else:
            checksum_final = passed_checksum_int

        ## ASSEMBLE THE RESULTANT PARTICLE..
        resultant_particle = [{'value_id': 'dcl_controller_timestamp', 'value': dcl_controller_timestamp},
                              {'value_id': 'unique_id', 'value': unique_id_int},
                              {'value_id': 'record_type', 'value': record_type_int},
                              {'value_id': 'record_time', 'value': record_time_int},
                              {'value_id': 'thermistor_start', 'value': thermistor_start_int},
                              {'value_id': 'reference_light_measurements', 'value': reference_light_measurements_list_int},
                              {'value_id': 'light_measurements', 'value': light_measurements_list_int},
                              {'value_id': 'voltage_battery', 'value': voltage_battery_int},
                              {'value_id': 'thermistor_end', 'value': thermistor_end_int},
                              {'value_id': 'passed_checksum', 'value': checksum_final}]

        return resultant_particle


class PhsenAbcdefDclMetadataRecoveredDataParticle(PhsenAbcdefDclMetadataDataParticle):

    _data_particle_type = DataParticleType.METADATA_RECOVERED

    def _build_parsed_values(self):
        """
        Takes a PHSenParser object and extracts PHSen DCL data from the
        data dictionary and puts the data into a PHSen DCL Data Particle.

        @returns result a list of dictionaries of particle data
        """
        resultant_particle = self._generate_particle()

        log.debug("PhsenAbcdefDclMetadataRecoveredDataParticle._build_parsed_values(): resultant_particle= %s",
                  resultant_particle)

        return resultant_particle


class PhsenAbcdefDclMetadataTelemeteredDataParticle(PhsenAbcdefDclMetadataDataParticle):

    _data_particle_type = DataParticleType.METADATA_TELEMETERED

    def _build_parsed_values(self):
        """
        Takes a PHSenParser object and extracts PHSen DCL data from the
        data dictionary and puts the data into a PHSen DCL Data Particle.

        @returns result a list of dictionaries of particle data
        """
        resultant_particle = self._generate_particle()

        log.debug("PhsenAbcdefDclMetadataTelemeteredDataParticle._build_parsed_values(): resultant_particle= %s",
                  resultant_particle)

        return resultant_particle


class PhsenAbcdefDclInstrumentRecoveredDataParticle(PhsenAbcdefDclInstrumentDataParticle):

    _data_particle_type = DataParticleType.INSTRUMENT_RECOVERED

    def _build_parsed_values(self):
        """
        Takes a PHSenParser particle object and extracts PHSen DCL data from the
        raw data and puts the data into a PHSen DCL Data Particle structure

        @returns result a list of dictionaries of particle data
        """
        resultant_particle = self._generate_particle()

        log.debug("PhsenAbcdefDclInstrumentRecoveredDataParticle._build_parsed_values(): resultant_particle= %s",
                  resultant_particle)

        return resultant_particle


class PhsenAbcdefDclInstrumentTelemeteredDataParticle(PhsenAbcdefDclInstrumentDataParticle):

    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED

    def _build_parsed_values(self):
        """
        Takes a PHSenParser particle object and extracts PHSen DCL data from the
        raw data and puts the data into a PHSen DCL Data Particle structure

        @returns result a list of dictionaries of particle data
        """
        resultant_particle = self._generate_particle()

        log.debug("PhsenAbcdefDclInstrumentTelemeteredDataParticle._build_parsed_values(): resultant_particle= %s",
                  resultant_particle)

        return resultant_particle


class dataTypeEnum():
    UNKNOWN = 0
    INSTRUMENT = 1
    CONTROL = 2


class PhsenAbcdefDclParser(BufferLoadingParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):

        # regex for first order parsing of input data from the chunker: newline
        line_regex = re.compile(r'.*(?:\r\n|\n)')
        # whitespace regex
        self._whitespace_regex = re.compile(r'\s*$')
        # instrument data regex: *
        self._instrument_data_regex = re.compile(r'\*')

        particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
        self._instrument_data_particle_class = particle_classes_dict.get('data_particle_class_key')
        self._metadata_particle_class = particle_classes_dict.get('metadata_particle_class_key')

        log.debug("PhsenAbcdefDclParser.init(): DATA PARTICLE CLASS= %s",
                  self._instrument_data_particle_class)
        log.debug("PhsenAbcdefDclParser.init(): METADATA PARTICLE CLASS= %s",
                  self._metadata_particle_class)


        super(PhsenAbcdefDclParser, self).__init__(config, stream_handle, state,
                                                            partial(StringChunker.regex_sieve_function,
                                                                    regex_list=[line_regex]),
                                                            state_callback,
                                                            publish_callback,
                                                            exception_callback,
                                                            *args,
                                                            **kwargs)

        self._read_state = {StateKey.POSITION: 0, StateKey.START_OF_DATA: False}

        self.working_record = ""

        self.in_record = False

        self.latest_dcl_time = ""

        self.result_particle_list = []

        if state:
            self.set_state(self._state)

    def set_state(self, state_obj):
        """
        Set the value of the state object for this parser
        @param state_obj The object to set the state to.
        @throws DatasetParserException if there is a bad state structure
        """
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")
        if not ((StateKey.POSITION in state_obj)):
            raise DatasetParserException("Missing state key %s" % StateKey.POSITION)
        if not ((StateKey.START_OF_DATA in state_obj)):
            raise DatasetParserException("Missing state key %s" % StateKey.START_OF_DATA)

        self._record_buffer = []
        self._state = state_obj
        self._read_state = state_obj
        self._chunker.clean_all_chunks()

        # seek to the position
        #log.debug("PhsenAbcdefDclParser._set_state(): seek to position: %d", state_obj[StateKey.POSITION])
        self._stream_handle.seek(state_obj[StateKey.POSITION])

    def _increment_state(self, increment):
        """
        Increment the parser state
        @param increment The amount to increment the position by
        """
        oldinc = increment
        oldstatepos = self._read_state[StateKey.POSITION]

        # log.debug("PhsenAbcdefDclParser._increment_state(): "
        #           "Incrementing current state: %s with inc: %s", self._read_state, increment)

        self._read_state[StateKey.POSITION] += increment

        # log.debug("PhsenAbcdefDclParser._increment_state(): "
        #           "Current State Position is %s, + Increment of %s", oldstatepos, oldinc)
        # log.debug("PhsenAbcdefDclParser._increment_state(): "
        #           "NEW State Position: %s", self._read_state[StateKey.POSITION])

    def _strip_logfile_line(self, logfile_line):
        """
        Strips any trailing newline and linefeed from the logfile line,
        and strips the leading DLC time from the logfile line
        """
        ## strip off any trailing linefeed or newline hidden characters
        working_logfile_line = self._strip_newline(logfile_line)

        ## strip off the preceding 24 characters (the DCL time) of the log line
        stripped_logfile_line = self._strip_time(working_logfile_line)

        return stripped_logfile_line

    def _strip_newline(self, logfile_line):

        ## strip off any trailing linefeed or newline hidden characters
        stripped_logfile_line = logfile_line.rstrip('\r\n')

        log.info("PhsenAbcdefDclParser._strip_newline(): newline stripped_logfile_line= %s", stripped_logfile_line)

        return stripped_logfile_line

    def _strip_time(self, logfile_line):

        ## strip off the leading 24 characters of the log line
        stripped_logfile_line = logfile_line[24:]

        # save off this DLC time in case this is the last DCL time recorded before the next record begins
        self.latest_dcl_time = logfile_line[:23]

        log.info("PhsenAbcdefDclParser._strip_time(): time stripped_logfile_line= %s, latest_dcl_time= %s",
                 stripped_logfile_line, self.latest_dcl_time)

        return stripped_logfile_line

    def _find(self, regex_pattern, line_to_process):
        """
        Determines whether the arg regex pattern appears in arg string
        @retval boolean - none (false), memory location (true)
        """
        match = re.search(regex_pattern, line_to_process)

        return match

    def _process_instrument_data(self, working_record):
        """
        Determines which particle to produce, calls extract_sample to create the given particle
        """
        log.debug("PhsenAbcdefDclParser._process_instrument_data(): aggregate working_record size %s is %s",
                  len(working_record), working_record)

        ## this size includes the leading * character
        instrument_record_length = 465

        ## this size includes the leading * character
        control_record_length_without_voltage_battery = 39

        ## this size includes the leading * character
        control_record_length_with_voltage_battery = 43

        data_type = self._determine_data_type(working_record)

        if data_type is not dataTypeEnum.UNKNOWN:

            ## Create a touple for the particle composed of the working record and latest DCL time
            ## The touple allows for DCL time to be available when EXTERNAL calls each particle's
            ## build_parse_values method
            particle_data = (self.latest_dcl_time, working_record)

            if data_type is dataTypeEnum.INSTRUMENT:

                log.debug("PhsenAbcdefDclParser._process_instrument_data(): "
                          "Record ID is INSTRUMENT")

                ## Per the IDD, if the candidate data is not the proper size, throw a recoverable exception
                if len(working_record) == instrument_record_length:

                    ## Create particle mule (to be used later to create the instrument particle)
                    particle = self._extract_sample(self._instrument_data_particle_class, None, particle_data, self.latest_dcl_time)

                    self.result_particle_list.append((particle, copy.copy(self._read_state)))
                else:
                    self._exception_callback(RecoverableSampleException("PhsenAbcdefDclParser._process_instrument_data(): "
                                                                        "Size of data record is not the length of an instrument data record"))

            elif data_type is dataTypeEnum.CONTROL:

                log.debug("PhsenAbcdefDclParser._process_instrument_data(): "
                          "Record ID is CONTROL")

                ## Per the IDD, if the candidate data is not the proper size, throw a recoverable exception
                if len(working_record) == control_record_length_without_voltage_battery or \
                                len(working_record) == control_record_length_with_voltage_battery:

                    ## Create particle mule (to be used later to create the metadata particle)
                    particle = self._extract_sample(self._metadata_particle_class, None, particle_data, self.latest_dcl_time)

                    self.result_particle_list.append((particle, copy.copy(self._read_state)))
                else:
                    self._exception_callback(RecoverableSampleException("PhsenAbcdefDclParser._process_instrument_data(): "
                                                                "Size of data record is not the length of a control data record"))
        else:
            log.debug("PhsenAbcdefDclParser._process_instrument_data(): "
                      "Record is neither instrument or control, throwing exception")

            self._exception_callback(RecoverableSampleException("PhsenAbcdefDclParser._process_instrument_data(): "
                                                                "Data Type is neither Control or Instrument"))

    def _determine_data_type(self, working_record):

        ## strip out the type from the working record
        type_ascii_hex = working_record[5:7]
        ## convert to a 16 bit unsigned int
        type_int = int(type_ascii_hex, 16)

        if type_int == 10:
            log.debug("PhsenAbcdefDclParser._determine_data_type(): dataType is %s, INSTRUMENT", type_int)
            return dataTypeEnum.INSTRUMENT
        elif 128 <= type_int <= 255:
            log.debug("PhsenAbcdefDclParser._determine_data_type(): dataType is %s, CONTROL", type_int)
            return dataTypeEnum.CONTROL
        else:
            log.debug("PhsenAbcdefDclParser._determine_data_type(): dataType is %s, UNKNOWN", type_int)
            return dataTypeEnum.UNKNOWN

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        self.result_particle_list = []

        # collect the non-data from the file
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        # collect the data from the file
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)

        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:

            log.info("PhsenAbcdefDclParser.parse_chunks(): ##############################")
            log.info("PhsenAbcdefDclParser.parse_chunks(): Chunk = %s", chunk)

            self._increment_state(len(chunk))

            ## if the chunk has no data, ie only whitespace, it should be ignored
            if self._whitespace_regex.match(chunk):

                log.debug("PhsenAbcdefDclParser.parse_chunks(): Only whitespace detected in record. Ignoring.")

            ## chunk contains some data, parse the chunk
            else:
                is_bracket_present = self._find(r'\[', chunk)

                ## check for a * in this chunk, signaling the start of a new record
                is_star_present = self._find(r'\*', chunk)

                ## if this chunk has a bracket it should not be processed...
                if is_bracket_present:
                    log.debug("PhsenAbcdefDclParser.parse_chunks(): ===!!!=== LINE HAS A BRACKET ===!!!===")

                    ## if the chunk has a bracket AND data has been previously parsed...
                    if self.in_record:
                        log.debug("PhsenAbcdefDclParser.parse_chunks(): ===!!!=== STAR FLAG IS SET ===!!!===")

                        ## if the aggregate working record is not empty,
                        ## the working record is complete and a particle can now be created
                        if len(self.working_record) > 0:
                            log.debug("PhsenAbcdefDclParser.parse_chunks(): "
                                      "===!!!=== Working_Record is Non-Zero - %s ===!!!===", len(self.working_record))

                            ## PROCESS WORKING STRING TO CREATE A PARTICLE
                            self._process_instrument_data(self.working_record)

                            ## clear out the working record (the last string that was being built)
                            log.debug(" ## PhsenAbcdefDclParser.parse_chunks(): "
                                      "### ### ### CLEARING WORKING RECORD ### ### ###")

                            self.working_record = ""

                    ## if the chunk has a bracket and data has NOT been previously parsed,
                    ## do nothing (this is one of the first chunks seen by this parser)
                    else:
                        log.debug("PhsenAbcdefDclParser.parse_chunks(): ===!!!=== STAR FLAG NOT SET ===!!!===")

                ## if the chunk does NOT have a bracket, it contains instrument or control log data
                else:
                    log.debug("PhsenAbcdefDclParser.parse_chunks(): ===!!!=== LINE DOES NOT HAVE A BRACKET ===!!!===")

                    ## if the * character is present this is the first piece of data for an instrument or control log
                    if is_star_present:
                        log.debug("PhsenAbcdefDclParser.parse_chunks(): ===!!!=== LINE HAS A STAR ===!!!===")

                        ## strip the trailing newlines and carriage returns from the string
                        ## strip the leading DCL data/time data from the string
                        ## save off the DLC time
                        stripped_logfile_line = self._strip_logfile_line(chunk)

                        ## append time_stripped_logfile_line to working_record
                        self.working_record += stripped_logfile_line

                        ## this is the first time a * has been found, set a flag
                        self.in_record = True

                    ## if there is no * character in this line,
                    ## it is the next part of an instrument or control log file
                    ## and will be appended to the previous portion of the log file
                    else:
                        log.debug("PhsenAbcdefDclParser.parse_chunks(): ===!!!=== LINE DOES NOT HAVE A STAR ===!!!===")

                        ## strip the trailing newlines and carriage returns from the string
                        ## strip the leading DCL data/time data from the string
                        ## save off the DLC time
                        stripped_logfile_line = self._strip_logfile_line(chunk)

                        ## append time_stripped_logfile_line to working_record
                        self.working_record += stripped_logfile_line

            # collect the non-data from the file
            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            # collect the data from the file
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)

            self.handle_non_data(non_data, non_end, start)

        # publish the results
        return self.result_particle_list

    def handle_non_data(self, non_data, non_end, start):
        """
        handle data in the non_data chunker queue
        @param non_data data in the non data chunker queue
        @param non_end ending index of the non_data chunk
        @param start start index of the next data chunk
        """

        #log.debug("PhsenAbcdefDclParser.handle_non_data(): non_data= %s", non_data)

        # we can get non_data after our current chunk, check that this chunk is before that chunk
        if non_data is not None and non_end <= start:
            log.error("Found %d bytes of unexpected non-data:%s", len(non_data), non_data)
            log.warn("PhsenAbcdefDclParser.handle_non_data(): "
                     "Found data in un-expected non-data from the chunker: %s", non_data)
            self._exception_callback(UnexpectedDataException("Found %d bytes of un-expected non-data:%s" %
                                                            (len(non_data), non_data)))
            self._increment_state(len(non_data))
