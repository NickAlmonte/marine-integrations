#!/usr/bin/env python

"""
@package mi.dataset.parser.phsen_abcdef_dcl
@file marine-integrations/mi/dataset/parser/phsen_abcdef_dclpy
@author Jeremy Amundson
@brief Parser for the phsen_abcdef_dcl dataset driver
Release notes:

initial release
"""

__author__ = 'Jeremy Amundson'
__license__ = 'Apache 2.0'

import copy
import re
import time

from functools import partial
from dateutil import parser

from mi.core.log import get_logger
log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, DataParticleKey, DataParticleValue
from mi.core.exceptions import SampleException, DatasetParserException, UnexpectedDataException, \
    ConfigurationException, RecoverableSampleException
from mi.dataset.dataset_parser import BufferLoadingParser
from mi.core.instrument.chunker import StringChunker
from mi.dataset.dataset_driver import DataSetDriverConfigKeys

# ASCII File with records separated by carriage return, newline, or carriage return - line feed
NEW_LINE = r'[\n\r]+'
DATE_REGEX = r'(\d{4}/\d{2}/\d{2})'
TIME_REGEX = r'(\d{2}:\d{2}:\d{2}:\d{3})'

START_OF_LINE_REGEX = DATE_REGEX + '\w'
START_OF_LINE_REGEX += TIME_REGEX + '\w'

START_OF_RECORD_REGEX = START_OF_LINE_REGEX + '\*\w{4}(\w{2})'
START_OF_RECORD_MATCHER = re.compile(START_OF_LINE_REGEX)

DCL_REGEX = START_OF_LINE_REGEX
DCL_REGEX += r'\[.+\]:.*' + NEW_LINE  # [<IDENTIFIER>]:<ASCII TEXT><Line Break>

FLAGS ='flags'

METADATA_PARTICLE_CLASS_KEY = 'metadata_particle_class'
# The key for the data particle class
DATA_PARTICLE_CLASS_KEY = 'data_particle_class'

class DataParticleType(BaseEnum):
    """
    The data particle types that a phsen_abcdef_dcl parser could generate
    """
    METADATA_RECOVERED = 'phsen_abcdef_dcl_metadata_recovered'
    INSTRUMENT_RECOVERED = 'phsen_abcdef_dcl_instrument_recovered'
    METADATA_TELEMETERED = 'phsen_abcdef_dcl_metadata'
    INSTRUMENT_TELEMETERED = 'phsen_abcdef_dcl_instrument'

def time_to_unix_time(sec_since_1904):
    """
    Convert between seconds since 1904 into unix time (epoch time)
    @param sec_since_1904 ascii string of seconds since Jan 1 1904
    @retval sec_since_1970 epoch time
    """
    local_dt_1904 = parser.parse("1904-01-01T00:00:00.00Z")
    elapse_1904 = float(local_dt_1904.strftime("%s.%f"))
    sec_since_1970 = sec_since_1904 + elapse_1904 - time.timezone
    return sec_since_1970

def gen_hex_ASCII_values(hex_text, start=0, stop=None, segment_length=2):
    """
    returns a generator that returns segments of pure hex-ascii text as an integer.
    Is called by the data particles _if_checksum() and _build_parsed_values() methods

    the yield statement means that this function is a generator. When called it returns
    a generator object that, when iterated through, returns the next value specified by yield
    """

    if stop is None:
        stop = len(hex_text)
    for segment in range(start, stop, segment_length):
        yield int(hex_text[segment:segment+segment_length], 16)

class StateKey(BaseEnum):
    POSITION = 'position'  # hold the current file position
    START_OF_DATA = 'start_of_data'

class PhsenAbcdefDclDataParticleKey(BaseEnum):

    CONTROLLER_TIMESTAMP = ' dcl_controller_timestamp'
    UNIQUE_ID = 'unique_id'
    RECORD_TYPE = 'record_type'
    RECORD_TIME = 'record_time'
    VOLTAGE_BATTERY = 'voltage_battery'
    PASSED_CHECKSUM = 'passed_checksum'


class PhsenAbcdefDclInstrumentDataParticleKey(PhsenAbcdefDclDataParticleKey):
    THERMISTOR_START = 'thermistor_start'
    REFERENCE_LIGHT_MEASUREMENTS = 'reference_light_measurements'
    LIGHT_MEASUREMENTS = 'light_measurements'
    THERMISTOR_END = 'thermistor_end'

PH_REGEX = START_OF_RECORD_REGEX
PH_REGEX += r'(?P<' + PhsenAbcdefDclDataParticleKey.UNIQUE_ID + '>\x{2})'  # ID
PH_REGEX += r'E7'  # length
PH_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.RECORD_TYPE + '>0A)'  # type
PH_REGEX += r'(?P<' + PhsenAbcdefDclDataParticleKey.RECORD_TIME + '>\x{8})(?:' + \
            NEW_LINE + START_OF_LINE_REGEX + ')?'  # time
PH_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.THERMISTOR_START + '>\x{4})(?:' + \
            NEW_LINE + START_OF_LINE_REGEX + ')?'  # starting thermistor
PH_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS + \
            '>(\x{16})(' + NEW_LINE + START_OF_LINE_REGEX + ')?{4})'  # reference light measurements
# The controller timestamp is the last timestamp in the record, which is inside the rlm,
# therefore they must be retrieved at the same time. Note that the capture group for the timestamp
# only captures the last match
PH_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.LIGHT_MEASUREMENTS + \
            '>(( \x{16})' + NEW_LINE + '(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.CONTROLLER_TIMESTAMP + '>'\
            + START_OF_LINE_REGEX + ')?){23})'  # light measurements and controller timestamp
PH_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.VOLTAGE_BATTERY + '\x{4})'  # battery voltage
PH_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.THERMISTOR_END + '\x{4})'  # thermistor end
PH_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.PASSED_CHECKSUM + '\x{2})'  # passed checksum

PH_MATCHER = re.compile(PH_REGEX)


class PhsenAbcdefDclInstrumentDataParticle(DataParticle):
    """
    Class for parsing data from the phsen_abcdef ph data set
    """

    _encoding_rules = [(PhsenAbcdefDclInstrumentDataParticleKey.CONTROLLER_TIMESTAMP, str),
                       (PhsenAbcdefDclInstrumentDataParticleKey.UNIQUE_ID, int),
                       (PhsenAbcdefDclInstrumentDataParticleKey.RECORD_TYPE, int),
                       (PhsenAbcdefDclInstrumentDataParticleKey.RECORD_TIME, int),
                       (PhsenAbcdefDclInstrumentDataParticleKey.THERMISTOR_START, int),
                       (PhsenAbcdefDclInstrumentDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS, list),
                       (PhsenAbcdefDclInstrumentDataParticleKey.LIGHT_MEASUREMENTS, list),
                       (PhsenAbcdefDclInstrumentDataParticleKey.VOLTAGE_BATTERY, int),
                       (PhsenAbcdefDclInstrumentDataParticleKey.PASSED_CHECKSUM, int)]

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(PhsenAbcdefDclInstrumentDataParticle, self).__init__(raw_data,
                                                                   port_timestamp,
                                                                   internal_timestamp,
                                                                   preferred_timestamp,
                                                                   quality_flag,
                                                                   new_sequence)

        sec_since_1904 = int(self.raw_data[PhsenAbcdefDclDataParticleKey.CONTROLLER_TIMESTAMP])
        unix_time = time_to_unix_time(sec_since_1904)
        self.set_internal_timestamp(unix_time=unix_time)


    def _clean_rows(self, cluttered_data):
        """
        removes the new-lines and text from the matching data, leaving only ASCII-hex
        """

        return re.sub(NEW_LINE + START_OF_LINE_REGEX, '', cluttered_data)

    def _if_checksum(self, passed_checksum):
        """
        verifies the checksum, which is the low byte of the sum of all the hex-Ascii bytes (2 characters)
        excluding ID and checksum
        """

        cleaned_data = self._clean_rows(self.raw_data.group[0])
        gen = gen_hex_ASCII_values(cleaned_data, start=2, stop=len(cleaned_data)-2)
        calculated_checksum = 0

        for i in gen:
            calculated_checksum += i
        calculated_checksum &= (2**8-1)  # and checksum with 11111111 in order to get last byte
        if passed_checksum == calculated_checksum:
            return True
        else:
            return False

    def _build_parsed_values(self):
        """
        Take a record in the ph data format and turn it into
        a particle with the instrument tag.
        @throws SampleException If there is a problem with sample creation
        """

        result = []

        try:
        # value[1] stores the DataParticleType and value[2] stores the data type
            for value in self._encoding_rules:
                if value[1] == PhsenAbcdefDclInstrumentDataParticleKey.REFERENCE_LIGHT_MEASUREMENTS or \
                            value[1] == PhsenAbcdefDclInstrumentDataParticleKey.LIGHT_MEASUREMENTS:

                    cleaned_data = self._clean_rows(self.raw_data.group(value[1]))
                    int_list = []
                    gen = gen_hex_ASCII_values(cleaned_data, segment_length=4)

                    [int_list.append(n) for n in gen]
                    result.append(self._encode_value(value[1], int_list, value[2]))
                elif value[1] == PhsenAbcdefDclDataParticleKey.PASSED_CHECKSUM:
                    self._encode_value(value[1], self._if_checksum(self.raw_data.group(value[1])), value[2])
                else:
                    if value[2] is not int:
                        result.append(self._encode_value(value[1], self.raw_data.group(value[1]), value(2)))
                    else:
                        decimal_number = int(self.raw_data.group(value[1]), 16)
                        result.append(self._encode_value(value[1], decimal_number, value(2)))
        except (ValueError, TypeError, IndexError) as ex:
            log.warn("Exception when building parsed values")
            raise RecoverableSampleException("Error (%s) while decoding parameters in data: %s" % (ex, self.raw_data))
        return result


class PhsenAbcdefDclInstrumentTelemeteredDataParticle(DataParticle):
    _data_particle_type = DataParticleType.INSTRUMENT_TELEMETERED


class PhsenAbcdefDclInstrumentRecoveredDataParticle(DataParticle):
    _data_particle_type = DataParticleType.METADATA_RECOVERED


class PhsenAbcdefDclMetadataDataParticleKey(PhsenAbcdefDclDataParticleKey):
    CLOCK_ACTIVE = 'clock_active'
    RECORDING_ACTIVE = 'recording_active'
    RECORD_END_TIME = 'record_end_on_time'
    RECORD_MEMORY_FULL = 'record_memory_full'
    RECORD_END_ON_ERROR = 'record_end_on_error'
    DATA_DOWNLOAD_OK = 'data_download_ok'
    FLASH_MEMORY_OPEN = 'flash_memory_open'
    BATTERY_LOW_PRESTART = 'battery_low_prestart'
    BATTERY_LOW_MEASUREMENT = 'battery_low_measurement'
    BATTERY_LOW_BLANK = 'battery_low_blank'
    BATTERY_LOW_EXTERNAL = 'battery_low_external'
    EXTERNAL_DEVICE1_FAULT = 'external_device1_fault'
    EXTERNAL_DEVICE2_FAULT = 'external_device2_fault'
    EXTERNAL_DEVICE3_FAULT = 'external_device3_fault'
    FLASH_ERASED = 'flash_erased'
    POWER_ON_INVALID = 'power_on_invalid'
    NUM_DATA_RECORDS = 'num_data_records'
    NUM_ERROR_RECORDS = 'num_error_records'
    NUM_BYTES_STORED = 'num_bytes_stored'

CONTROL_REGEX = START_OF_RECORD_REGEX
CONTROL_REGEX += r'(?P<' + PhsenAbcdefDclDataParticleKey.UNIQUE_ID + '>\x{2})'  # ID
CONTROL_REGEX += r'\x{2}'  # length
CONTROL_REGEX += r'(?P<' + PhsenAbcdefDclInstrumentDataParticleKey.RECORD_TYPE + '>\x{2})'  # type
CONTROL_REGEX += r'(?P<' + PhsenAbcdefDclDataParticleKey.RECORD_TIME + '>\x{8})(?:' + \
                 NEW_LINE + START_OF_LINE_REGEX + ')?'  # time
CONTROL_REGEX += r'(?P<' + FLAGS + '>\x{4}(' + NEW_LINE + START_OF_LINE_REGEX + ')?'  # flags
CONTROL_REGEX += r'(?P<' + PhsenAbcdefDclMetadataDataParticleKey.NUM_DATA_RECORDS + '>\x{6})(' + \
                 NEW_LINE + START_OF_LINE_REGEX + ')?'  # #of records
CONTROL_REGEX += r'(?P<' + PhsenAbcdefDclMetadataDataParticleKey.NUM_ERROR_RECORDS + '>\x{6})(' + \
                 NEW_LINE + START_OF_LINE_REGEX + ')?'  # #of Errors
CONTROL_REGEX += r'(?P<' + PhsenAbcdefDclMetadataDataParticleKey.NUM_BYTES_STORED + '>\x{4})(' + \
                 NEW_LINE + START_OF_LINE_REGEX + ')?'  # #of Bytes
CONTROL_REGEX += r'(?P<extra>\x{6})?(?:' + NEW_LINE + START_OF_LINE_REGEX + ')?'  # #of records
CONTROL_REGEX += r'(?P<' + PhsenAbcdefDclMetadataDataParticleKey.PASSED_CHECKSUM + '>\x{2})(' + \
                 NEW_LINE + START_OF_LINE_REGEX + ')?'  # #of records

CONTROL_MATCHER = re.compile(CONTROL_REGEX)


class PhsenAbcdefDclMetadataDataParticle(DataParticle):
    """
    Class for parsing data from the phsen_abcdef control data set
    """

    _encoding_rules = [(PhsenAbcdefDclMetadataDataParticleKey.CONTROLLER_TIMESTAMP, str),
                       (PhsenAbcdefDclMetadataDataParticleKey.UNIQUE_ID, int),
                       (PhsenAbcdefDclMetadataDataParticleKey.RECORD_TYPE, int),
                       (PhsenAbcdefDclMetadataDataParticleKey.RECORD_TIME, int),
                       (FLAGS, int),
                       (PhsenAbcdefDclMetadataDataParticleKey.NUM_DATA_RECORDS, int),
                       (PhsenAbcdefDclMetadataDataParticleKey.NUM_ERROR_RECORDS, int),
                       (PhsenAbcdefDclMetadataDataParticleKey.NUM_BYTES_STORED, int),
                       (PhsenAbcdefDclMetadataDataParticleKey.VOLTAGE_BATTERY, int),
                       (PhsenAbcdefDclMetadataDataParticleKey.PASSED_CHECKSUM, int)]

    _flag_encoding_rules = [(PhsenAbcdefDclMetadataDataParticleKey.CLOCK_ACTIVE, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.RECORDING_ACTIVE, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.RECORD_END_TIME, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.RECORD_MEMORY_FULL, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.RECORD_END_ON_ERROR, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.DATA_DOWNLOAD_OK, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.FLASH_MEMORY_OPEN, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_PRESTART, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_MEASUREMENT, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_BLANK, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.BATTERY_LOW_EXTERNAL, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.EXTERNAL_DEVICE1_FAULT, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.EXTERNAL_DEVICE2_FAULT, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.EXTERNAL_DEVICE3_FAULT, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.FLASH_ERASED, int),
                            (PhsenAbcdefDclMetadataDataParticleKey.POWER_ON_INVALID, int)]

    def __init__(self, raw_data,
                 port_timestamp=None,
                 internal_timestamp=None,
                 preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
                 quality_flag=DataParticleValue.OK,
                 new_sequence=None):
        super(PhsenAbcdefDclMetadataDataParticle, self).__init__(raw_data,
                                                                 port_timestamp,
                                                                 internal_timestamp,
                                                                 preferred_timestamp,
                                                                 quality_flag,
                                                                 new_sequence)

        # use the timestamp from the sio header as internal timestamp
        sec_since_1904 = int(self.raw_data.group(PhsenAbcdefDclInstrumentDataParticleKey.CONTROLLER_TIMESTAMP))
        unix_time = time_to_unix_time(sec_since_1904)
        self.set_internal_timestamp(unix_time=unix_time)

    def _read_flags(self, text):

        flags = int(text, 16)
        for n in range(16):
            result = flags & 1
            flags >>= 1
            yield result

    def _build_parsed_values(self):
        """
        Take a record in the control data format and turn it into
        a particle with the metadata tag.
        @throws SampleException If there is a problem with sample creation
        """

        result = []
        try:
            for value in self._encoding_rules:
                if value[1] == FLAGS:
                    flags = int(self._read_flags(self.raw_data.group(FLAGS)), 16)
                    for bit_value in self._flag_encoding_rules:

                        result.append(self._encode_value(bit_value, flags & 1, value[2]))
                        flags >>= 1
                elif value[1] == PhsenAbcdefDclMetadataDataParticleKey.CONTROLLER_TIMESTAMP:
                    n = -1
                    found = False
                    while not found:
                        if re.search(self.raw_data.group(n), START_OF_LINE_REGEX):
                            result.append(self._encode_value(value[1], self.raw_data.group(n), value[2]))
                            found = True
                        else:
                            n -= 1
                elif value[1] == PhsenAbcdefDclMetadataDataParticleKey.VOLTAGE_BATTERY:
                    if self.raw_data.group(PhsenAbcdefDclMetadataDataParticleKey.RECORD_TYPE) == 'C0' or\
                       self.raw_data.group(PhsenAbcdefDclMetadataDataParticleKey.RECORD_TYPE) == 'C1':
                        result.append(self._encode_value(value[1], self.raw_data(value[1]), int))
                    else:
                        result.append({value[1]: None})
                else:
                    if value[2] is not int:
                        result.append(self._encode_value(value[1], self.raw_data.group(value[1]), value(2)))
                    else:
                        decimal_number = int(self.raw_data.group(value[1]), 16)
                        result.append(self._encode_value(value[1], decimal_number, value(2)))
        except (ValueError, TypeError, IndexError) as ex:
            log.warn("Exception when building parsed values")
            raise RecoverableSampleException("Error (%s) while decoding parameters in data: %s" % (ex, self.raw_data))
        return result


class PhsenAbcdefDclMetadataTelemeteredDataParticle(PhsenAbcdefDclMetadataDataParticle):
    _data_particle_type = DataParticleType.METADATA_TELEMETERED


class PhsenAbcdefDclMetadataRecoveredDataParticle(PhsenAbcdefDclMetadataDataParticle):
    _data_particle_type = DataParticleType.METADATA_RECOVERED


class PhsenAbcdefDclParser(BufferLoadingParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):

        # noinspection PyArgumentList,PyArgumentList,PyArgumentList,PyArgumentList,PyArgumentList
        super(PhsenAbcdefDclParser, self).__init__(config, stream_handle, state,
                                                            partial(StringChunker.regex_sieve_function,
                                                                    regex_list=[START_OF_RECORD_REGEX]),
                                                            state_callback,
                                                            publish_callback,
                                                            exception_callback,
                                                            *args,
                                                            **kwargs)

        self._read_state = {StateKey.POSITION: 0, StateKey.START_OF_DATA: False}

        if state:
            self.set_state(self._state)

        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
            # Set the metadata and data particle classes to be used later
            if METADATA_PARTICLE_CLASS_KEY in particle_classes_dict and \
                            DATA_PARTICLE_CLASS_KEY in particle_classes_dict:
                self._data_particle_class = particle_classes_dict.get(DATA_PARTICLE_CLASS_KEY)
                self._metadata_particle_class = particle_classes_dict.get(METADATA_PARTICLE_CLASS_KEY)
            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
        else:
            log.warning('Configuration missing particle classes dict')
            raise ConfigurationException('Configuration missing particle classes dict')

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
        self._stream_handle.seek(state_obj[StateKey.POSITION])

    def _increment_state(self, increment):
        """
        Increment the parser state
        @param increment The amount to increment the position by
        """
        self._read_state[StateKey.POSITION] += increment

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
        self.handle_non_data(non_data, non_end, start)

        while chunk is not None:

            log.info("Chunk: ****%s****", chunk)

            self._increment_state(len(chunk))
            if not self._read_state[StateKey.START_OF_DATA]:

                log.info("In if not self._read_state[StateKey.START_OF_DATA]")

                start_of_data = START_OF_RECORD_MATCHER.match(chunk)
                if start_of_data:
                    self._read_state[StateKey.START_OF_DATA] = True
            else:

                # if this chunk is a data match process it, otherwise it is a metadata record which is ignored
                ph_match = PH_MATCHER.match(chunk)
                control_match = CONTROL_MATCHER.match(chunk)

                if ph_match:

                    # particle-ize the data block received, return the record
                    sample = self._extract_sample(self._data_particle_class,
                                                  None, ph_match, None)

                    if sample:
                        # create particle
                        result_particles.append((sample, copy.copy(self._read_state)))

                elif control_match:

                   # particle-ize the data block received, return the record
                    sample = self._extract_sample(self._metadata_particle_class,
                                                  None, control_match, None)

                    if sample:
                        # create particle
                        result_particles.append((sample, copy.copy(self._read_state)))
                elif (START_OF_RECORD_MATCHER.match(chunk)).group(1) == '05' or\
                     (START_OF_RECORD_MATCHER.match(chunk)).group(1) == '04':
                    log.error('Co2 record found, should not be in data')
                    raise RecoverableSampleException('CO2 record found')
                else:
                    # Non-PH or non-Control type or REGEX not matching PH/Control record
                    error_str = 'REGEX does not match PH or Control record %s'
                    log.warn(error_str, chunk)
                    self._exception_callback(SampleException(error_str % chunk))

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles

    def handle_non_data(self, non_data, non_end, start):
        """
        handle data in the non_data chunker queue
        @param non_data data in the non data chunker queue
        @param non_end ending index of the non_data chunk
        @param start start index of the next data chunk
        """
        # we can get non_data after our current chunk, check that this chunk is before that chunk
        if non_data is not None and non_end <= start:
            log.error("Found %d bytes of unexpected non-data:%s", len(non_data), non_data)
            self._exception_callback(UnexpectedDataException("Found %d bytes of un-expected non-data:%s" %
                                                            (len(non_data), non_data)))
            self._increment_state(len(non_data))
