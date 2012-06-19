"""
@file mi/idk/da_server.py
@author Bill French
@brief Main script class for running the direct access process
"""

import os
import re
import time
import gevent

from mi.core.log import log

from mi.idk.instrument_agent_client import InstrumentAgentClient
from mi.idk.comm_config import CommConfig
from mi.idk.config import Config

from interface.objects import AgentCommand

from ion.agents.instrument.instrument_agent import InstrumentAgentState
from ion.agents.instrument.driver_process import DriverProcessType
from ion.agents.instrument.direct_access.direct_access_server import DirectAccessTypes
from ion.agents.port.logger_process import EthernetDeviceLogger

TIMEOUT = 600

class DirectAccessServer():
    """
    Main class for running the start driver process.
    """
    port_agent = None
    instrument_agent_manager = None
    instrument_agent_client = None

    def __del__(self):
        log.info("tearing down agents and containers")

        log.debug("killing the capability container")
        if self.instrument_agent_manager:
            self.instrument_agent_manager.stop_container()

        log.debug("killing the port agent")
        if self.port_agent:
            self.port_agent.stop()


    def start_container(self):
        self.init_comm_config()
        self.init_port_agent()
        self.instrument_agent_manager = InstrumentAgentClient();
        self.instrument_agent_manager.start_container()
        self.init_instrument_agent_client()

    def comm_config_file(self):
        """
        @brief Return the path the the driver comm config yaml file.
        @return if comm_config.yml exists return the full path
        """
        repo_dir = Config().get('working_repo')
        driver_path = 'mi.instrument.seabird.sbe37smb.example.driver'
        p = re.compile('\.')
        driver_path = p.sub('/', driver_path)
        abs_path = "%s/%s/%s" % (repo_dir, os.path.dirname(driver_path), CommConfig.config_filename())

        log.debug(abs_path)
        return abs_path

    def init_comm_config(self):
        """
        @brief Create the comm config object by reading the comm_config.yml file.
        """
        log.info("Initialize comm config")
        config_file = self.comm_config_file()

        log.debug( " -- reading comm config from: %s" % config_file )
        if not os.path.exists(config_file):
            raise TestNoCommConfig(msg="Missing comm config.  Try running start_driver or switch_driver")

        self.comm_config = CommConfig.get_config_from_file(config_file)

    def init_port_agent(self):
        """
        @brief Launch the driver process and driver client.  This is used in the
        integration and qualification tests.  The port agent abstracts the physical
        interface with the instrument.
        @retval return the pid to the logger process
        """
        log.info("Startup Port Agent")

        # Create port agent object.
        this_pid = os.getpid()
        log.debug( " -- our pid: %d" % this_pid)
        log.debug( " -- address: %s, port: %s" % (self.comm_config.device_addr, self.comm_config.device_port))

        # Working dir and delim are hard coded here because this launch process
        # will change with the new port agent.
        self.port_agent = EthernetDeviceLogger.launch_process(self.comm_config.device_addr,
            self.comm_config.device_port,
            "/tmp",
            ['<<', '>>'],
            this_pid)


        log.debug( " Port agent object created" )

        start_time = time.time()
        expire_time = start_time + 20
        pid = self.port_agent.get_pid()
        while not pid:
            time.sleep(.1)
            pid = self.port_agent.get_pid()
            if time.time() > expire_time:
                log.error("!!!! Failed to start Port Agent !!!!")
                raise PortAgentTimeout()

        port = self.port_agent.get_port()

        start_time = time.time()
        expire_time = start_time + 20
        while not port:
            time.sleep(.1)
            port = self.port_agent.get_port()
            if time.time() > expire_time:
                log.error("!!!! Port Agent could not bind to port !!!!")
                raise PortAgentTimeout()

        log.info('Started port agent pid %s listening at port %s' % (pid, port))
        return port

    def stop_port_agent(self):
        """
        Stop the port agent.
        """
        if self.port_agent:
            pid = self.port_agent.get_pid()
            if pid:
                log.info('Stopping pagent pid %i' % pid)
                # self.port_agent.stop() # BROKE
            else:
                log.info('No port agent running.')

    def init_instrument_agent_client(self):
        log.info("Start Instrument Agent Client")

        # Port config
        port = self.port_agent.get_port()
        port_config = {
            'addr': 'localhost',
            'port': port
        }

        # Driver config
        driver_config = {
            #'dvr_mod' : 'mi.instrument.seabird.sbe37smb.example.driver',
            #'dvr_cls' : 'InstrumentDriver',
            'dvr_mod' : 'mi.instrument.seabird.sbe37smb.ooicore.driver',
            'dvr_cls' : 'SBE37Driver',

            'process_type' : DriverProcessType.PYTHON_MODULE,

            'workdir' : "/tmp",
            'comms_config' : port_config,
        }

        # Create agent config.
        agent_config = {
            'driver_config' : driver_config,
            'stream_config' : {},
            'agent'         : {'resource_id': 'da_idk_run'},
            'test_mode' : True  ## Enable a poison pill. If the spawning process dies
            ## shutdown the daemon process.
        }

        # Start instrument agent client.
        self.instrument_agent_manager.start_client(
            name='agent_name',
            module='ion.agents.instrument.instrument_agent',
            cls='InstrumentAgent',
            config=agent_config,
            resource_id='da_idk_run',
            deploy_file='res/deploy/r2deploy.yml',
        )

        self.instrument_agent_client = self.instrument_agent_manager.instrument_agent_client

    def _start_da(self, type):
        self.start_container()

        log.info("--- Starting DA server ---")

        cmd = AgentCommand(command='power_down')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)

        cmd = AgentCommand(command='power_up')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)

        cmd = AgentCommand(command='initialize')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)

        cmd = AgentCommand(command='go_active')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)

        cmd = AgentCommand(command='run')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)
        cmd = AgentCommand(command='get_current_state')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)

        cmd = AgentCommand(command='go_direct_access',
            kwargs={'session_type': type,
                    'session_timeout': TIMEOUT,
                    'inactivity_timeout': TIMEOUT})
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)

        ip_address = retval.result['ip_address']
        port= retval.result['port']
        token= retval.result['token']

        cmd = AgentCommand(command='get_current_state')
        retval = self.instrument_agent_client.execute_agent(cmd)
        log.debug("retval: %s", retval)

        ia_state = retval.result
        log.debug("IA State: %s", ia_state)

        if ia_state == InstrumentAgentState.DIRECT_ACCESS:
            print "Direct access server started, IP: %s Port: %s" % (ip_address, port)
            if token:
                print "Token: %s" % token

            while ia_state == InstrumentAgentState.DIRECT_ACCESS:
                cmd = AgentCommand(command='get_current_state')
                retval = self.instrument_agent_client.execute_agent(cmd)

                ia_state = retval.result
                gevent.sleep(.1)

        else:
            log.error("Failed to start DA server")




    def start_telnet_server(self):
        """
        @brief Run the telnet server
        """
        self._start_da(DirectAccessTypes.telnet)

    def start_vps_server(self):
        """
        @brief run the vps server
        """
        self._start_da(DirectAccessTypes.vsp)
