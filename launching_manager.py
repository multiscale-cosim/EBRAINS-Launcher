# ------------------------------------------------------------------------------
#  Copyright 2020 Forschungszentrum Jülich GmbH and Aix-Marseille Université
# "Licensed to the Apache Software Foundation (ASF) under one or more contributor
#  license agreements; and to You under the Apache License, Version 2.0. "
#
# Forschungszentrum Jülich
#  Institute: Institute for Advanced Simulation (IAS)
#    Section: Jülich Supercomputing Centre (JSC)
#   Division: High Performance Computing in Neuroscience
# Laboratory: Simulation Laboratory Neuroscience
#       Team: Multi-scale Simulation and Design
#
# ------------------------------------------------------------------------------
import os
import multiprocessing
import pickle
import base64

# Co-Simulator's imports
from EBRAINS_Launcher.common.utils.common_utils import strtobool

from EBRAINS_ConfigManager.workflow_configurations_manager.xml_parsers import constants
from EBRAINS_ConfigManager.workflow_configurations_manager.xml_parsers import enums
from EBRAINS_ConfigManager.workflow_configurations_manager.xml_parsers.variables import CO_SIM_EXECUTION_ENVIRONMENT
from EBRAINS_RichEndpoint.launcher import Launcher
from EBRAINS_RichEndpoint.launcher_hpc import LauncherHPC
from EBRAINS_RichEndpoint.application_companion.common_enums import Response


class LaunchingManager(object):
    """
    1. Groups the actions and their launching methods based on events
    such as SEQUENTIAL or CONCURRENT
    2. Performs SEQUENTIAL actions
    3. Performs CONCURRENT actions
    """

    def __init__(self,
                 action_plan_dict,
                 action_plan_variables_dict,
                 action_plan_parameters_dict,
                 actions_popen_args_dict,
                 log_settings,
                 configurations_manager,
                 actions_sci_params_dict,
                 is_interactive,
                 communication_settings_dict=None,
                 services_deployment_dict=None,):
        # initialize logger with uniform settings
        self._logger_settings = log_settings
        self._configurations_manager = configurations_manager
        self.__logger = self._configurations_manager.load_log_configurations(
            name=__name__,
            log_configurations=self._logger_settings)
        # The action plan to be carried out
        self.__action_plan_dict = action_plan_dict
        # CO_SIM_ variables inherent to the action plan
        self.__action_plan_variables_dict = action_plan_variables_dict
        # CO_SIM_ parameters inherent to the action plan
        self.__action_plan_parameters_dict = action_plan_parameters_dict
        # Popen arguments keyed by action XML IDs
        self.__actions_popen_args_dict = actions_popen_args_dict
        # Dictionary containing the XML PATH+FILENAME of Scientific Parameters by Action ID
        self.__actions_sci_params_dict = actions_sci_params_dict
        # XML filenames from <action_xml> element of the action plan XML file
        self.__actions_xml_filenames_dict = {}
        # Dictionary containing the communication settings to be used by ZMQ or another communication framework/library
        # e.g. default port, ports range, communication pattern, pace, delay, resilience approach, etc.
        self.__communication_settings_dict = communication_settings_dict
        #
        # Co-Sim nodes arrangement for deploying
        self.__services_deployment_dict = services_deployment_dict
        #
        # Mapped action plan, actions grouped by events
        self.__launching_strategy_dict = {}
        # the number of spawner to be started
        self.__maximum_number_actions_found = 0
        # Joinable queue to trigger spawning actions processes
        self.__actions_to_be_carried_out_jq = multiprocessing.JoinableQueue()
        # Queue where the actions return codes will be placed
        self.__actions_return_codes_q = multiprocessing.Queue()
        self.__launching_manager_PID = os.getpid()
        self.__stopping_event = multiprocessing.Event()
        self.__is_execution_environment_hpc = False  # by default the running environment is considered "Local"
        # flag whether the steering is interactive
        self.__is_interactive =  is_interactive

        # set the defulat settings for resource usage monitoring
        self.__is_monitoring_enabled = False
        # overwirte the default settings from XML configurations
        try:
            self.__is_monitoring_enabled = strtobool(self.__action_plan_parameters_dict.get("CO_SIM_ENABLE_MONITORING"))
        except Exception as e:
            # This could happen when the value is not set in XML file properly
            # Now, fall back to default settings
            self.__log_exception(
                exception=e,
                message="resource usage monitoring settings could not be set from XML")
            # self.__logger.critical("resource usage monitoring settings could not be set from XML")
            # self.__logger.exception(f"got the exception: {e}")
            self.__logger.critical("falling back to default settings")

        self.__logger.debug("is resource usage monitoring enabled: "
                            f"{self.__is_monitoring_enabled}")

        # set the defulat settings for REST service
        self.__is_app_server_enabled = False
        # overwirte the default settings from XML configurations
        try:
            self.__is_app_server_enabled = strtobool(self.__action_plan_parameters_dict.get("CO_SIM_ENABLE_REST_APP_SERVER"))
        except Exception as e:
            # This could happen when the value is not set in XML file properly
            # Now, fall back to default settings
            self.__log_exception(
                exception=e,
                message="App Server en/disable settings could not be set from XML")
            self.__logger.critical("falling back to default settings")

        self.__logger.debug("is app server enabled: "
                            f"{self.__is_app_server_enabled}")

        self.__logger.debug('Launching Manager is initialized.')

    def __log_exception(self, exception, message):
        """logs the custom message and the exception with traceback"""
        self.__logger.critical(message)
        self.__logger.exception(f"got the exception: {exception}")
    
    def __get_expected_action_launch_method(self, action_event):
        """
        helper function which returns the relative launching method
        based on the action-event type.
        """
        # Case 1: SEQUENTIAL_ACTIONS
        if action_event == constants.CO_SIM_WAIT_FOR_SEQUENTIAL_ACTIONS:
            return constants.CO_SIM_SEQUENTIAL_ACTION
        # Case 2: CONCURRENT_ACTIONS
        if action_event == constants.CO_SIM_WAIT_FOR_CONCURRENT_ACTIONS:
            return constants.CO_SIM_CONCURRENT_ACTION

        # Otherwise, the <action_event> element contains an erroneous entry
        self.__logger.error(f'wrong <action_event> entry found:{action_event}')
        return enums.LauncherReturnCodes.XML_ERROR

    def __check_actions_grouping(self):
        """
            Goes through the launching strategy dictionary in order to check
            whether the actions associated to the waiting events have the
            proper action type, i.e. SEQUENTIAL or CONCURRENT

        :return:
            LAUNCHER_OK: The actions' launching methods concord with the
            waiting event

            ACTIONS_GROUPING_ERROR: The launcher strategy is inconsistent due
            to wrong grouping association

            XML_ERROR: The <action_plan> section contains some erroneous
            entries
        """
        self.__logger.debug('Validating the grouping of action and launching'
                            'methods')
        for key, value in self.__launching_strategy_dict.items():
            action_xml_id = key  # e.g. action_010 taken from XML <action_010>
            action_event = value['action_event']  # SEQUENTIAL or CONCURRENT
            actions_list = value['actions_list']  # e.g.[action_006,action_008]
            expected_action_launch_method = ''

            # get action event launching method type
            expected_action_launch_method = \
                self.__get_expected_action_launch_method(action_event)
            # validate action event launching method type
            if expected_action_launch_method == \
                    enums.LauncherReturnCodes.XML_ERROR:
                # an error is already logged for wrong action_event entry
                return enums.LauncherReturnCodes.XML_ERROR

            # validate the grouping of actions and their launching method types
            for current_action in actions_list:
                if self.__action_plan_dict[current_action]['action_launch_method'] \
                        == expected_action_launch_method:
                    # NOTE: here could be placed additional code to grab
                    # information associated to the current_action
                    # notwithstanding, it has been preferred to keep each
                    # method performing one particular task
                    pass
                else:
                    # action is associated with a wrong launching method type
                    self.__logger.error(
                        f'<{action_xml_id}> method {action_event} '
                        f'expects <{current_action}> having the '
                        f'{expected_action_launch_method} launching method.')
                    return enums.LauncherReturnCodes.ACTIONS_GROUPING_ERROR

        # otherwise, action and launching methods grouping is validated
        return enums.LauncherReturnCodes.LAUNCHER_OK

    def __gather_action_xml_filenames(self):
        """
        Goes through the launching strategy dictionary in order to gather the
        XML filenames from the action plan XML file.

        :return:
            LAUNCHER_OK: All the XML filenames were gathered from the action
            plan XML file

            GATHERING_XML_FILENAMES_ERROR: Error by gathering the XML filename
            from the section <action_xml>
        """
        self.__logger.debug('Gathering action XML file names.')
        try:
            for _, value in self.__launching_strategy_dict.items():
                actions_list = value['actions_list']  # e.g. [action_006,...]

                for current_action in actions_list:
                    self.__actions_xml_filenames_dict[current_action] = \
                        {'action_xml':
                             self.__action_plan_dict[current_action]['action_xml']}
        except KeyError:
            # XML file name is not found in action_plan_dict
            self.__logger.error('Error in gathering action XML file names.')
            return enums.LauncherReturnCodes.GATHERING_XML_FILENAMES_ERROR

        # otherwise, XML file names are gatheres from the action plan XML file
        self.__logger.debug('action XML file names are gathered.')
        return enums.LauncherReturnCodes.LAUNCHER_OK

    def __map_out_launching_strategy(self):
        """
            Goes through the action plan dictionary and find the CO_SIM_EVENT
            entries in order to establish the converging points where the
            launcher should perform a wait for the actions previously launched.

        :return:
            LAUNCHER_OK: The launching strategy was built out properly

            MAPPING_OUT_ERROR: The action plan contain some unlogical sequence
            of actions XML_ERROR: The <action_plan> section contains some
            erroneous entries.
        """
        actions_list = []
        actions_counter = 0

        # looking for the events and grouping the actions based on them.
        # e.g. {'action_type': 'CO_SIM_EVENT',
        #       'action_event': 'CO_SIM_WAIT_FOR_SEQUENTIAL_ACTIONS'}
        # and
        # {'action_type': 'CO_SIM_ACTION_SCRIPT',
        #  'action_script': 'initial_spikes_generator.xml',
        #  'action_launch_method': 'CO_SIM_SEQUENTIAL_ACTION'}
        # will be grouped
        for key, value in self.__action_plan_dict.items():
            # checking again the <action_type> value, just in case...
            if value['action_type'] == constants.CO_SIM_ACTION:
                # accumulating the actions before finding an event action type
                actions_list.append(key)

                # counting the number the actions associated to the event
                # to establish the number of spawner processes
                # to be created later
                actions_counter += 1
            elif value['action_type'] == constants.CO_SIM_EVENT:
                # an event has been found (meaning, a graph node)
                # related to actions (task to be spawned),
                # it must be SEQUENTIAL or CONCURRENT
                self.__launching_strategy_dict[key] = \
                    {'action_event': value['action_event'],
                     'actions_list': actions_list, }
                actions_list = []
                actions_counter = 0
            else:
                self.__logger.error(f"wrong <action_type> found:"
                                    f"{value['action_type']}")
                return enums.LauncherReturnCodes.XML_ERROR

            if actions_counter > self.__maximum_number_actions_found:
                # keeping the maximum number of actions associated to one event
                # in order to use it as number of spawner process to be
                # initiated
                self.__maximum_number_actions_found = actions_counter

            self.__logger.debug(f'Maximum number of actions: '
                                f'{self.__maximum_number_actions_found}')

        if self.__launching_strategy_dict and actions_list:
            self.__logger.error('<action_plan> must be ended with a'
                                ' CO_SIM_EVENT element')
            return enums.LauncherReturnCodes.MAPPING_OUT_ERROR

        # at this point __launching_strategy_dict contains the actions
        # grouped by events
        return enums.LauncherReturnCodes.LAUNCHER_OK

    def __perform_sequential_actions(self, actions_list, event_action_xml_id):
        '''
        helper function for performing the SEQUENTIAL actions

        Parameters
        ----------

        actions_list : list
            list of (SEQUENTIAL) actions to be performed

        event_action_xml_id: str
            XML tag of the action event

        Returns
        ------
           LAUNCHER_OK: All the SEQUENTIAL actions are performed successfully

           LAUNCHER_NOT_OK: Something went wrong such as no Popen args are
           found to spawn the process
        '''
        self.__logger.info(f'Sequentially processing of actions owned by the '
                           f'event <{event_action_xml_id}>')
        # start spawner processes to perform SEQUENTIAL actions
        if self.__start_spawner_processes() == \
                enums.LauncherReturnCodes.LAUNCHER_NOT_OK:
            # processes could not be started,
            # a more specific error is already logged
            return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

        # processes are started, now perform SEQUENTIAL actions
        for action_xml_id in actions_list:
            action_popen_args_list = []
            try:
                # get action (Popen args) to be performed
                action_popen_args_list = \
                    self.__actions_popen_args_dict[action_xml_id]
            except KeyError:
                self.__logger.error(f'There are no Popen args to spawn'
                                    f'<{action_xml_id}>')
                return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

            # Popen args are found
            try:
                # sending action to spawner process to perform it
                self.__actions_to_be_carried_out_jq.put(Action(
                    event_action_xml_id=event_action_xml_id,
                    action_xml_id=action_xml_id,
                    action_popen_args_list=action_popen_args_list,
                    logger=self.__logger))
                # SEQUENTIAL effect
                # waiting until the Task has finished (task by task)
                self.__actions_to_be_carried_out_jq.join()
            except KeyboardInterrupt:
                self.__logger.critical('Caught KeyboardInterrupt! '
                                       'Setting stop event')
                # TODO: rather handle it with signal manager
                self.__stopping_event.set()

        # All sequential actions have been performed,
        # stop the spawner processes
        if self.__stop_spawner_processes() == \
                enums.LauncherReturnCodes.LAUNCHER_NOT_OK:
            # processes could not be stopped,
            # a more specific error is already logged
            return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

        # The processes are stopped after performing all SEQUENTIAL actions
        return enums.LauncherReturnCodes.LAUNCHER_OK

    def __action_identifiers(self):
        goal = self.__action_plan_dict[action_xml_id]['action_goal']
        label = self.__action_plan_dict[action_xml_id]['action_label']

    def __perform_concurrent_actions(self, actions_list, event_action_xml_id):
        '''
        helper function for performing the CONCURRENT actions

        Parameters
        ----------

        actions_list : list
            list of (CONCURRENT) actions to be performed

        event_action_xml_id: str
            XML tag of the action event e.g. action_004, etc

        Returns
        ------

           LAUNCHER_OK: All the SEQUENTIAL actions are performed successfully

           LAUNCHER_NOT_OK: Something went wrong such as no Popen args are
           found to spawn the process
        '''
        concurrent_actions_list = []
        # gather all concurrent actions to be performed
        self.__logger.debug('populating the list of CONCURRENT actions to be'
                            ' performed')
        for action_xml_id in actions_list:
            action_popen_args_list = []
            try:
                # get action specific identifiers such as goal, label etc.
                goal = self.__action_plan_dict[action_xml_id]['action_goal']
                label = self.__action_plan_dict[action_xml_id]['action_label']

                # get action (Popen args) to be performed
                action_popen_args_list = \
                    self.__actions_popen_args_dict[action_xml_id]

                # append configurations_manager and log_settings to Inject
                # Dependencies to have uniform log settings and centralized
                # location for output directories
                action_popen_args_list.append(
                    base64.b64encode(
                        pickle.dumps(self._configurations_manager)))
                action_popen_args_list.append(
                    base64.b64encode(
                        pickle.dumps(self._logger_settings)))
                action_popen_args_list.append(self.__actions_sci_params_dict[action_xml_id])
            except KeyError:
                self.__logger.error(f'There are no Popen args to spawn'
                                    f'<{action_xml_id}>')
                return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

            # actions (Popen args) are found
            self.__logger.debug(f'appending action: {action_popen_args_list}')
            concurrent_actions_list.append(
                {'action': action_popen_args_list,
                 'action-id': action_xml_id,
                 'action-goal': goal,
                 'action-label':label})

        # initialize launcher to perform concurrent actions
        if self.__action_plan_variables_dict[CO_SIM_EXECUTION_ENVIRONMENT].upper() != "LOCAL":
            self.__is_execution_environment_hpc = True

        concurrent_actions_launcher = \
            LauncherHPC(self._logger_settings,
                        self._configurations_manager,
                        proxy_manager_server_address=None,  # Using default values
                        communication_settings_dict=self.__communication_settings_dict,
                        is_execution_environment_hpc=self.__is_execution_environment_hpc,
                        services_deployment_dict=self.__services_deployment_dict,
                        is_interactive=self.__is_interactive,
                        is_monitoring_enabled=self.__is_monitoring_enabled,
                        is_app_server_enabled=self.__is_app_server_enabled)
        # perform concurrent actions
        self.__logger.debug(f'performing CONCURRENT actions: '
                            f'{concurrent_actions_list}')
        if concurrent_actions_launcher.launch(concurrent_actions_list) == \
                Response.OK:
            return enums.LauncherReturnCodes.LAUNCHER_OK
        else:
            return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

    def __start_spawner_processes(self):
        """
        helper function to start the spawner processes for performing
        SEQUENTIAL actions.
        """
        self.__spawners = [Spawner(
            self.__launching_manager_PID,  # PPID for the Spawner
            actions_to_be_carried_out=self.__actions_to_be_carried_out_jq,
            returned_codes=self.__actions_return_codes_q,
            logger=self.__logger,
            stopping_event=self.__stopping_event)
            for _ in range(self.__maximum_number_actions_found)]

        # start spawner processes
        self.__logger.debug('starting the spawners.')
        for current_spawner in self.__spawners:
            if current_spawner.start() is not None:
                self.__logger.error(f'{current_spawner} could not be started')
                # TODO terminate loudly with error
                return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

        # spawner processes are started
        return enums.LauncherReturnCodes.LAUNCHER_OK

    def __stop_spawner_processes(self):
        """
        helper function to stop the spawner processes after performing
        SEQUENTIAL actions.
        """
        # poison pill to all spawner processes
        self.__logger.debug('Poison pilling to spawners.')
        try:
            for _ in self.__spawners:
                self.__actions_to_be_carried_out_jq.put(None)

            # Waiting until all the spawner processes have taken their pill
            self.__actions_to_be_carried_out_jq.join()
            self.__logger.debug('All spawners have taken their pill.')
        except KeyboardInterrupt:
            # TODO handle with signal manager
            self.__logger.info("Caught KeyboardInterrupt! Setting stop event")
            self.__stopping_event.set()
            return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

        # all spawner processes have taken their pill
        return enums.LauncherReturnCodes.LAUNCHER_OK

    def __perform_spawning_strategy(self):
        """
        Performs the (SEQUENTIAL and CONCURRENT) actions as per launching
        strategy

        :return:
            LAUNCHER_OK: all the action finished as expected
        """
        # switcher to invoke relevant function to perform specific action types
        action_execution_choices = {
            # case 1
            constants.CO_SIM_WAIT_FOR_SEQUENTIAL_ACTIONS: \
                self.__perform_sequential_actions,
            # case 2
            constants.CO_SIM_WAIT_FOR_CONCURRENT_ACTIONS: \
                self.__perform_concurrent_actions}

        # retrieve the actions from launching_strategy_dict to perform them
        for key, value in self.__launching_strategy_dict.items():
            # i. get the event
            event_action_xml_id = key
            # ii. get the event defining the spawning strategy
            action_event = value['action_event']
            # iii. get the list of actions owned by the event
            actions_list = value['actions_list']
            # iv. perform the actions
            if not action_execution_choices[action_event](
                    actions_list,
                    event_action_xml_id) == \
                   enums.LauncherReturnCodes.LAUNCHER_OK:
                # something went wrong while performing actions,
                # more specific errors are already logged
                return enums.LauncherReturnCodes.LAUNCHER_NOT_OK

        # otherwise, all actions are performed successfully
        return enums.LauncherReturnCodes.LAUNCHER_OK

    def carry_out_action_plan(self):
        """
        Goes through the action-plan dictionary and spawn the required actions
        and waits for and manages  the happened events

        :return:
            LAUNCHER_OK: All the action are spawned successfully according to
            the action-plan

            MAPPING_OUT_ERROR: The action-plan has not proper logic to be
            mapped out into the launching strategy dictionary

            PERFORMING_STRATEGY_ERROR: Some action ended with error
        """
        ########
        # STEP 1 - Grouping actions by events
        ########
        if not self.__map_out_launching_strategy() == \
               enums.LauncherReturnCodes.LAUNCHER_OK:
            self.__logger.debug('something went wrong by mapping out the'
                                ' action-plan')
            return enums.LauncherReturnCodes.MAPPING_OUT_ERROR

        ########
        # STEP 2 - Checking the actions grouping, i.e. SEQUENTIAL or CONCURRENT
        ########
        if not self.__check_actions_grouping() == \
               enums.LauncherReturnCodes.LAUNCHER_OK:
            self.__logger.debug('an action inconsistently associated to a'
                                ' waiting event was found')
            return enums.LauncherReturnCodes.ACTIONS_GROUPING_ERROR

        ########
        # STEP 3 - Gathering the XML filenames from the <action_xml> element of
        # each action
        ########
        if not self.__gather_action_xml_filenames() == \
               enums.LauncherReturnCodes.LAUNCHER_OK:
            self.__logger.debug('error by gathering XML filenames from the'
                                ' action plan')
            return enums.LauncherReturnCodes.GATHERING_XML_FILENAMES_ERROR

        ########
        # STEP 4 - Carrying out the action plan, based on events and their
        # associated actions
        ########
        if not self.__perform_spawning_strategy() == \
               enums.LauncherReturnCodes.LAUNCHER_OK:
            self.__logger.debug('something went wrong by executing the '
                                'action-plan')
            return enums.LauncherReturnCodes.PERFORMING_STRATEGY_ERROR

        # Check if all actions are performed without error
        there_was_an_error = False
        while not self.__actions_return_codes_q.empty():
            current_action_result = self.__actions_return_codes_q.get()
            if current_action_result == enums.ActionReturnCodes.OK:
                continue
            else:
                there_was_an_error = True
                break
        # some actions finish with error
        if there_was_an_error:
            return enums.LauncherReturnCodes.ACTIONS_FINISHED_WITH_ERROR

        # no errors! (all actions returned Popen rc=0)
        return enums.LauncherReturnCodes.LAUNCHER_OK
