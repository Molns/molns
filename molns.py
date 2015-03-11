#!/usr/bin/env python
import os
import re
import sys
from MolnsLib.molns_datastore import Datastore, DatastoreException, VALID_PROVIDER_TYPES
from collections import OrderedDict
import subprocess
from MolnsLib.ssh_deploy import SSHDeploy
import multiprocessing

import logging
logger = logging.getLogger()
#logger.setLevel(logging.INFO)  #for Debugging
logger.setLevel(logging.CRITICAL)
###############################################

class CommandException(Exception):
    pass
###############################################
def table_print(column_names, data):
    column_width = [0]*len(column_names)
    for i,n in enumerate(column_names):
        column_width[i] = len(str(n))
    for row in data:
        if len(row) != len(column_names):
            print "len(row) != len(column_names): {0} vs {1}".format(len(row), len(column_names))
        for i,n in enumerate(row):
            if len(str(n)) > column_width[i]:
                column_width[i] = len(str(n))
    out = "|".join([ "-"*(column_width[i]+2) for i in range(len(column_names))])
    print '|'+out+'|'
    out = " | ".join([ column_names[i].ljust(column_width[i]) for i in range(len(column_names))])
    print '| '+out+' |'
    out = "|".join([ "-"*(column_width[i]+2) for i in range(len(column_names))])
    print '|'+out+'|'
    for row in data:
        out = " | ".join([ str(n).ljust(column_width[i]) for i,n in enumerate(row)])
        print '| '+out+' |'
    out = "|".join([ "-"*(column_width[i]+2) for i in range(len(column_names))])
    print '|'+out+'|'

def raw_input_default(q, default=None, obfuscate=False):
    if default is None or default == '':
        return raw_input("{0}:".format(q))
    else:
        if obfuscate:
            ret = raw_input("{0} [******]: ".format(q))
        else:
            ret = raw_input("{0} [{1}]: ".format(q, default))
        if ret == '':
            return default
        else:
            return ret.strip()

def raw_input_default_config(q, default=None, obj=None):
    """ Ask the user and process the response with a default value. """
    if default is None:
        if callable(q['default']):
            f1 = q['default']
            try:
                default = f1(obj)
            except TypeError:
                pass
        else:
            default = q['default']
    if 'ask' in q and not q['ask']:
        return default
    if 'obfuscate' in q and q['obfuscate']:
        return raw_input_default(q['q'], default=default, obfuscate=True)
    else:
        return raw_input_default(q['q'], default=default, obfuscate=False)

def setup_object(obj):
    """ Setup a molns_datastore object using raw_input_default function. """
    for key, conf, value in obj.get_config_vars():
        obj[key] = raw_input_default_config(conf, default=value, obj=obj)

###############################################
class SubCommand():
    def __init__(self, command, subcommands):
        self.command = command
        self.subcommands = subcommands
    def __str__(self):
        r = ''
        for c in self.subcommands:
             r +=  self.command + " " + c.__str__() + "\n"
        return r[:-1]
    def __eq__(self, other):
        return self.command == other

    def run(self, args, config_dir=None):
        #print "SubCommand().run({0}, {1})".format(self.command, args)
        if len(args) > 0:
            cmd = args[0]
            for c in self.subcommands:
                if c == cmd:
                    return c.run(args[1:], config_dir=config_dir)
        raise CommandException("command not found")

###############################################
class Command():
    def __init__(self, command, args_defs={}, description=None, function=None):
        self.command = command
        self.args_defs = args_defs
        if function is None:
            raise Exception("Command must have a function")
        self.function = function
        if description is None:
            self.description = function.__doc__.strip()
        else:
            self.description = description
    def __str__(self):
        ret = self.command+" "
        for k,v in self.args_defs.iteritems():
            if v is None:
                ret += "[{0}] ".format(k)
            else:
                ret += "[{0}={1}] ".format(k,v)
        ret += "\n\t"+self.description
        return ret
        
    def __eq__(self, other):
        return self.command == other

    def run(self, args, config_dir=None):
        config = MOLNSConfig(config_dir=config_dir)
        self.function(args, config=config)

###############################################
class MOLNSConfig(Datastore):
    def __init__(self, config_dir):
        Datastore.__init__(self,config_dir=config_dir)
    
    def __str__(self):
        return "MOLNSConfig(config_dir={0})".format(self.config_dir)

###############################################
class MOLNSbase():
    @classmethod
    def _get_workerobj(cls, args, config):
        # Name
        worker_obj = None
        if len(args) > 0:
            worker_name = args[0]
            # Get worker db object
            try:
                worker_obj = config.get_object(name=worker_name, kind='WorkerGroup')
            except DatastoreException:
                worker_obj = None
            #logging.debug("controller_obj {0}".format(controller_obj))
            if worker_obj is None:
                print "worker group '{0}' is not initialized, use 'molns worker setup {0}' to initialize the controller.".format(worker_name)
        else:
            print "No worker name specified, please specify a name"
        return worker_obj

    @classmethod
    def _get_controllerobj(cls, args, config):
        # Name
        if len(args) > 0:
            controller_name = args[0]
        else:
            print "No controller name given"
            return None
        # Get controller db object
        try:
            controller_obj = config.get_object(name=controller_name, kind='Controller')
        except DatastoreException:
            controller_obj = None
        #logging.debug("controller_obj {0}".format(controller_obj))
        if controller_obj is None:
            print "controller '{0}' is not initialized, use 'molns controller setup {0}' to initialize the controller.".format(controller_name)
        return controller_obj

class MOLNSController(MOLNSbase):
    @classmethod
    def setup_controller(cls, args, config):
        """Setup a controller.  Set the provider configuration for the head node.  Use 'worker setup' to set the configuration for worker nodes
        """
        logging.debug("MOLNSController.setup_controller(config={0})".format(config))
        # name
        if len(args) > 0:
            controller_name = args[0]
        else:
            print "Usage: molns.py controller setup NAME"
            return
        try:
            controller_obj = config.get_object(args[0], kind='Controller')
        except DatastoreException as e:
            # provider
            providers = config.list_objects(kind='Provider')
            if len(providers)==0:
                print "No providers configured, please configure one ('molns provider setup') before initializing controller."
                return
            print "Select a provider:"
            for n,p in enumerate(providers):
                print "\t[{0}] {1}".format(n,p.name)
            provider_ndx = int(raw_input_default("enter the number of provider:", default='0'))
            provider_id = providers[provider_ndx].id
            provider_obj = config.get_object(name=providers[provider_ndx].name, kind='Provider')
            logging.debug("using provider {0}".format(provider_obj))
            # create object
            try:
                controller_obj = config.create_object(ptype=provider_obj.type, name=controller_name, kind='Controller', provider_id=provider_id)
            except DatastoreException as e:
                print e
                return
        setup_object(controller_obj)
        config.save_object(controller_obj, kind='Controller')

    @classmethod
    def list_controller(cls, args, config):
        """ List all the currently configured controllers."""
        controllers = config.list_objects(kind='Controller')
        if len(controllers) == 0:
            print "No controllers configured"
        else:
            table_data = []
            for c in controllers:
                provider_name = config.get_object_by_id(c.provider_id, 'Provider').name
                table_data.append([c.name, provider_name])
            table_print(['name', 'provider'], table_data)
    
    @classmethod
    def show_controller(cls, args, config):
        """ Show all the details of a controller config. """
        if len(args) == 0:
            print "USAGE: molns controller show name"
            return
        print config.get_object(name=args[0], kind='Controller')

    @classmethod
    def delete_controller(cls, args, config):
        """ Delete a controller config. """
        #print "MOLNSProvider.delete_provider(args={0}, config={1})".format(args, config)
        if len(args) == 0:
            print "USAGE: molns cluser delete name"
            return
        config.delete_object(name=args[0], kind='Controller')

    @classmethod
    def ssh_controller(cls, args, config):
        """ SSH into the controller. """
        #logging.debug("MOLNSController.ssh_controller(args={0})".format(args))
        controller_obj = cls._get_controllerobj(args, config)
        if controller_obj is None: return
        # Check if any instances are assigned to this controller
        instance_list = config.get_controller_instances(controller_id=controller_obj.id)
        #logging.debug("instance_list={0}".format(instance_list))
        # Check if they are running
        ip = None
        if len(instance_list) > 0:
            for i in instance_list:
                status = controller_obj.get_instance_status(i)
                logging.debug("instance={0} has status={1}".format(i, status))
                if status == controller_obj.STATUS_RUNNING:
                    ip = i.ip_address
        if ip is None:
            print "No active instance for this controller"
            return
        #print " ".join(['/usr/bin/ssh','-oStrictHostKeyChecking=no','-oUserKnownHostsFile=/dev/null','-i',controller_obj.provider.sshkeyfilename(),'ubuntu@{0}'.format(ip)])
        #os.execl('/usr/bin/ssh','-oStrictHostKeyChecking=no','-oUserKnownHostsFile=/dev/null','-i',controller_obj.provider.sshkeyfilename(),'ubuntu@{0}'.format(ip))
        cmd = ['/usr/bin/ssh','-oStrictHostKeyChecking=no','-oUserKnownHostsFile=/dev/null','-i',controller_obj.provider.sshkeyfilename(),'ubuntu@{0}'.format(ip)]
        print " ".join(cmd)
        subprocess.call(cmd)
        print "SSH process completed"
        

    @classmethod
    def status_controller(cls, args, config):
        """ Get status of the head node of a MOLNs controller. """
        #logging.debug("MOLNSController.status_controller(args={0})".format(args))
        if len(args) > 0:
            controller_obj = cls._get_controllerobj(args, config)
            if controller_obj is None: return
            # Check if any instances are assigned to this controller
            instance_list = config.get_controller_instances(controller_id=controller_obj.id)
            table_data = []
            if len(instance_list) > 0:
                for i in instance_list:
                    provider_name = config.get_object_by_id(i.provider_id, 'Provider').name
                    controller_name = config.get_object_by_id(i.controller_id, 'Controller').name
                    status = controller_obj.get_instance_status(i)
                    table_data.append([controller_name, status, 'controller', provider_name, i.provider_instance_identifier, i.ip_address])

            else:
                print "No instance running for this controller"
                return
            # Check if any worker instances are assigned to this controller
            instance_list = config.get_worker_instances(controller_id=controller_obj.id)
            if len(instance_list) > 0:
                for i in instance_list:
                    worker_name = config.get_object_by_id(i.worker_group_id, 'WorkerGroup').name
                    worker_obj = cls._get_workerobj([worker_name], config)
                    provider_name = config.get_object_by_id(i.provider_id, 'Provider').name
                    status = worker_obj.get_instance_status(i)
                    table_data.append([worker_name, status, 'worker', provider_name, i.provider_instance_identifier, i.ip_address])
            table_print(['name','status','type','provider','instance id', 'IP address'],table_data)
        else:
            instance_list = config.get_all_instances()
            if len(instance_list) > 0:
                print "Current instances:"
                table_data = []
                for i in instance_list:
                    provider_name = config.get_object_by_id(i.provider_id, 'Provider').name
                    controller_name = config.get_object_by_id(i.controller_id, 'Controller').name
                    if i.worker_group_id is not None:
                        worker_name = config.get_object_by_id(i.worker_group_id, 'WorkerGroup').name
                        table_data.append([worker_name, 'worker', provider_name, i.provider_instance_identifier])
                    else:
                        table_data.append([controller_name, 'controller', provider_name, i.provider_instance_identifier])

                table_print(['name','type','provider','instance id'],table_data)
                print "\n\tUse 'molns status NAME' to see current status of each instance."
            else:
                print "No instance found"


    @classmethod
    def start_controller(cls, args, config):
        """ Start the MOLNs controller. """
        #logging.debug("MOLNSController.start_controller(args={0})".format(args))
        controller_obj = cls._get_controllerobj(args, config)
        if controller_obj is None: return
        # Check if any instances are assigned to this controller
        instance_list = config.get_all_instances(controller_id=controller_obj.id)
        # Check if they are running or stopped (if so, resume them)
        inst = None
        if len(instance_list) > 0:
            for i in instance_list:
                status = controller_obj.get_instance_status(i)
                if status == controller_obj.STATUS_RUNNING:
                    print "controller already running at {0}".format(i.ip_address)
                    return
                elif status == controller_obj.STATUS_STOPPED:
                    print "Resuming instance at {0}".format(i.ip_address)
                    controller_obj.resume_instance(i)
                    inst = i
                    break
        if inst is None:
            # Start a new instance
            print "Starting new controller"
            inst = controller_obj.start_instance()
        # deploying
        sshdeploy = SSHDeploy(config=controller_obj.provider, config_dir=config.config_dir)
        sshdeploy.deploy_ipython_controller(inst.ip_address)
        sshdeploy.deploy_molns_webserver(inst.ip_address)
        #sshdeploy.deploy_stochss(inst.ip_address, port=443)

    @classmethod
    def stop_controller(cls, args, config):
        """ Stop the head node of a MOLNs controller. """
        #logging.debug("MOLNSController.stop_controller(args={0})".format(args))
        controller_obj = cls._get_controllerobj(args, config)
        if controller_obj is None: return
        # Check if any instances are assigned to this controller
        instance_list = config.get_all_instances(controller_id=controller_obj.id)
        # Check if they are running
        if len(instance_list) > 0:
            for i in instance_list:
                if i.worker_group_id is None:
                    status = controller_obj.get_instance_status(i)
                    if status == controller_obj.STATUS_RUNNING:
                        print "Stopping controller running at {0}".format(i.ip_address)
                        controller_obj.stop_instance(i)
                else:
                    worker_name = config.get_object_by_id(i.worker_group_id, 'WorkerGroup').name
                    worker_obj = cls._get_workerobj([worker_name], config)
                    status = worker_obj.get_instance_status(i)
                    if status == worker_obj.STATUS_RUNNING or status == worker_obj.STATUS_STOPPED:
                        print "Terminating worker '{1}' running at {0}".format(i.ip_address, worker_name)
                        worker_obj.terminate_instance(i)
    
        else:
            print "No instance running for this controller"


    @classmethod
    def terminate_controller(cls, args, config):
        """ Terminate the head node of a MOLNs controller. """
        #logging.debug("MOLNSController.terminate_controller(args={0})".format(args))
        controller_obj = cls._get_controllerobj(args, config)
        if controller_obj is None: return
        instance_list = config.get_all_instances(controller_id=controller_obj.id)
        # Check if they are running or stopped 
        if len(instance_list) > 0:
            for i in instance_list:
                if i.worker_group_id is None:
                    status = controller_obj.get_instance_status(i)
                    if status == controller_obj.STATUS_RUNNING:
                        print "Terminating controller running at {0}".format(i.ip_address)
                        controller_obj.terminate_instance(i)
                else:
                    worker_name = config.get_object_by_id(i.worker_group_id, 'WorkerGroup').name
                    worker_obj = cls._get_workerobj([worker_name], config)
                    status = worker_obj.get_instance_status(i)
                    if status == worker_obj.STATUS_RUNNING or status == worker_obj.STATUS_STOPPED:
                        print "Terminating worker '{1}' running at {0}".format(i.ip_address, worker_name)
                        worker_obj.terminate_instance(i)


        else:
            print "No instance running for this controller"

    @classmethod
    def connect_controller_to_local(cls, args, config):
        """ Connect a local iPython installation to the controller. """
        logging.debug("MOLNSController.connect_controller_to_local(args={0})".format(args))
        if len(args) != 2:
            print "USAGE: molns local-connect controller_name profile_name"
            return
        controller_name = args[1]
        profile_name = args[1]
        logging.debug("connecting controller {0} to local ipython profile {1}".format(controller_name, profile_name))
        controller_obj = cls._get_controllerobj(args, config)
        if controller_obj is None: return
        # Check if any instances are assigned to this controller
        instance_list = config.get_all_instances(controller_id=controller_obj.id)
        # Check if they are running
        inst = None
        if len(instance_list) > 0:
            for i in instance_list:
                status = controller_obj.get_instance_status(i)
                if status == controller_obj.STATUS_RUNNING:
                    print "Connecting to controller at {0}".format(i.ip_address)
                    inst = i
                    break
        if inst is None:
            print "No instance running for this controller"
            return
        # deploying
        sshdeploy = SSHDeploy(config=controller_obj.provider, config_dir=config.config_dir)
        client_file_data = sshdeploy.get_ipython_client_file(inst.ip_address)
        home_dir = os.environ.get('HOME')
        ipython_client_filename = os.path.join(home_dir, '.ipython/profile_{0}/'.format(profile_name), 'security/ipcontroller-client.json')
        logging.debug("Writing file {0}".format(ipython_client_filename))
        with open(ipython_client_filename, 'w') as fd:
            fd.write(client_file_data)
        print "Success"

###############################################

class MOLNSWorkerGroup(MOLNSbase):
    @classmethod
    def setup_worker_groups(cls, args, config):
        """ Configure a worker group. """
        logging.debug("MOLNSWorkerGroup.setup_worker_groups(config={0})".format(config))
        # name
        if len(args) == 0:
            print "USAGE: molns worker setup name"
            return
        group_name = args[0]
        try:
            worker_obj = config.get_object(args[0], kind='WorkerGroup')
        except DatastoreException as e:
            # provider
            providers = config.list_objects(kind='Provider')
            if len(providers)==0:
                print "No providers configured, please configure one ('molns provider setup') before initializing worker group."
                return
            print "Select a provider:"
            for n,p in enumerate(providers):
                print "\t[{0}] {1}".format(n,p.name)
            provider_ndx = int(raw_input_default("enter the number of provider:", default='0'))
            provider_id = providers[provider_ndx].id
            provider_obj = config.get_object(name=providers[provider_ndx].name, kind='Provider')
            logging.debug("using provider {0}".format(provider_obj))
            # controller
            controllers = config.list_objects(kind='Controller')
            if len(controllers)==0:
                print "No controllers configured, please configure one ('molns controller setup') before initializing worker group."
                return
            print "Select a controller:"
            for n,p in enumerate(controllers):
                print "\t[{0}] {1}".format(n,p.name)
            controller_ndx = int(raw_input_default("enter the number of controller:", default='0'))
            controller_id = controllers[controller_ndx].id
            controller_obj = config.get_object(name=controllers[controller_ndx].name, kind='Controller')
            logging.debug("using controller {0}".format(controller_obj))
            # create object
            try:
                worker_obj = config.create_object(ptype=provider_obj.type, name=group_name, kind='WorkerGroup', provider_id=provider_id, controller_id=controller_obj.id)
            except DatastoreException as e:
                print e
                return
        setup_object(worker_obj)
        config.save_object(worker_obj, kind='WorkerGroup')

    @classmethod
    def list_worker_groups(cls, args, config):
        """ List all the currently configured worker groups."""
        groups = config.list_objects(kind='WorkerGroup')
        if len(groups) == 0:
            print "No worker groups configured"
        else:
            table_data = []
            for g in groups:
                provider_name = config.get_object_by_id(g.provider_id, 'Provider').name
                controller_name = config.get_object_by_id(g.controller_id, 'Controller').name
                table_data.append([g.name, provider_name, controller_name])
            table_print(['name', 'provider', 'controller'], table_data)

    @classmethod
    def show_worker_groups(cls, args, config):
        """ Show all the details of a worker group config. """
        if len(args) == 0:
            print "USAGE: molns worker show name"
            return
        print config.get_object(name=args[0], kind='WorkerGroup')

    @classmethod
    def delete_worker_groups(cls, args, config):
        """ Delete a worker group config. """
        if len(args) == 0:
            print "USAGE: molns worker delete name"
            return
        config.delete_object(name=args[0], kind='WorkerGroup')

    @classmethod
    def status_worker_groups(cls, args, config):
        """ Get status of the workers of a MOLNs cluster. """
        logging.debug("MOLNSWorkerGroup.status_worker_groups(args={0})".format(args))
        if len(args) > 0:
            worker_obj = cls._get_workerobj(args, config)
            if worker_obj is None: return
            # Check if any instances are assigned to this worker
            instance_list = config.get_all_instances(worker_group_id=worker_obj.id)
            # Check if they are running or stopped 
            if len(instance_list) > 0:
                table_data = []
                for i in instance_list:
                    status = worker_obj.get_instance_status(i)
                    #print "{0} type={3} ip={1} id={2}".format(status, i.ip_address, i.provider_instance_identifier, worker_obj.PROVIDER_TYPE)
                    worker_name = config.get_object_by_id(i.worker_group_id, 'WorkerGroup').name
                    provider_name = config.get_object_by_id(i.provider_id, 'Provider').name
                    status = worker_obj.get_instance_status(i)
                    table_data.append([worker_name, status, 'worker', provider_name, i.provider_instance_identifier, i.ip_address])
                table_print(['name','status','type','provider','instance id', 'IP address'],table_data)
            else:
                print "No worker instances running for this cluster"
        else:
            print "USAGE: molns worker status NAME"

    @classmethod
    def start_worker_groups(cls, args, config):
        """ Start workers of a MOLNs cluster. """
        logging.debug("MOLNSWorkerGroup.start_worker_groups(args={0})".format(args))
        worker_obj = cls._get_workerobj(args, config)
        if worker_obj is None: return
        num_vms = worker_obj['num_vms']
        num_vms_to_start = int(num_vms)
        controller_ip = cls.__launch_workers__get_controller(worker_obj, config)
        if controller_ip is None: return
        #logging.debug("\tcontroller_ip={0}".format(controller_ip))
        inst_to_deploy = cls.__launch_worker__start_or_resume_vms(worker_obj, config, num_vms_to_start)
        #logging.debug("\tinst_to_deploy={0}".format(inst_to_deploy))
        cls.__launch_worker__deploy_engines(worker_obj, controller_ip, inst_to_deploy, config)

    
    @classmethod
    def add_worker_groups(cls, args, config):
        """ Add workers of a MOLNs cluster. """
        logging.debug("MOLNSWorkerGroup.add_worker_groups(args={0})".format(args))
        if len(args) < 2:
            print "Usage: molns worker add GROUP num"
            return
        try:
            num_vms_to_start = int(args[1])
        except ValueError:
            print "'{0}' in not a valid number of engines.".format(args[1])
            return
        worker_obj = cls._get_workerobj(args, config)
        if worker_obj is None: return
        controller_ip = cls.__launch_workers__get_controller(worker_obj, config)
        if controller_ip is None: return
        inst_to_deploy = cls.__launch_worker__start_vms(worker_obj, num_vms_to_start)
        cls.__launch_worker__deploy_engines(worker_obj, controller_ip, inst_to_deploy, config)

    @classmethod
    def __launch_workers__get_controller(cls, worker_obj, config):
        # Check if a controller is running
        controller_ip = None
        instance_list = config.get_all_instances(controller_id=worker_obj.controller.id)
        provider_obj = worker_obj.controller
        # Check if they are running or stopped (if so, resume them)
        if len(instance_list) > 0:
            for i in instance_list:
                status = provider_obj.get_instance_status(i)
                logging.debug("instance {0} has status {1}".format(i.id, status))
                if status == provider_obj.STATUS_RUNNING or status == provider_obj.STATUS_STOPPED:
                    controller_ip = i.ip_address
                    print "Controller running at {0}".format(controller_ip)
                    break
        if controller_ip is None:
            print "No controller running for this worker group."
            return
        return controller_ip
        

    @classmethod
    def __launch_worker__start_or_resume_vms(cls, worker_obj, config, num_vms_to_start=0):
        # Check for any instances are assigned to this worker group
        instance_list = config.get_all_instances(worker_group_id=worker_obj.id)
        # Check if they are running or stopped (if so, resume them)
        inst_to_resume = []
        inst_to_deploy = []
        if len(instance_list) > 0:
            for i in instance_list:
                status = worker_obj.get_instance_status(i)
                if status == worker_obj.STATUS_RUNNING:
                    print "Worker running at {0}".format(i.ip_address)
                    num_vms_to_start -= 1
                elif status == worker_obj.STATUS_STOPPED:
                    print "Resuming worker at {0}".format(i.ip_address)
                    inst_to_resume.append(i)
                    num_vms_to_start -= 1
        #logging.debug("inst_to_resume={0}".format(inst_to_resume))
        if len(inst_to_resume) > 0:
            worker_obj.resume_instance(inst_to_resume)
            inst_to_deploy.extend(inst_to_resume)
        inst_to_deploy.extend(cls.__launch_worker__start_vms(worker_obj, num_vms_to_start))
        #logging.debug("inst_to_deploy={0}".format(inst_to_deploy))
        return inst_to_deploy

    @classmethod
    def __launch_worker__start_vms(cls, worker_obj, num_vms_to_start=0):
        """ Return a list of booted instances ready to be deployed as workers."""
        inst_to_deploy = []
        if num_vms_to_start > 0:
            # Start a new instances
            print "Starting {0} new workers".format(num_vms_to_start)
            inst_to_deploy  = worker_obj.start_instance(num=num_vms_to_start)
        if not isinstance(inst_to_deploy,list):
            inst_to_deploy = [inst_to_deploy]
        return inst_to_deploy


    @classmethod
    def __launch_worker__deploy_engines(cls, worker_obj, controller_ip, inst_to_deploy, config):
        print "Deploying on {0} workers".format(len(inst_to_deploy))
        if len(inst_to_deploy) > 0:
            # deploying
            controller_ssh = SSHDeploy(config=worker_obj.controller.provider, config_dir=config.config_dir)
            engine_ssh = SSHDeploy(config=worker_obj.provider, config_dir=config.config_dir)
            engine_file = controller_ssh.get_ipython_engine_file(controller_ip)
            controller_ssh_keyfile = worker_obj.controller.provider.sshkeyfilename()
            if len(inst_to_deploy) > 1:
                logging.debug("__launch_worker__deploy_engines() workpool(size={0})".format(len(inst_to_deploy)))
                jobs = []
                for i in inst_to_deploy:
                    logging.debug("multiprocessing.Process(target=engine_ssh.deploy_ipython_engine({0}, engine_file)".format(i.ip_address))
                    p = multiprocessing.Process(target=engine_ssh.deploy_ipython_engine, args=(i.ip_address, controller_ip, engine_file, controller_ssh_keyfile,))
                    jobs.append(p)
                    p.start()
                    logging.debug("__launch_worker__deploy_engines() joining processes.")
                for p in jobs:
                    p.join()
                logging.debug("__launch_worker__deploy_engines() joined processes.")
            else:
                for i in inst_to_deploy:
                    logging.debug("starting engine on {0}".format(i.ip_address))
                    engine_ssh.deploy_ipython_engine(i.ip_address, controller_ip, engine_file, controller_ssh_keyfile)
        else:
            return
        print "Success"

    @classmethod
    def stop_worker_groups(cls, args, config):
        """ Stop workers of a MOLNs cluster. """
        logging.debug("MOLNSWorkerGroup.stop_worker_groups(args={0})".format(args))
        worker_obj = cls._get_workerobj(args, config)
        if worker_obj is None: return
        # Check for any instances are assigned to this worker group
        instance_list = config.get_all_instances(worker_group_id=worker_obj.id)
        # Check if they are running or stopped (if so, resume them)
        inst_to_stop = []
        if len(instance_list) > 0:
            for i in instance_list:
                status = worker_obj.get_instance_status(i)
                if status == worker_obj.STATUS_RUNNING:
                    print "Stopping worker at {0}".format(i.ip_address)
                    inst_to_stop.append(i)
        if len(inst_to_stop) > 0:
            worker_obj.stop_instance(inst_to_stop)
        else:
            print "No workers running in the worker group"

    @classmethod
    def terminate_worker_groups(cls, args, config):
        """ Terminate workers of a MOLNs cluster. """
        logging.debug("MOLNSWorkerGroup.terminate_worker_groups(args={0})".format(args))
        worker_obj = cls._get_workerobj(args, config)
        if worker_obj is None: return
        # Check for any instances are assigned to this worker group
        instance_list = config.get_all_instances(worker_group_id=worker_obj.id)
        # Check if they are running or stopped (if so, resume them)
        inst_to_stop = []
        if len(instance_list) > 0:
            for i in instance_list:
                status = worker_obj.get_instance_status(i)
                if status == worker_obj.STATUS_RUNNING or status == worker_obj.STATUS_STOPPED:
                    print "Terminating worker at {0}".format(i.ip_address)
                    inst_to_stop.append(i)
        if len(inst_to_stop) > 0:
            worker_obj.terminate_instance(inst_to_stop)
        else:
            print "No workers running in the worker group"

###############################################

class MOLNSProvider():
    @classmethod
    def provider_setup(cls, args, config):
        """ Setup a new provider. Create the MOLNS image and SSH key if necessary."""
        #print "MOLNSProvider.provider_setup(args={0})".format(args)
        if len(args) < 1:
            print "USAGE: molns provider setup name"
            print "\tCreates a new provider with the given name."
            return
        # find the \n\tWhere PROVIDER_TYPE is one of: {0}".format(VALID_PROVIDER_TYPES)
        # provider name
        provider_name = args[0]
        # check if provider exists
        try:
            provider_obj = config.get_object(args[0], kind='Provider')
        except DatastoreException as e:
            # ask provider type
            print "Select a provider type:"
            for n,p in enumerate(VALID_PROVIDER_TYPES):
                print "\t[{0}] {1}".format(n,p)
            while True:
                try:
                    provider_ndx = int(raw_input_default("enter the number of type:", default='0'))
                    provider_type = VALID_PROVIDER_TYPES[provider_ndx]
                    break
                except (ValueError, IndexError):
                    pass
            logging.debug("provider type '{0}'".format(provider_type))
            # Create provider
            try:
                provider_obj = config.create_object(name=args[0], ptype=provider_type, kind='Provider')
            except DatastoreException as e:
                logging.exception(e)
                print e
                return
        print "Enter configuration for provider {0}:".format(args[0])
        setup_object(provider_obj)
        config.save_object(provider_obj, kind='Provider')
        #
        print "Checking all config artifacts."
        # check for ssh key
        if provider_obj['key_name'] is None or provider_obj['key_name'] == '':
            print "Error: no key_name specified."
            return
        elif not provider_obj.check_ssh_key():
            print "Creating key '{0}'".format(provider_obj['key_name'])
            provider_obj.create_ssh_key()
        else:
            print "SSH key={0} is valid.".format(provider_obj['key_name'])

        # check for security group
        if provider_obj['group_name'] is None or provider_obj['group_name'] == '':
            print "Error: no security group specified."
            return
        elif not provider_obj.check_security_group():
            print "Creating security group '{0}'".format(provider_obj['group_name'])
            provider_obj.create_seurity_group()
        else:
            print "security group={0} is valid.".format(provider_obj['group_name'])
        
        # check for MOLNS image
        if provider_obj['molns_image_name'] is None or provider_obj['molns_image_name'] == '':
            if provider_obj['ubuntu_image_name'] is None or provider_obj['ubuntu_image_name'] == '':
                print "Error: no ubuntu_image_name given, can not create molns image."
            else:
                print "Creating new image, this process can take a long time (10-30 minutes)."
                provider_obj['molns_image_name'] = provider_obj.create_molns_image()
        elif not provider_obj.check_molns_image():
            print "Error: an molns image was provided, but it is not available in cloud."
            return

        print "Success."
        config.save_object(provider_obj, kind='Provider')
    
    
    @classmethod
    def provider_rebuild(cls, args, config):
        """ Rebuild the MOLNS image."""
        if len(args) < 1:
            print "USAGE: molns provider rebuild name"
            print "\tCreates a new provider with the given name."
            return
        # provider name
        provider_name = args[0]
        # check if provider exists
        try:
            provider_obj = config.get_object(args[0], kind='Provider')
            if provider_obj['ubuntu_image_name'] is None or provider_obj['ubuntu_image_name'] == '':
                print "Error: no ubuntu_image_name given, can not create molns image."
            else:
                provider_obj['molns_image_name'] = provider_obj.create_molns_image()
                print "Success. new image = {0}".format(provider_obj['molns_image_name'])
                config.save_object(provider_obj, kind='Provider')
        except DatastoreException as e:
            print "provider not found"

    @classmethod
    def provider_list(cls, args, config):
        """ List all the currently configured providers."""
        #print "MOLNSProvider.provider_list(args={0}, config={1})".format(args, config)
        providers = config.list_objects(kind='Provider')
        if len(providers) == 0:
            print "No providers configured"
        else:
            table_data = []
            for p in providers:
                table_data.append([p.name, p.type])
            table_print(['name', 'type'], table_data)

    @classmethod
    def show_provider(cls, args, config):
        """ Show all the details of a provider config. """
        #print "MOLNSProvider.show_provider(args={0}, config={1})".format(args, config)
        if len(args) == 0:
            print "USAGE: molns provider show name"
            return
        print config.get_object(name=args[0], kind='Provider')

    @classmethod
    def delete_provider(cls, args, config):
        """ Delete a provider config. """
        #print "MOLNSProvider.delete_provider(args={0}, config={1})".format(args, config)
        if len(args) == 0:
            print "USAGE: molns provider delete name"
            return
        config.delete_object(name=args[0], kind='Provider')
###############################################

class MOLNSInstances():
    @classmethod
    def show_instances(cls, args, config):
        """ List all instances in the db """
        instance_list = config.get_all_instances()
        if len(instance_list) > 0:
            table_data = []
            for i in instance_list:
                provider_name = config.get_object_by_id(i.provider_id, 'Provider').name
                if i.worker_group_id is not None:
                    name = config.get_object_by_id(i.worker_id, 'WorkerGroup').name
                    itype = 'worker'
                else:
                    name = config.get_object_by_id(i.controller_id, 'Controller').name
                    itype = 'controller'
                table_data.append([i.id, provider_name, i.provider_instance_identifier, itype, name])
            table_print(['ID', 'provider', 'instance id', 'type', 'name'],table_data)
        else:
            print "No instance found"

    @classmethod
    def delete_instance(cls, args, config):
        """ delete an instance in the db """
        if len(args) == 0:
            print "Usage: molns instance delete INSTANCE_ID"
            return
        try:
            instance_id = int(args[0])
        except ValueError:
            print "instance ID must be a integer"
            return
        instance = config.get_instance_by_id(instance_id)
        if instance is None:
            print "instance not found"
        else:
            config.delete_instance(instance)
            print "instance {0} deleted".format(instance_id)


    @classmethod
    def clear_instances(cls, args, config):
        """ delete all instances in the db """
        instance_list = config.get_all_instances()
        if len(instance_list) > 0:
            for i in instance_list:
                print i
                config.delete_instance(i)
                print "instance {0} deleted".format(i.id)
        else:
            print "No instance found"



###############################################

COMMAND_LIST = [
        # Commands to interact with the head-node.
        Command('ssh', {'name':None},
            function=MOLNSController.ssh_controller),
        Command('status', {'name':None},
            function=MOLNSController.status_controller),
        Command('start', {'name':None},
            function=MOLNSController.start_controller),
        Command('stop', {'name':None},
            function=MOLNSController.stop_controller),
        Command('terminate', {'name':None},
            function=MOLNSController.terminate_controller),
        #Command('local-connect', {'name':None},
        #    function=MOLNSController.connect_controller_to_local),
        # Commands to interact with controller
        SubCommand('controller',[
            Command('setup', {'name':None},
                function=MOLNSController.setup_controller),
            Command('list', {'name':None},
                function=MOLNSController.list_controller),
            Command('show', {'name':None},
                function=MOLNSController.show_controller),
            Command('delete', {'name':None},
                function=MOLNSController.delete_controller),
        ]),
        # Commands to interact with Worker-Groups
        SubCommand('worker',[
            Command('setup', {'name':None},
                function=MOLNSWorkerGroup.setup_worker_groups),
            Command('list', {'name':None},
                function=MOLNSWorkerGroup.list_worker_groups),
            Command('show', {'name':None},
                function=MOLNSWorkerGroup.show_worker_groups),
            Command('delete', {'name':None},
                function=MOLNSWorkerGroup.delete_worker_groups),
            Command('start', {'name':None},
                function=MOLNSWorkerGroup.start_worker_groups),
            Command('add', {'name':None},
                function=MOLNSWorkerGroup.add_worker_groups),
            Command('status', {'name':None},
                function=MOLNSWorkerGroup.status_worker_groups),
            #Command('stop', {'name':None},
            #    function=MOLNSWorkerGroup.stop_worker_groups),
            Command('terminate', {'name':None},
                function=MOLNSWorkerGroup.terminate_worker_groups),
        ]),
        # Commands to interact with Infrastructure-Providers
        SubCommand('provider',[
            Command('setup',{'name':None},
                function=MOLNSProvider.provider_setup),
            Command('rebuild',{'name':None},
                function=MOLNSProvider.provider_rebuild),
            Command('list',{'name':None},
                function=MOLNSProvider.provider_list),
            Command('show',{'name':None},
                function=MOLNSProvider.show_provider),
            Command('delete',{'name':None},
                function=MOLNSProvider.delete_provider),
        ]),
        # Commands to interact with the instance DB
        SubCommand('instances',[
            Command('list', {},
                function=MOLNSInstances.show_instances),
            Command('delete', {'ID':None},
                function=MOLNSInstances.delete_instance),
            Command('clear', {},
                function=MOLNSInstances.clear_instances),
        ])
    ]

def printHelp():
    print "molns <command> <command-args>"
    print " --config=[Config Directory=./.molns/]"
    print "\tSpecify an alternate config location.  (Must be first argument.)"
    for c in COMMAND_LIST:
        print c


def parseArgs():
    if len(sys.argv) < 2 or sys.argv[1] == '-h':
        printHelp()
        return
    
    if sys.argv[1].startswith('--config='):
        config_dir = sys.argv[1].split('=',2)[1]
        arg_list = sys.argv[2:]
    else:
        config_dir = './.molns/'
        arg_list = sys.argv[1:]
    
    #print "config_dir", config_dir
    #print "arg_list ", arg_list
    if len(arg_list) == 0 or arg_list[0] =='help' or arg_list[0] == '-h':
        printHelp()
        return
        
    if arg_list[0] in COMMAND_LIST:
        #print arg_list[0] + " in COMMAND_LIST"
        for cmd in COMMAND_LIST:
            if cmd == arg_list[0]:
                try:
                    cmd.run(arg_list[1:], config_dir=config_dir)
                    return
                except CommandException:
                    pass
    print "unknown command: " +  " ".join(arg_list)
    #printHelp()
    print "use 'molns help' to see all possible commands"


if __name__ == "__main__":
    parseArgs()
