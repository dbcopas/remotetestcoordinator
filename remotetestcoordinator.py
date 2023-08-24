#! /usr/bin/python

import argparse
import socket
import struct
import fcntl
import sys
import threading
import thread
from random import randint
from time import sleep
import Queue
import json
from contextlib import contextmanager
from StringIO import StringIO
import re
import subprocess 
import errno
from socket import error as socket_error
import uuid
from  netaddr import *
import time
import os.path
import os
import signal
import shlex

LISTEN_PORT = 50007


config_vars_q = Queue.Queue()  # holds a collection of local tasks
sending_q = Queue.Queue() # holds the pending messages to be sent remote
confirmation_q = Queue.Queue() # holds the pending confirmation jobs
network_targets = []
my_network_address = None
run_and_exit = False
result_dict = dict()

DEBUG = False
DEFAULT_WAIT_TIME = 2

##################################
# Objects
##################################
class Controller:

    listener_thread = None
    dispatcher_thread = None
    sender_thread = None
    confirmation_thread = None

    is_daemon = False
    run_and_exit = False
    def __init__(self):
        global run_and_exit
        self.listener_thread = Listener()
        self.dispatcher_thread = Dispatcher()
        self.sender_thread = Sender()
        self.confirmation_thread = Confirmer()
        self.start()
        if len(sys.argv) > 1:
            arg_list = map(ensure_proper_quoting, sys.argv[1:])            
            arg_handler = ArgHandler(arg_list)
            run_and_exit = arg_handler.exit_after_parse
            if arg_handler.last_error is None:
                arg_handler.handle_args()
                if arg_handler.arg_result is not None:
                    print arg_handler.arg_result.replace('\\n','\n')
                self.is_daemon = arg_handler.is_daemon
            else:
                print arg_handler.last_error
    def start(self):
        self.listener_thread.start()
        self.dispatcher_thread.start()
        self.sender_thread.start()
        self.confirmation_thread.start()

class ThrowingArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParserError(message)

class ArgumentParserError(Exception): 

    strerror = None

    def __init__(self, message):
        self.strerror = message


class ArgHandler:

    config_args = None
    config_vars = None
    last_error = None
    is_local = False
    is_daemon = False
    arg_result = None
    task_uuid = None
    is_immediate = False
    exit_after_parse = False
    confirmation_aggregate = None

    def __init__(self, config_args = None):
        global run_and_exit
        self.task_uuid = uuid.uuid4()
        parser = ThrowingArgumentParser()
        parser = self.add_argument_defs(parser)
        
        try:
            if config_args is None:
                self.config_args = parser.parse_args() #parses from commandline
                # we never do this...
            else:
                self.config_args = parser.parse_args(config_args)
        except ArgumentParserError as e:
            self.config_args = 'error'
            self.last_error = e.strerror

        if self.last_error is None:
            self.config_vars = vars(self.config_args)
            self.exit_after_parse = self.config_vars['once']
            self.is_immediate = self.config_vars['block'] or self.exit_after_parse
            if self.config_vars['target'] is None:
                self.is_local = True
            if self.config_vars['daemon']:
                self.is_daemon = True

    def add_argument_defs(self, parser):
        parser.add_argument("-target", help="""[IP] ip address of target vm""") 
        parser.add_argument("-p", help="""[IP] ping the given ip""") # todo
        parser.add_argument("-discover", help="""[CIDR] - discover targets in a
                            given network.  Persistant""")
        parser.add_argument("-force_ip", help="""[IP] force the use of
                            address""")
        parser.add_argument("-cmd", help="""command to execute""")
        parser.add_argument("-send_all", action='store_true', help="""send command to all discovered
                            targets""")
        parser.add_argument("-c", help=argparse.SUPPRESS)
        parser.add_argument("-once", action='store_true', help="""run once and
                            exit""")
        parser.add_argument("-block", action='store_true', help="""block on network
                            calls""")
        parser.add_argument("-daemon", action='store_true', help="""run in daemon
                            mode""")
        parser.add_argument("-uuid", help=argparse.SUPPRESS)
        parser.add_argument("-wait_time", help="""time to wait for remote system""")
        parser.add_argument("-verbose", action='store_true', help="""be verbose""")
        parser.add_argument("-iperf_auto", action='store_true', help="""auto generate load via
                            iperf""")
        return parser

    def handle_args(self):

        if DEBUG: print 'going to handle: ', self.config_vars
        if self.is_immediate: # return results           
            if DEBUG: print 'this is immediate: ', self.config_vars
            if self.is_local:
                if DEBUG: print 'blocking on local execute'
                self.arg_result = execute_task(self.config_vars)
            else:
                self.arg_result = send_remote(self.config_vars,
                                              self.is_immediate)
        else: # qish
            if DEBUG: print 'this is qish: ', self.config_vars
            
            if self.is_local: # LOCAL
                if DEBUG: print 'this is local: ', self.config_vars
                config_vars_q.put(self.config_vars)
                if self.confirmation_aggregate:
                    self.config_vars['uuid'] = self.task_uuid.hex
                    confirmation_job = Confirmation_Job(self.config_vars, self.confirmation_aggregate)
                    confirmation_q.put(confirmation_job)       
            
            else:
                if DEBUG: print 'gotta put on send q: ', self.config_vars
                self.config_vars['uuid'] = self.task_uuid.hex
                sending_q.put(self.config_vars)

                confirmation_job = Confirmation_Job(self.config_vars,
                                                    self.confirmation_aggregate)
                
                
                confirmation_q.put(confirmation_job)       

class Confirmer(threading.Thread):
    def __init__(self):
        super(Confirmer, self).__init__()
        self.daemon = True
    
    def run(self):
        while 1:
            confirmation_job = confirmation_q.get()
            worker = threading.Thread(target=self.confirming_worker,
                                      args=(confirmation_job,))
            worker.start()
        print'Confirmer done'

    def confirming_worker(self, confirmation_job):
        while 1:
            config_vars = confirmation_job.arg_handler.config_vars.copy()
            time.sleep(confirmation_job.sleep_time)
            if is_remote_job_done(confirmation_job):
                received_message = confirmation_job.result
                transformed_message = received_message.replace('\\n', '\n')
                if confirmation_job.confirmation_aggregate is not None:
                    confirmation_job.confirmation_aggregate.final_result += transformed_message
                    confirmation_job.confirmation_aggregate.pending_confirmations.remove(confirmation_job.uuid)

                target_name = confirmation_job.target_ip if \
                    confirmation_job.target_ip else str(my_network_address)

                global run_and_exit
                if not run_and_exit:
                    print 'Host ' + target_name + ' says that job ' + confirmation_job.uuid + ' is done with result: \n' + transformed_message
                break
            
            else:
                confirmation_job.arg_handler.config_vars = config_vars.copy()

class Confirmation_Aggregate:
    
    final_result = ''
    pending_confirmations = []

    def __init__(self):
        self.final_result = ''

class Confirmation_Job:

    uuid = None
    target_ip = None
    sleep_time = 5
    result = ''
    arg_handler = None
    confirmation_aggregate = None

    def __init__(self, config_vars, confirmation_aggregate = None):
        self.uuid = config_vars['uuid']
        self.target_ip = config_vars['target']
        self.arg_handler = self.create_arg_handler(config_vars)
        self.confirmation_aggregate = confirmation_aggregate

    def create_arg_handler(self, config_vars):
        if self.target_ip:
            msg = "-target " + config_vars['target'] + " " + "-c " + config_vars['uuid'] + " " + "-block"
        else:
            msg = "-c " + config_vars['uuid'] + " " + "-block"
        arg_message = split_message(msg)
        return ArgHandler(arg_message)


class Sender(threading.Thread):
    def __init__(self):
        super(Sender, self).__init__()
        self.daemon = True
        
    def run(self):
        while 1:
            config_vars = sending_q.get()
            if DEBUG: print 'got from q: ', config_vars
            worker = threading.Thread(target=self.sending_worker,
                                      args=(config_vars,))
            worker.start()
        print 'Sender done'

    def sending_worker(self, config_vars):
        remote_result = send_remote(config_vars)
        print remote_result


class Listener(threading.Thread):
   
    def __init__(self):
        super(Listener, self).__init__()
        self.daemon = True

    def run(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', LISTEN_PORT))
        s.listen(1)
        while 1:
            conn, addr = s.accept()
            if DEBUG: print 'Connected by', addr
            data = recv_impl(conn)
            try:
                config_vars = json.loads(data)
            except ValueError:
                continue
            if DEBUG: print 'heard this message '
            if DEBUG: print config_vars
                
            arg_message= split_message(self.flatten_vars(config_vars))
            arg_handler = ArgHandler(arg_message)
            if arg_handler.last_error is None:
                arg_handler.handle_args()
                if arg_handler.arg_result is not None:
                   send_impl(conn, arg_handler.arg_result)
                else:
                    send_impl(conn, 'OK')
            else:
                print 'Bad input from remote client: ', arg_handler.last_error
                conn.sendall(arg_handler.last_error)
                if DEBUG: print 'closing the connection'
            conn.close()

        print 'Listener done'

    def flatten_vars(self, config_vars):
        str = ""
        for key in config_vars:
            val = config_vars[key]
            if val is None:
                continue
            if isinstance(val, bool):
                if (val):
                    val = ''
                else:
                    continue
            else:
                if " " in val and "\"" not in val :
                    val = "\"" + val + "\"" 
            str += "-"
            str += key
            str += " "
            str += val
            str += " "
        return str 


class Dispatcher(threading.Thread):
    'takes a task off of the q and executes it'
   
    def __init__(self):
        super(Dispatcher, self).__init__()
        self.daemon = True
    def run(self):
        while 1:
            config_vars = config_vars_q.get()
            worker = threading.Thread(target=self.dispatch_worker,
                                      args=(config_vars,))
            worker.start()
        print 'Dispatcher done'

    def dispatch_worker(self, config_vars):
        global result_dict
        result = execute_task(config_vars)

        if config_vars['verbose']:
            print result
        
        result_dict[config_vars['uuid']] = result





##################################
# Helpers
##################################


def execute_task(config_vars):
    command_result = 'None'

    command = unquote_string(config_vars['cmd'])


    if command is not None:
       
        if config_vars['send_all']:

            if config_vars['discover'] is not None:
                populate_targets(config_vars['discover'])
            time_to_wait = config_vars['wait_time'] if config_vars['wait_time'] else DEFAULT_WAIT_TIME
           
            client_time = int(time_to_wait) + 20
    
            
            confirmation_aggregate = Confirmation_Aggregate()
           
             
            for target in network_targets:

                msg = "-target " + str(target) + " -cmd " + command + " -wait_time " + str(time_to_wait)
                if DEBUG: print 'going to handle ',msg 
                arg_message = split_message(msg)
                arg_handler = ArgHandler(arg_message)
                confirmation_aggregate.pending_confirmations.append(arg_handler.task_uuid.hex)
                arg_handler.confirmation_aggregate = confirmation_aggregate
                arg_handler.handle_args()
            while confirmation_aggregate.pending_confirmations:
                time.sleep(5)
        
            command_result = confirmation_aggregate.final_result
        else:    
        
            if DEBUG: print 'going to execute: ', command        
            args = shlex.split(command)
            p = subprocess.Popen(args, stdout=subprocess.PIPE, bufsize=1)
    
            time_to_wait = config_vars['wait_time'] if config_vars['wait_time'] else DEFAULT_WAIT_TIME
    
            time.sleep(float(time_to_wait))
            output_lines = []
            p.terminate()   
    
            for line in p.stdout:
                output_lines.append(line)
            command_result = ''.join(output_lines)
    
    elif config_vars['c'] is not None:

        if config_vars['c'] in result_dict:
            command_result = result_dict[config_vars['c']]
        else:
            command_result = "-"
    
    elif config_vars['iperf_auto']:
        
        if config_vars['discover'] is not None:
            populate_targets(config_vars['discover'])

        server_address = config_vars['force_ip'] if config_vars['force_ip'] else str(my_network_address)


        time_to_wait = config_vars['wait_time'] if config_vars['wait_time'] else DEFAULT_WAIT_TIME
       
        client_time = int(time_to_wait) + 20

        server_command = '"iperf -s -print_mss -p 5001 "'
        client_command = '"iperf -c ' + server_address + ' -t ' + str(client_time) + ' -p 5001 "'
        
        confirmation_aggregate = Confirmation_Aggregate()
       
        msg = "-cmd " + server_command + " -wait_time " + str(time_to_wait)
        arg_message = split_message(msg)
        arg_handler = ArgHandler(arg_message)
        confirmation_aggregate.pending_confirmations.append(arg_handler.task_uuid.hex)
        arg_handler.confirmation_aggregate = confirmation_aggregate
        arg_handler.handle_args()

        for target in network_targets:
            msg = "-target " + str(target) + " -cmd " + client_command + " -wait_time " + str(time_to_wait)
            arg_message = split_message(msg)
            arg_handler = ArgHandler(arg_message)
            confirmation_aggregate.pending_confirmations.append(arg_handler.task_uuid.hex)
            arg_handler.confirmation_aggregate = confirmation_aggregate
            arg_handler.handle_args()

        
        while confirmation_aggregate.pending_confirmations:
            time.sleep(5)
        
        command_result = confirmation_aggregate.final_result
    
    elif config_vars['discover'] is not None:
        populate_targets(config_vars['discover'])
        command_result = "network discovery complete."


    else:
        if DEBUG: print '***nothing to do'
        pass    

    return 'Final Result: ' + command_result


def populate_targets(network_definition):
    # 3.3 would be nice here...
    # we'll install netaddr instead - but must be install on all
    # clients!

    networks = network_definition.split('_')
    for network in networks:
        network = IPNetwork(network)
        all_hosts_list = list(network)

        host_list = [host for host in all_hosts_list if host !=
                 my_network_address]
        for host in host_list:
            if DEBUG: print 'trying: ', host
            if host_is_alive(host):
                if host not in network_targets:
                    network_targets.append(host)

    if DEBUG: print network_targets


def is_remote_job_done(confirmation_job):
    is_done = False
    if confirmation_job.target_ip is not None:
        remote_answer = send_remote(confirmation_job.arg_handler.config_vars)
        is_done = remote_answer != "'-'"
        if is_done:
            confirmation_job.result = remote_answer

    elif confirmation_job.arg_handler.config_vars['c'] in result_dict:
        confirmation_job.result = result_dict[confirmation_job.arg_handler.config_vars['c']]
        is_done = True

    return is_done 


def host_is_alive(target):
    msg = "-target " + str(target)
    arg_message = split_message(msg)
    arg_handler = ArgHandler(arg_message)
    send_response = send_remote(arg_handler.config_vars, False)
    return send_response == "'OK'"
       
def get_ip_address():
    ifcs = ['eth0','enp0s3']
    for ifc in ifcs:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            return socket.inet_ntoa(fcntl.ioctl(
                s.fileno(),
                0x8915,  # SIOCGIFADDR
                struct.pack('256s', ifc[:15])
            )[20:24])
        except:
            pass



def split_message(msg):
# when given a cla string, returns a list of strings
# where each element in the list is either an option 
# (for example '-i'), or a quoted argument (for 
# example "ls -lat")

# this list is strings is the data structure that
# the ArgParser module expects, and is given
# to the constructor of our ArgHandler object

# replace with shlex?

    msg_list = msg.split() 
    count = len(msg_list)
    newlist = []
    i = 0

    while i < count:

        if not "\"" in msg_list[i]:
            newlist.append(msg_list[i])
        else:
            quoted_string = (msg_list[i] + ' ')
            i += 1
            while not "\"" in msg_list[i]:
                quoted_string += (msg_list[i] + ' ')
                i += 1
            quoted_string += msg_list[i]
            newlist.append(quoted_string)
            
        i += 1

    return newlist

def ensure_proper_quoting(str):
    # given a string, return the same string
    # wrapped in double quotes if the string
    # contains a space, and replace all single
    # quotes with escaped single quotes

    # only used when parsing the command line params
    # given when starting the tool

    # this func is map'd on the array of args from the command line

    str = re.sub('\'','\'', str)
    if " " in str and "\"" not in str:
        str = "\"" + str + "\""
    return str

def unquote_string(quoted_string):
    
    # when we execute the contents of the -cmd argument
    # we first need to remove the double quotes
    
    if quoted_string is None:
        return quoted_string
    if quoted_string.startswith('"') and quoted_string.endswith('"'):
        quoted_string = quoted_string[1:-1]
    return quoted_string

def requote_string(quoted_string):
    
    # never used
        # an argument had outer double quotes, and all
        # other quotes being single.  when that arg is processed
        # the outer quotes are removed, leaving an even remaining
        # number of single quotes.  the outer most single quotes
        # should be changed to double quotes.

    
    quoted_string = re.sub('\"','',quoted_string)
    quote_list = [i for i, char in enumerate(quoted_string) if char ==
                    '\'']
    first_quote_index = quote_list[0]
    last_quote_index = quote_list[-1]
    quoted_string_list = list(quoted_string)
    quoted_string_list[first_quote_index] = '\"'
    quoted_string_list[last_quote_index] = '\"'
    quoted_string = "".join(quoted_string_list)
    
    return quoted_string

def convert_to_bytes(no):
    result = bytearray()
    result.append(no & 255)
    for i in range(3):
        no = no >> 8
        result.append(no & 255)
    return result
                          
def bytes_to_number(b):
    b = map(ord, b)
    res = 0                                                       
    for i in range(4):
      res += b[i] << (i*8)
    return res

def send_remote(config_vars, is_immediate = False):
    target_ip = config_vars['target']
    if DEBUG: print "***send remote config vars: ", config_vars
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if is_immediate:
            timeout_val = 360
        else:
            timeout_val = .1
        s.settimeout(timeout_val)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.connect((target_ip, LISTEN_PORT))
    except socket.timeout:
        return 'timeout!'
    except socket_error as serr:
        if serr.errno == errno.ECONNREFUSED:
            return 'Connection Refused...remote not running?'
        elif serr.errno == errno.ENETUNREACH:
            return 'Network Unreachable'
        else:
            raise serr
    del config_vars['target']
    config_json = json.dumps(config_vars)
    send_impl(s, config_json)
    data = recv_impl(s)
    s.close()
    return repr(data)




def send_impl(s, data):
    length = len(data)
    s.send(convert_to_bytes(length))
    s.sendall(data)

def recv_impl(s):
    length = s.recv(4)
    length = bytes_to_number(length)
    received_message = recvall(s, length)
    return received_message


def recvall(s, count):
    buf = b''
    while count:
        newbuf = s.recv(count)
        if not newbuf: return None
        buf += newbuf
        count -= len(newbuf)
    return buf

def kill_proc(process_name):
    proc = subprocess.Popen(["pgrep", process_name], stdout=subprocess.PIPE)
    for pid in proc.stdout:
        os.kill(int(pid), signal.SIGTERM)
        try: 
            os.kill(int(pid), 0)
        except OSError as ex:
            continue




###################################
# Script 
###################################

if __name__ == "__main__":
 
    kill_proc('iperf')

    controller = Controller()

    my_network_address = IPAddress(get_ip_address())

    if controller.is_daemon:
        print 'starting in daemon mode'
        controller.listener_thread.join()
        controller.dispatcher_thread.join()
        controller.sender_thread.join()
        controller.confirmation_thread.join()

    elif not run_and_exit:
        while True:
            msg = raw_input("> ")
            if msg == '':
                continue
            arg_message = split_message(msg)
            arg_handler = ArgHandler(arg_message)
            if arg_handler.last_error is None:
                arg_handler.handle_args()
                if arg_handler.arg_result is not None:
                    print arg_handler.arg_result
            else:
                print arg_handler.last_error
   
# app does exit until it gets a signal
