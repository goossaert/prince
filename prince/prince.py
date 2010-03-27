"""
Prince distributed computing module.
"""
__docformat__ = "restructuredtext en"

## Copyright (c) 2010 Emmanuel Goossaert 
##
## This file is part of Prince, an extra-light Python module to run
## MapReduce tasks in the Hadoop framework. MapReduce is a patented
## software framework introduced by Google, and Hadoop is a registered
## trademark of the Apache Software Foundation.
##
## Prince is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## Prince is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Prince.  If not, see <http://www.gnu.org/licenses/>.


import os
import sys

import dfs
import job
import config


def get_parameters_all():
    """
    Return a dictionary of ALL parameters of the command line.

    :Return:
        All parameters of the command line, *including* the API's internal
        parameters. Each entry is a list of strings, each string being one
        of the values for a parameter.

    :ReturnType:
        Dictionary of list of strings.
    """
    params = {}
    is_value = False
    for index, value in enumerate(sys.argv):
        if value.startswith('--'):
            if (index + 1) < len(sys.argv) and not sys.argv[index + 1].startswith('-'):
                content = sys.argv[index + 1]
            else:
                content = None
            params[value[2:]] = content
            is_value = True
        elif is_value:
            is_value = False
    return params


params = {} # global to mimic static variable behavior
def get_parameters(*args):
    """
    Return the parameters passed to the mapper and reducer tasks through
    the run() method. When used, it has to be called in the mapper and
    reducer methods.

    :Parameters:
        *args : strings 
            List of the parameters to search for. See the Examples section.

    :Return:
        The value of the parameters of which the names have been given
        in parameter.

    :ReturnType:
        String or tuple of strings

    :Examples:
        param1_value = get_parameters('param1')
        (param1_value, param2_value) = get_parameters('param1', 'param2')
    """
    global params
    if not params:
        params = get_parameters_all()
        for name in [config.option_mapper, config.option_reducer]:
            if name in params:
                del params[name]
 
    ret = []
    for arg in args:
        if arg in params:
            ret.append(params[arg])
        else:
            ret.append(None)

    return ret[0] if len(ret) == 1 else tuple(ret)


def inspect_methods(filename):
    """
    Return a generator of all functions of a given file.

    :Parameters:
        filename : string
            Name of the file in which to inspect.

    :Return:
        Methods found in the given file.

    :ReturnType:
        Generator of methods.
    """
    import inspect
    file = __import__(inspect.getmodulename(filename))
    for name in dir(file):
        obj = getattr(file, name)
        if inspect.isfunction(obj):
            yield obj


def find_method(filename, methodname):
    """
    Search for a method in a given file.

    :Parameters:
        filename : string
            Name of the file in which to search for.
        methodname : string
            Name of the method to look for.

    :Return:
        The method if it is found, None otherwise.

    :ReturnType:
        method
    """
    for method in inspect_methods(os.path.basename(filename)):
        if method.__name__ == methodname:
            return method
    return None
 

def get_task():
    """
    Return task type and name from the parameters of the command line.

    :Return:
        Task type and name if one of the tasks mapper or reducer is found
        in the parameters, None otherwise.

    :ReturnType:
        Tuple of two strings, the task type and the task name.
    """
    params = get_parameters_all()
    for task in [config.option_mapper, config.option_reducer]:
        if task in params:
            return task, params[task]
    return None, None



def get_errorfile(tracefile=None):
    """
    Get an available error path on the DFS.

    :Parameters:
        tracefile : string
            Basename for the trace file

    :Return:
        Available file name where to put the trace
    
    :ReturnType:
        String
    """
    errno = 0
    while True:
        filepath = '%s%d' % (tracefile, errno)
        if not dfs.exists(filepath):
            return filepath
        errno += 1


def handle_exception(tracefile):
    """
    Write the last traceback to the given file on the DFS.

    :Parameters:
        tracefile : string
            Name of the file where to save the traceback on the DFS
    """
    import traceback
    type, value, trace = sys.exc_info()
    message = traceback.format_exception(type, value, trace)
    errorfile = get_errorfile(tracefile)
    dfs.write(errorfile, ''.join(message))


def cleanup_parameters(names):
    """
    Cleanup sys.argv from certain parameters to that program behavior remain
    the same when testing len(sys.argv) even though some parameters have been
    added.

    :Parameters:
        names : string or list of strings
            Names of the parameters to take off of sys.argv
    """
    names = names if isinstance(names, list) else [names]
    indices = []
    is_value = False
    for index, value in enumerate(sys.argv):
        if value.startswith('--') and value[2:] in names or is_value:
            is_value = not is_value
            indices.append(index)

    for index in reversed(indices):
        del sys.argv[index]


def init():
    """
    Initializer function, that *have* to be called as early as possible in the
    '__main__' section of the calling program. It ensures that all accesses to
    mapper and reducer functions are intercepted.
    NOTE: The function is called 'init' so that people who don't want to get
    into these details won't get confused with fancy method names.
    """
    global filename_caller, filename_trace
    filename_caller = sys.argv[0]
    filename_trace  = get_parameters('trace')
    if filename_trace: # Must be done before the test of task type
        cleanup_parameters('trace')

    tasktype, taskname = get_task()
    if not tasktype: return # This is the main program

    
    method = find_method(filename_caller, taskname)
    if method:
        tasks = {config.option_mapper:  job.mapper_wrapper,
                 config.option_reducer: job.reducer_wrapper }
        try:
            tasks[tasktype](method)
        except:
            if filename_trace:
                handle_exception(filename_trace)
            raise # re-raise the exception so that the task fail
        sys.exit(0)


def run_program(commandline, options=None):
    """
    Run a program with the given command line and options.

    :Parameters:
        commandline : string
            Command line to use for called the program.
        options : dictionary
            Dictionary of the options to use to complete the command line.

    :Return:
        The return of the program called.

    :ReturnType:
        String.
    """
    if options == None: options = {}
    child = os.popen(commandline % options)
    return child.read()


def quote_list(content, quote_mark='\''):
    """
    Quote the item of a list.

    :Parameters:
        content : list of strings
            Files to read from on the DFS.
        quote_mark : string
            Character or string used to encapsulate every item of the list.

    :Return:
        List of quoted items.

    :ReturnType:
        List of strings.
    """
    return [quote_mark + c + quote_mark for c in content]


def parameter_dict_to_command(parameters, quote_mark='"'):
    """
    Create a string of parameters for command-line use from a dictionary.

    :Parameters:
        parameters : dictionary
            Parameters, each value being a string or list of strings.
        quote_mark : string
            Character or string used to encapsulate every value of an option.

    :Return:
        Parameters.

    :ReturnType:
        string
    """
    options_list = []
    for (key, values) in parameters.items():
        if not isinstance(values, list): values = [values]
        for v in values:
            options_list.append('--' + key)
            options_list.append(quote_mark + str(v) + quote_mark)
    return ' '.join(options_list)


def get_path_package():
    """Get the location of the egg package."""
    for path in sys.path:
        if path.endswith('egg') and 'prince' in path:
            return path
    return None


def run(mapper,
        reducer,
        inputs,
        output,
        files=None,
        parameters=None,
        inputformat='auto',
        outputformat='auto'):
    """
    Run a MapReduce task using Hadoop Streaming.

    :Parameters:
        mapper : method
            Mapper method. The prototype has to be map(key, value), and 'key'
            and 'value' will be filled with the data read from the specified
            input files. 'key' and 'value' are strings.
        reducer : method
            Reducer method. The prototype has to be reduce(key, values),
            and 'key' and 'values' will be filled with the data read from the
            mapper task. 'key' is a string and 'values' is a list of strings.
        inputs : string or list of strings
            Paths to the files for the mapper read from on the DFS.
        output : string
            Name of the file for the reducer to write on the DFS.
        files : list of strings
            Names of the files to be included in the path of the mapper and
            reducer methods. All file used by the program, and imported python
            files must be specified here.
        parameters : dictionary
            Dictionary of options to pass to the mapper and reducer methods.
            Each key is the name of the parameter, and the value is value of
            the parameters. If a list of string is given as a value, then all
            these values will be passed to the mapper and reducer tasks.
        inputformat : string
            Format of the input files. Can be either 'text' or 'auto', default
            is 'auto'.
        outputformat : string
            Format of the output file. Can be either 'text' or 'auto', default
            is 'auto'.

    :Return:
        Return of the Hadoop task called.

    :ReturnType:
        String
    """
    if files == None: files = []
    if parameters == None: parameters = {}

    global filename_trace
    if filename_trace:
        parameters['trace'] = filename_trace

    # TODO: Check if all necessary files exist?
    if not isinstance(inputs, list): inputs = [inputs]
    if not isinstance(files, list): files= [files]

    global filename_caller
    files.append(os.path.join(filename_caller))
    #filename_module = os.path.splitext(os.path.abspath(__file__))[0] + '.py'
    #files.append(os.path.join(filename_module))

    path_package = get_path_package()
    if path_package:
        files.append(path_package)

    options = parameter_dict_to_command(parameters)

    pattern_command  = '\'python -m %s --%s %s %s\'' 
    filename_program = os.path.splitext(os.path.basename(filename_caller))[0]
    command_mapper   = pattern_command % (filename_program, config.option_mapper, mapper.__name__, options)
    command_reducer  = pattern_command % (filename_program, config.option_reducer, reducer.__name__, options)

    options = {'path':         config.mapreduce_path,
               'mapreduce':    config.mapreduce_program,
               'streaming':    config.mapreduce_streaming,
               'inputs':       ' -input '.join([''] + inputs),
               'output':       ' -output ' + output,
               'mapper':       '-mapper ' + command_mapper,
               'reducer':      '-reducer ' + command_reducer,
               'files':        ' -file '.join([''] + quote_list(files)),
               'env':          '-cmdenv PYTHONPATH=./%s' % os.path.basename(path_package),
               'inputformat':  '-inputformat \'%s\'' % config.inputformats[inputformat],
               'outputformat': '-outputformat \'%s\'' % config.outputformats[outputformat]
              }

    commandline = '%(mapreduce)s jar %(path)s%(streaming)s %(inputs)s %(output)s %(mapper)s %(reducer)s %(files)s %(env)s %(inputformat)s %(outputformat)s'

    # TODO: Put this in a logger
    print 'EXECUTE:'
    print commandline % options

    content = run_program(commandline, options)
    return content

