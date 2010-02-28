"""
Babar distributed computing module.
"""
__docformat__ = "restructuredtext en"

## Copyright (c) 2010 Emmanuel Goossaert 
##
## This file is part of Babar, an extra-light Python module to run
## MapReduce tasks in the Hadoop framework. MapReduce is a patented
## software framework introduced by Google, and Hadoop is a registered
## trademark of the Apache Software Foundation.
##
## Babar is free software; you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation; either version 3 of the License, or
## (at your option) any later version.
##
## Babar is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with Babar.  If not, see <http://www.gnu.org/licenses/>.

__all__ = ['init', 'run', 'get_parameters', 'dfs_read']
import os
import sys


# TODO: put all constants into a configuration file.
inputformats = {'text': 'org.apache.hadoop.mapred.TextInputFormat',
                'auto': 'org.apache.hadoop.streaming.AutoInputFormat'}

outputformats = {'text': 'org.apache.hadoop.mapred.TextOutputFormat',
                 'auto': 'org.apache.hadoop.mapred.SequenceFileOutputFormat'}

mapreduce_path      = os.environ.get('HADOOP_HOME') + '/'
mapreduce_program   = mapreduce_path + 'bin/hadoop'

mapreduce_streaming = 'contrib/streaming/hadoop-0.20.1-streaming.jar'

option_mapper  = 'bmapper'
option_reducer = 'breducer'
separator = '\t'


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
    for index, value in enumerate(sys.argv[1:]):
        if index % 2 == 0:
            params[value[2:]] = params.get(value[2:], []) + [sys.argv[index + 2]]
    return params


def get_parameters():
    """
    Return the parameters passed to the mapper and reducer tasks through
    the run() method. When used, it has to be called in the mapper and
    reducer methods.

    :Return:
        Each entry is a list of strings, each string being one of the values
        for a parameter.

    :ReturnType:
        Dictionary of list of strings.
    """
    params = get_parameters_all()
    for name in [option_mapper, option_reducer]:
        if name in params:
            del params[name]
    return params


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
    for task in [option_mapper, option_reducer]:
        if task in params:
            return task, params[task][0]
    return None, None


def get_errorfile(filetrace=None):
    """
    Get an available error path on the DFS

    :Parameters:
        filetrace : string
            Basename for the trace file

    :Return:
        Avaible file name where to put the trace
    
    :ReturnType:
        String
    """
    errno = 0
    while True:
        options = {'mapreduce': mapreduce_program,
                   'filename':  filetrace,
                   'errno':     errno}
        # TODO: very slow, so implement the found test with another technique
        found = run_program('%(mapreduce)s dfs -ls %(filename)s%(errno)d', options)
        if not found:
            return '%(filename)s%(errno)d' % options
        errno += 1


def dfs_write(filename, content):
    """
    Write text to a file on the DFS

    :Parameters:
        filename : string
            File name where to write the text on the DFS
        content : string or list of two-item tuples
            If it is a string, the text is just written as it is.
            If it is a list of tuples, each tuple is written as a MapReduce
            entry (key, value), separated by the default separator.

    :Examples:
        dfs_write('foo', 'String of text')
        dfs_write('foo', (0, 0))
        dfs_write('foo', [(0, 1), (1, 1)])
    """
    #print 'content', content
    if not isinstance(content, str):
        if not isinstance(content, list):
            content = [content]
        content = ''.join(['%s%s%s' % (str(item[0]), '\t', str(item[1])) for item in content])
    options = {'content':   content,
               'mapreduce': mapreduce_program,
               'filename':  filename }
    run_program('echo "%(content)s" | %(mapreduce)s dfs -put - %(filename)s', options)



def handle_exception(filetrace):
    """
    Write the last traceback to the given file on the DFS.

    :Parameters:
        filetrace : string
            Name of the file where to save the traceback on the DFS
    """
    import traceback
    type, value, trace = sys.exc_info()
    message = traceback.format_exception(type, value, trace)
    errorfile = get_errorfile(filetrace)
    options = {'mapreduce': mapreduce_program,
               'message':   ''.join(message),
               'errorfile': errorfile}
    run_program('echo "%(message)s" | %(mapreduce)s dfs -put - %(errorfile)s', options)


def get_tracefile():
    """Get tracefile name from parameters passed to mappers and reducers"""
    option = 'tracefile'
    params = get_parameters()
    if option in params:
        return params[option][0]
    return None


def init(program=sys.argv[0], tracefile=None):
    """
    Initializer function, that *have* to be called as early as possible in the
    '__main__' section of the calling program. It ensures that all accesses to
    mapper and reducer functions are intercepted.
    NOTE: The function is called 'init' so that people who don't want to get
    into these details don't get confused with fancy method names

    :Parameters:
        program : string
            Name of the program from which Babar is included.
            Default is sys.argv[0].
        tracefile : string
            Base name of the file on the DFS where to write the traceback in
            case an exception is caught in a mapper/reducer when debugging.
            By default it is desactivated.
    """
    global filename_caller, filename_trace
    filename_caller = program
    filename_trace  = tracefile

    tasktype, taskname = get_task()
    if not tasktype: return

    method = find_method(filename_caller, taskname)
    if method:
        tasks = {option_mapper:  mapper_wrapper,
                 option_reducer: reducer_wrapper }
        try:
            tasks[tasktype](method)
        except:
            tracefile = get_tracefile()
            if tracefile:
                handle_exception(tracefile)
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
    #print 'command:', commandline % options
    child = os.popen(commandline % options)
    return child.read()


def dfs_read(files, first=None, last=None):
    """
    Read the content of files on the DFS. Multiple files can be specified,
    and it is possible to read only n lines at the beginning or at the end
    of the file. 'first' and 'last' being exclusive parameters, if both of
    them are used then only 'first' is used.

    :Parameters:
        files : string or list of strings
            Files to read from on the DFS.
        first : int
            Number of lines to read at the beginning of the file
        last : int
            Number of lines to read at the end of the file

    :Return:
        Lines of the file(s) on the DFS.

    :ReturnType:
        List of strings.
    """
    if not isinstance(files, list): files = [files]
    options = {'mapreduce': mapreduce_program,
               'files':     ' '.join(files) }
    if first:   truncate = ' | head -n %s' % first
    elif last:  truncate = ' | tail -n %s' % last
    else:       truncate = ''
    commandline = '%(mapreduce)s dfs -cat %(files)s' + truncate
    return run_program(commandline, options)


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
            Input format of the input files. Can be either 'text' or 'auto',
            default is 'auto'.
        outputformat : string
            Output format of the output file. Can be either 'text' or 'auto',
            default is 'auto'.

    :Return:
        Return of the Hadoop task called.

    :ReturnType:
        String
    """
    if files == None: files = []
    if parameters == None: parameters = {}

    global filename_trace
    if filename_trace:
        parameters['tracefile'] = filename_trace

    # TODO: Check if all necessary files exist?
    if not isinstance(inputs, list): inputs = [inputs]
    if not isinstance(files, list): files= [files]

    global filename_caller
    files.append(os.path.join(filename_caller))
    filename_module = os.path.splitext(os.path.abspath(__file__))[0] + '.py'
    files.append(os.path.join(filename_module))

    options = parameter_dict_to_command(parameters)

    pattern_command  = '\'python -m %s --%s %s %s\'' 
    filename_program = os.path.splitext(os.path.basename(filename_caller))[0]
    command_mapper   = pattern_command % (filename_program, option_mapper, mapper.__name__, options)
    command_reducer  = pattern_command % (filename_program, option_reducer, reducer.__name__, options)

    options = {'path':         mapreduce_path,
               'mapreduce':    mapreduce_program,
               'streaming':    mapreduce_streaming,
               'inputs':       ' -input '.join([''] + inputs),
               'output':       ' -output ' + output,
               'mapper':       '-mapper ' + command_mapper,
               'reducer':      '-reducer ' + command_reducer,
               'files':        ' -file '.join([''] + quote_list(files)),
               'inputformat':  '-inputformat \'%s\'' % inputformats[inputformat],
               'outputformat': '-outputformat \'%s\'' % outputformats[outputformat]
              }

    commandline = '%(mapreduce)s jar %(path)s%(streaming)s %(inputs)s %(output)s %(mapper)s %(reducer)s %(files)s %(inputformat)s %(outputformat)s'

    # TODO: Put this in a logger
    print 'EXECUTE:'
    print commandline % options

    content = run_program(commandline, options)
    return content


def read_input_reducer(file, separator='\t'):
    """
    Prepare the input for the reducer.

    :Parameters:
        file : file descriptor
            File to read from.
        separator : string
            Character or string used to split the key from the value.

    :Return:
        Lines read from the descriptor.

    :ReturnType:
        Generator of strings.
    """
    for line in file:
        yield line.rstrip().split(separator, 1)


def valuesof(items):
    for k, v in items:
        yield v


def reducer_wrapper(reducer_fct, separator='\t'):
    """
    General reducer function, that call reducer_fct() to perform
    the reducing job on a items of same key. Results are printed
    to the standard output.

    :Parameters:
        reducer_fct : method
            Reducer method to call on each tuple (<key>, (<value>, ...)).
        separator : string
            Character or string used to split the key from the value.
    """
    from itertools import groupby
    from operator import itemgetter

    # As Babar uses Hadoop streaming, input data come from the standard input
    data = read_input_reducer(sys.stdin, separator=separator)

    # groupby() groups items by key, and creates an iterator on the items
    #   key:   key of the current item
    #   items: iterator yielding all ['<key>', '<value>'] items
    for (key, items) in groupby(data, itemgetter(0)):
        #if not key: continue  # in case of invalid key
        pairs =  reducer_fct(key, valuesof(items))
        if pairs:
            if isinstance(pairs, tuple):
                # Simple tuple, so we make it a tuple in a list
                pairs = [pairs]
            for (key_r, value_r) in pairs:
                print "%s%s%s" % (str(key_r), separator, str(value_r).rstrip())


def read_input_mapper(file):
    """
    Create a generator from a file descriptor, needed by the mappers.

    :Parameters:
        file : file descriptor
            File to read from.

    :Return:
        Lines read from the descriptor.

    :ReturnType:
        Generator of strings.
    """
    for line in file:
        yield line

 
def mapper_wrapper(mapper_fct, separator='\t'):
    """
    General mapper function, that call mapper_fct() to perform
    the mapping job on a single item.

    :Parameters:
        mapper_fct : method
            Mapper method to call on each tuple (<key>, <value>).
        separator : string
            Character or string used to split the key from the value.
    """
    # As Babar uses Hadoop streaming, input data come from the standard input
    data = read_input_mapper(sys.stdin)
    key = 0
    for line in data:
        pairs = mapper_fct(str(key), line.rstrip())
        if pairs:
            if isinstance(pairs, tuple):
                # Simple tuple, so we make it a tuple in a list
                pairs = [pairs]
            for (key_m, value_m) in pairs:
                # Special case to get sequential keys
                if key_m == None:   key_m = key
                print '%s%s%s' % (str(key_m), separator, str(value_m).rstrip())
                key += 1
