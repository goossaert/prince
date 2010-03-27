"""
Prince job handling module.
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

import sys


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

    # As Prince uses Hadoop streaming, input data come from the standard input
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
    # As Prince uses Hadoop streaming, input data come from the standard input
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
