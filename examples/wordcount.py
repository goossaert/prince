#!/usr/bin/env python
"""
Word count example using Babar.
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



import os
import sys
import babar

def wc_mapper(key, value):
    """Mapper function, where 'key' and 'value' are strings."""
    for word in value.split():
        yield word, 1


def wc_reducer(key, values):
    """Reducer function, where 'key' is a string and 'values' a list of strings."""
    try:
        values = [int(v) for v in values]
        yield key, sum(values)
    except ValueError:
        # discard non-numerical values
        pass


if __name__ == "__main__":
    # Always call babar.init() at the beginning of the program
    babar.init()

    input  = 'logs/*' # change to input data on the DFS
    output = 'count'

    # Run the task with specified mapper and reducer methods
    babar.run(wc_mapper, wc_reducer, input, output, inputformat='text', outputformat='text')

    # Read the output file and print it 
    file = babar.dfs_read(count + '/part*')
    print file
