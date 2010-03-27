#!/usr/bin/env python
"""
Word count example using Prince.
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
import prince


def wc_mapper(key, value):
    """Mapper method with 'key' and 'value' as strings"""
    for word in value.split():
        yield word, 1


def wc_reducer(key, values):
    """Reducer method with 'key' a string and 'values' a generator of strings"""
    try:                yield key, sum([int(v) for v in values])
    except ValueError:  pass # discard non-numerical values


def display_usage():
    print 'usage: ./%s input output' % sys.argv[0]
    print '  input: input file on the DFS'
    print '  output: output file on the DFS'


if __name__ == "__main__":
    # Always call prince.init() at the beginning of the program
    prince.init()

    if len(sys.argv) != 3:
        display_usage()
        sys.exit(0)

    input  = sys.argv[1]
    output = sys.argv[2]

    # Run the task with specified mapper and reducer methods
    prince.run(wc_mapper, wc_reducer, input, output, inputformat='text', outputformat='text')

    # Read the output file and print it 
    file = prince.dfs.read(output + '/part*')
    print file
