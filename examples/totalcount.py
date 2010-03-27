#!/usr/bin/env python
"""
Count all the items in a data set.

The one iteration solution that consists in reducing all items to the same key
requires a heavy work load from only one reducer. This solution balances the
work load amongst possibly multiple reducers, however it requires two iterations.
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


def count_mapper(key, value):
    """
    Distribute the values over keys, equality enough to average computational
    complexity for the reducers. By using a modulo, we are sure to balance the
    number of values for each key in the reducers. Therefore, this method
    avoids the case where a single reducer faces the whole data set.
    """
    nb_buckets = 100 # this value is correct for small- and medium-sized
                     # data sets, but should be adjusted to each case
    key = int(key)
    for index, item in enumerate(value.split()):
        yield (key + index) % nb_buckets, 1


def count_reducer(key, values):
    """Sum up the items of same key"""
    try:                yield key, sum([int(v) for v in values])
    except ValueError:  pass # discard non-numerical values


def sum_mapper(key, value):
    """Map all intermediate sums to same key"""
    (index, count) = value.split()
    yield 1, count


def count_items(input, output):
    """Sum all the items in the input data set"""
    # Intermediate file name
    inter = output + '_inter'

    # Run the task with specified mapper and reducer methods
    prince.run(count_mapper, count_reducer, input, inter, inputformat='text', outputformat='text', files=__file__)
    prince.run(sum_mapper, count_reducer, inter + '/part*', output, inputformat='text', outputformat='text', files=__file__)

    # Read the output file and print it 
    file = prince.dfs.read(output + '/part*', first=1)
    return int(file.split()[1])


def display_usage():
    print 'usage: %s input output' % sys.argv[0]
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

    # Count all items in the input data set and print the result
    print 'Total items:', count_items(input, output)
