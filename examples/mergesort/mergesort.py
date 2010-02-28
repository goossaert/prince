#!/usr/bin/env python
"""
Distributed merge sort on integer numbers using MapReduce/Hadoop.

Quite inefficient because of Python, because Prince handles keys and values as
strings, and because reducers have to handle increasing number of values as
the sort goes on. But it is interesting as it shows how to code such a sort
algorithm using the MapReduce paradigm.
O(n lg n) iterations are necessary as for any merge sort. Two additional
iterations are used, one to detect termination, and another one to write
the result file.
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


def str_to_int(strings):
    """Transform a list of string into a list of integers"""
    try:                ret = [int(v) for v in strings]
    except ValueError:  ret = []
    return ret


def int_to_str(ints):
    """Transform a list of integers into a list of strings"""
    return [str(i) for i in ints]


def merge_lists(list1, list2):
    """Merge two sorted lists of integers"""
    list1.append(sys.maxint)
    list2.append(sys.maxint)
    index1 = 0
    index2 = 0

    list_sorted = []
    for i in range(len(list1) + len(list2) - 2):
        if list1[index1] < list2[index2]:
            list_sorted.append(list1[index1])
            index1 += 1
        else:
            list_sorted.append(list2[index2])
            index2 += 1

    del list1[-1]
    del list2[-1]
    return list_sorted
 

def init_mapper(key, value):
    """
    Distribute the values equally over buckets. By having each mapper using
    each key only once, all buckets are guaranteed to have at most m values,
    where m is the number of mappers used for the computation.
    """
    key = int(key) + 1
    for index, item in enumerate(value.split()):
        yield key + index, item


def init_reducer(key, values):
    """
    Use a sort function to sort this small bucket of values. As the number
    of values in an initial bucket is limited by the number of mappers m
    (roughly 1 <= m < 200), any sort algorithm can be used here.
    """
    values_sorted = sorted(str_to_int([v for v in values]))
    yield key, ' '.join(int_to_str(values_sorted))


def merge_mapper(key, value):
    """Group two buckets together by dividing their ids by 2 to get same id"""
    (id_bucket, numbers) = value.split(None, 1)
    id_bucket_new = (int(id_bucket) + 1) / 2 
    yield id_bucket_new, numbers


def merge_reducer(key, values):
    """Merge two buckets of same id together"""
    buckets = [v for v in values]
    if int(key) > 0:
        if len(buckets) == 2:
            bucket_sorted = merge_lists(str_to_int(buckets[0].split()),
                                        str_to_int(buckets[1].split()))
            yield key, ' '.join(int_to_str(bucket_sorted))
        else:
            # Only one bucket here, so just return it
            yield key, buckets[0]
            if int(key) == 1:
                # Termination: there is only one bucket with id 1
                # An invalid id 0 is returned to stop the sorting loop
                yield 0, 1


def split_mapper(key, value):
    """Forward the list of numbers to the reducer for final arrangement"""
    (id_bucket, numbers) = value.split(None, 1)
    if int(id_bucket) > 0:
        yield id_bucket, numbers


def split_reducer(key, values):
    """Return the numbers sorted with one number per tuple"""
    buckets = [v for v in values]
    numbers = str_to_int(buckets[0].split())
    for index, number in enumerate(numbers):
        yield index + 1, number


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
    output = sys.argv[2] + '%04d'
    sorted = sys.argv[2] + '_sorted'
    suffix  = '/part*'

    # Create the initial buckets from the data
    prince.run(init_mapper, init_reducer, input, output % 0, inputformat='text', outputformat='text')

    stop = False
    iteration = 1
    while not stop:
        # Merge current buckets
        previous = output % (iteration - 1)
        current  = output % iteration
        prince.run(merge_mapper, merge_reducer, previous + suffix, current, inputformat='text', outputformat='text')
 
        # Check if sort is done
        state = prince.dfs_read(current + suffix, last=1) 
        if int(state.split()[0]) == 0:
            stop = True
        iteration += 1

    # Organize the sorted numbers, one per tuple
    prince.run(split_mapper, split_reducer, current + suffix, sorted, inputformat='text', outputformat='text')
