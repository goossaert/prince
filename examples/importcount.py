#!/usr/bin/env python
"""
Example of import of mapper and reducer methods.
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

import babar

# Methods from wordcount are now in the local name space
from wordcount import * 


if __name__ == "__main__":
    # Always call babar.init() at the beginning of the program
    babar.init()

    input  = 'logs/*' # change to input data on the DFS
    output = 'count'

    # Run the task with the mapper and reducer methods from the wordcount.py file
    # Note that the file wordcount.py is added to the 'files' argument
    babar.run(wc_mapper, wc_reducer, input, output, inputformat='text', outputformat='text', files='wordcount.py')

    # Read the output file and print it 
    file = babar.dfs_read(output + '/part*')
    print file
