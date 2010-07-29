"""
Prince configuration module
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

inputformats = {'text': 'org.apache.hadoop.mapred.TextInputFormat',
                'auto': 'org.apache.hadoop.streaming.AutoInputFormat'}

outputformats = {'text': 'org.apache.hadoop.mapred.TextOutputFormat',
                 'auto': 'org.apache.hadoop.mapred.SequenceFileOutputFormat'}

mapreduce_path      = os.environ.get('HADOOP_HOME') + '/'
mapreduce_program   = mapreduce_path + 'bin/hadoop'

mapreduce_dirstreaming = 'contrib/streaming/'
mapreduce_streaming = mapreduce_dirstreaming + os.listdir(mapreduce_path + mapreduce_dirstreaming)[0]

option_mapper  = 'pmapper'
option_reducer = 'preducer'
separator = '\t'


