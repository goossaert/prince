#!/usr/bin/env python
"""
Distributed ingle-source shortest path with Dijsktra's algorithm.

Explore a graph considering all weights are one, and find all shortest
distances from a given source node. Each iteration requires three
MapReduce tasks, one to compute the new frontier, another one to compute
the changes in distances, and a last one to check if the search is over.
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
import math
import prince


def node_info(value):
    """Get the information about a node from a mapper value"""
    try:
        (node, distance) = value.split()
        node = int(node)
        distance = int(distance)
        return (node, distance)
    except ValueError:
        return (None, None)


def frontier_mapper(key, value):
    """Expand the frontier of one hop."""
    (node, distance) = node_info(value)
    if node != None:
        yield node, int(math.fabs(distance)) # reinject itself
        if distance >= 0: # if the value is negative, it is skipped
            params = prince.get_parameters()
            graph = read_graph(params['graph'][0])
            for node_adjacent in graph[node]:
                yield node_adjacent, distance + 1


def frontier_reducer(node, values):
    """Keep the minimum to follow Dijsktra's algorithm"""
    try:                yield node, min([int(v) for v in values])
    except ValueError:  pass


def filter_mapper(key, value):
    """Identity mapper"""
    (node, distance) = node_info(value)
    if node != None:
        yield node, distance


def filter_reducer(node, values):
    """
    For each node, get minimum distance between current and previous frontiers.
    Return negative distances for distances that have not been updated.
    """
    try:
        distances = [int(d) for d in values] 
    except ValueError:
        return
    if len(distances) == 1: 
        yield node, distances[0]
    else: # len == 2
        if distances[0] == distances[1]:
            yield node, -distances[0]
        else:
            yield node, min(distances)


def termination_mapper(key, value):
    """Check if an update has been made in the current iteration"""
    (node, distance) = node_info(value)
    if node != None:
        changed = 0 if distance <= 0 else 1
        yield 0, changed # important: must reduce to the same key


def termination_reducer(key, changed):
    """Check whether any of the distances have changed"""
    try:
        if any(int(c) for c in changed):
            return 0, 0 # must perform another iteration
    except ValueError:
        pass # the algorithm is stopped in case of error
    return 1, 1


def read_graph(filename):
    """Create a file with only the starting node of the graph."""
    with open(filename, 'r') as file:
        # Existence of the file voluntarily not tested to help debugging
        adlists = [line.split(None, 1) for line in file]
    graph = {}
    for adlist in adlists:
        node_source = int(adlist[0])
        nodes_adjacent = [int(n) for n in adlist[1].split()] if len(adlist) > 1 else []
        graph[node_source] = nodes_adjacent
    return graph


def display_usage():
    print 'usage: ./%s graph source_node [iteration_max] [iteration_start]' % sys.argv[0]
    print '  graph: graph file on local hard drive: each line begin with the id of a node, and it'
    print '         is continued by its adjacenty list, ie: the ids of the nodes it points to'
    print '  source_node: id of the source node'
    print '  iteration_max: maximum number of iterations (default=infinite)'
    print '  iteration_start: iteration to start from, useful to restart a stopped task (default=0)'
 

if __name__ == "__main__":
    prince.init()

    if len(sys.argv) != 3:
        display_usage()
        sys.exit(0)

    filename_graph  = sys.argv[1]
    source_node     = sys.argv[2]
    iteration_max   = int(sys.argv[3]) if len(sys.argv) == 4 else sys.maxint
    iteration_start = int(sys.argv[4]) if len(sys.argv) == 5 else 1

    frontier    = 'frontier%04d'
    filter      = 'filter%04d'
    termination = 'termination%04d'
    suffix      = '/part*'
    part        = '/part-00000'
    options     = {'graph': filename_graph, 'source': source_node}

    # Create the initial files with the tuple (source, 0)
    frontier_current = frontier % (iteration_start - 1)
    prince.dfs_write(frontier_current + part, (source_node, 0))
    filter_current = filter % (iteration_start - 1)
    prince.dfs_write(filter_current + part, (source_node, 0))

    stop = False
    iteration = iteration_start
    while not stop and iteration < iteration_max:
        # Update file names
        frontier_previous   = frontier_current
        frontier_current    = frontier % iteration
        filter_previous     = filter_current
        filter_current      = filter % iteration
        termination_current = termination % iteration

        # Compute the frontier from the previous filter
        prince.run(frontier_mapper, frontier_reducer, filter_previous + suffix, frontier_current,
                   filename_graph, options, 'text', 'text')
        print prince.dfs_read(frontier_current + suffix)

        # Compute the filter from the previous and current frontier
        prince.run(filter_mapper, filter_reducer, [frontier_previous + suffix, frontier_current + suffix], filter_current,
                   filename_graph, options, 'text', 'text')
        print prince.dfs_read(filter_current + suffix)

        # Compute the termination condition from the current filter
        prince.run(termination_mapper, termination_reducer, filter_current + suffix, termination_current,
                   filename_graph, options, 'text', 'text')
        print prince.dfs_read(termination_current + suffix)

        # Termination: check if all distances are stable
        termination_value = prince.dfs_read(termination_current + suffix)
        stop = int(termination_value.split()[1])
        iteration += 1
