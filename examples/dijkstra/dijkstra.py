#!/usr/bin/env python
"""
Distributed single-source shortest path with Dijsktra's algorithm.

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
import prince


def node_info(value):
    """Get the information about a node from a mapper value"""
    try:
        (node, d_previous, d_current) = value.split()
        node = int(node)
        d_previous = int(d_previous)
        d_current = int(d_current)
        return (node, d_previous, d_current)
    except ValueError:
        return (None, None, None)


def frontier_mapper(key, value):
    """Expand the frontier of one hop."""
    (node, d_previous, d_current) = node_info(value)
    if node != None:
        yield node, '%d %d' % (d_current, d_current) # reinject itself
        if d_current != d_previous: # expand only if distance has changed
            graph = read_graph(prince.get_parameters('graph'))
            for node_adjacent in graph[node]:
                yield node_adjacent, '%d %d' % (sys.maxint, d_current + 1)


def frontier_reducer(node, values):
    """Keep the minimum to follow Dijkstra's algorithm"""
    try:
        distances = [v for v in values]
        d_previous = min([int(d.split()[0]) for d in distances])
        d_current  = min([int(d.split()[1]) for d in distances])
        yield node, '%d %d' % (d_previous, d_current)
    except ValueError:
        pass


def term_mapper(key, value):
    """Check if an update has been made during the current iteration"""
    (node, d_previous, d_current) = node_info(value)
    if node != None:
        changed = 0 if d_previous == d_current else 1
        yield 0, changed # important: must reduce to the same key


def term_reducer(key, changed):
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
    print 'usage: %s graph source_node output [iteration_max] [iteration_start]' % sys.argv[0]
    print '  graph: graph file on local hard drive: each line begin with the id of a node, and it'
    print '         is continued by its adjacenty list, ie: the ids of the nodes it points to'
    print '  source_node: id of the source node'
    print '  output: basename of the output files on the DFS'
    print '  iteration_max: maximum number of iterations (default=infinite)'
    print '  iteration_start: iteration to start from, useful to restart a stopped task (default=1)'
 

if __name__ == "__main__":
    prince.init()

    if len(sys.argv) < 4:
        display_usage()
        sys.exit(0)

    filename_graph  = sys.argv[1]
    source_node     = sys.argv[2]
    output          = sys.argv[3]
    iteration_max   = int(sys.argv[4]) if len(sys.argv) >= 5 else sys.maxint
    iteration_start = int(sys.argv[5]) if len(sys.argv) >= 6 else 1

    frontier = output + '_frontier%04d'
    term     = output + '_term%04d'
    suffix   = '/part*'
    part     = '/part-00000'
    options  = {'graph': filename_graph, 'source': source_node}

    # Create the initial frontier with the tuple (source, 0)
    frontier_current = frontier % iteration_start
    if iteration_start == 1:
        prince.dfs.write(frontier_current + part, (source_node, '%d %d' % (sys.maxint, 0)))
        iteration_start += 1

    stop = False
    iteration = iteration_start
    while not stop and iteration < iteration_max:
        # Update file names
        frontier_previous = frontier_current
        frontier_current  = frontier % iteration
        term_current      = term % iteration

        # Compute the new frontier
        prince.run(frontier_mapper, frontier_reducer, frontier_previous + suffix, frontier_current,
                   filename_graph, options, 'text', 'text')
        print prince.dfs.read(frontier_current + suffix)

        # Termination: check if all distances are stable
        prince.run(term_mapper, term_reducer, frontier_current + suffix, term_current,
                   filename_graph, options, 'text', 'text')
        print prince.dfs.read(term_current + suffix)
        term_value = prince.dfs.read(term_current + suffix)
        stop = int(term_value.split()[1])

        # Get ready for the next iteration
        iteration += 1
