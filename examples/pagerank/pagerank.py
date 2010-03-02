#!/usr/bin/env python
"""
Distributed PageRank algorithm.

Compute the PageRank values for the nodes of a graph. The algorithm uses a
uniform probability distribution to set the initial values: each value is
equal to 1 / N, where N is the number of nodes in the whole graph.
Dandlings nodes (ie: nodes that do not have any out-links, and therefore
have an empty adjacency list) are connected to the whole graph in order to
take advantage of their PageRank values.
The computation of the PageRank values is stopped when the error in quadratic
norm converges under a chosen threshold.
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
        value = value.split()
        node = int(value[0])
        pr_previous = float(value[1])
        pr_current = float(value[2])
        nodes_adjacent = [int(n) for n in value[3:]]
        return (node, pr_previous, pr_current, nodes_adjacent)
    except ValueError:
        return (None, None, None, None)


def pagerank_mapper(key, value):
    """Perform one PageRank iteration"""
    (node, pr_previous, pr_current, nodes_adjacent) = node_info(value)
    if node != None:
        nb_nodes = len(nodes_adjacent)
        for node_adjacent in nodes_adjacent:
            # Map the normalized PageRank value to the node that needs it
            yield node_adjacent, pr_current / nb_nodes
        yield (node, 'infos ' + make_value(pr_current, pr_current, nodes_adjacent))


def pagerank_reducer(node, values):
    """Compute the new PageRank for the node"""
    try:
        # sort because we want the 'infos' value at the end of the list
        values = sorted([v for v in values]) # as values is a generator
        damping = float(prince.get_parameters('damping'))
        infos = values[-1].split()

        pr_previous = float(infos[2])
        nodes_adjacent = [int(n) for n in infos[3:]]
        pageranks = [float(v) for v in values[:-1]]

        nb_nodes = float(prince.get_parameters('nb_nodes'))
        pr_new = (1.0 - damping) / nb_nodes + damping * sum(pageranks)
        yield (node, make_value(pr_previous, pr_new, nodes_adjacent))
    except ValueError:
        pass


def term_mapper(key, value):
    """Check if an update has been made during the the current iteration"""
    (node, pr_previous, pr_current, nodes_adjacent) = node_info(value)
    if node != None:
        yield 0, (pr_previous - pr_current)


def term_reducer(key, pagerank_changes):
    """Check whether the values are converging using the quadratic norm"""
    try:
        precision = float(prince.get_parameters('precision'))
        if sum([float(p) ** 2 for p in pagerank_changes]) > precision ** 2:
            return 0, 0 # let's do another iteration
    except ValueError:
        pass # the algorithm is stopped in case of error
    return 1, 1


def read_graph(filename):
    """Create a file with only the starting node of the graph"""
    with open(filename, 'r') as file:
        # Existence of the file voluntarily not tested to help debugging
        adlists = [line.split(None, 1) for line in file]
    graph = {}
    for adlist in adlists:
        node_source = int(adlist[0])
        nodes_adjacent = [int(n) for n in adlist[1].split()] if len(adlist) > 1 else []
        graph[node_source] = nodes_adjacent
    return graph


def handle_dandling_nodes(graph):
    """Connect dandlings nodes to the whole graph"""
    nodes = set([n for n in graph.keys()])
    for node, nodes_adjacent in graph.items():
        if not nodes_adjacent:
            nodes_all_others = list(nodes - set([node]))
            nodes_adjacent.extend(nodes_all_others)


def make_value(pr_previous, pr_current, nodes):
    """Build a value to be used in an item (key, value)"""
    adjacency_list = ' '.join([str(n) for n in nodes])
    return '%.40f %.40f %s' % (pr_previous, pr_current, adjacency_list)


def initial_pagerank(graph, pr_init):
    """Set the initial PageRank values"""
    return [(node, make_value(pr_init, pr_init, nodes_adjacent)) for node, nodes_adjacent in graph.items()]


def display_usage():
    print 'usage: ./%s graph output damping precision [iteration_max] [iteration_start]' % sys.argv[0]
    print '  graph: graph file on local hard drive: each line begin with the id of a node, and it'
    print '         is continued by its adjacenty list, ie: the ids of the nodes it points to'
    print '  output: basename of the output files on the DFS'
    print '  damping: value of the damping factor within (0,1), the PageRank paper suggesting .85'
    print '  precision: precision value below which a PageRank value is considered stable'
    print '             for instance .01 means no more than 1% difference between two iterations'
    print '             the quadratic norm is used for computing change'
    print '  iteration_max: maximum number of iterations (default=infinite)'
    print '  iteration_start: iteration to start from, useful to restart a stopped task (default=1)'
 

if __name__ == "__main__":
    prince.init()

    if len(sys.argv) < 5:
        display_usage()
        sys.exit(0)

    filename_graph  = sys.argv[1]
    output          = sys.argv[2]
    damping         = float(sys.argv[3])
    precision       = float(sys.argv[4])
    iteration_max   = int(sys.argv[5]) if len(sys.argv) >= 6 else sys.maxint
    iteration_start = int(sys.argv[6]) if len(sys.argv) >= 7 else 1

    graph    = read_graph(filename_graph)
    pr_init  = 1.0 / len(graph) # initial values: uniform probability distribution
    handle_dandling_nodes(graph)

    pagerank = output + '_pagerank%04d'
    term     = output + '_term%04d'
    suffix   = '/part*'
    part     = '/part-00000'
    options  = {'damping': damping, 'precision': precision, 'nb_nodes': len(graph)}

    # Create the initial values
    pagerank_current = pagerank % iteration_start
    if iteration_start == 1:
        pagerank_values = [(n, make_value(pr_init, pr_init, n_adjacent)) for n, n_adjacent in graph.items()]
        prince.dfs_write(pagerank_current + part, pagerank_values)
        iteration_start += 1

    stop = False
    iteration = iteration_start
    while not stop and iteration < iteration_max:
        # Update file names
        pagerank_previous = pagerank_current
        pagerank_current  = pagerank % iteration
        term_current      = term % iteration

        # Compute the new PageRank values
        prince.run(pagerank_mapper, pagerank_reducer, pagerank_previous + suffix, pagerank_current,
                   [], options, 'text', 'text')

        # Termination: check if all PageRank values are stable
        prince.run(term_mapper, term_reducer, pagerank_current + suffix, term_current,
                   [], options, 'text', 'text')
        term_value = prince.dfs_read(term_current + suffix)
        stop = int(term_value.split()[1])

        # Get ready for the next iteration
        iteration += 1
