
/*
 * Copyright (C) 2003-2014 Paolo Boldi and Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */


import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;

import java.io.IOException;
import java.util.Arrays;


/** The main method of this class loads an arbitrary {@link it.unimi.dsi.webgraph.ImmutableGraph}
 * and performs a breadth-first visit of the graph (optionally starting just from a given node, if provided,
 * in which case it prints the eccentricity of the node, i.e., the maximum distance from the node).
 */

public class GraphConverter{


    static public void main(String[] arg) throws IllegalArgumentException, SecurityException,  IOException {

        final String basename = arg[0];
        ImmutableGraph graph = ImmutableGraph.load( basename);

        final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
        final int n = graph.numNodes();
        final int[] dist = new int[ n ];

        Arrays.fill( dist, Integer.MAX_VALUE ); // Initially, all distances are infinity.

        int curr, succ, ecc = 0;


        for(int i = 0; i < n; i++ ) {
            if ( dist[ i ] == Integer.MAX_VALUE ) { // Not already visited
                queue.enqueue( i );
                dist[ i ] = 0;

                LazyIntIterator successors;

                while( ! queue.isEmpty() ) {
                    curr = queue.dequeueInt();
                    successors = graph.successors( curr );
                    int d = graph.outdegree( curr );
                    while( d-- != 0 ) {
                        succ = successors.nextInt();
                        if ( dist[ succ ] == Integer.MAX_VALUE ) {
                            dist[ succ ] = dist[ curr ] + 1;
                            ecc = Math.max( ecc, dist[ succ ] );
                            queue.enqueue( succ );
                            System.out.println( i + " \t " + succ + " ");
                        }
                    }
                }
            }
        }
    }
}
