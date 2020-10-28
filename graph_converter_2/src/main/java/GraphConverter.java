import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;

import java.lang.reflect.InvocationTargetException;
import java.io.*;


public class GraphConverter {
    public static void main(String[] arg) throws ClassCastException, IllegalArgumentException, SecurityException, IllegalArgumentException, IOException {
        if ( arg.length != 1 ) {
            System.err.println( "Usage: BV2Ascii BASENAME" );
            return;
        }

        final ImmutableGraph graph = it.unimi.dsi.webgraph.ImmutableGraph.loadOffline( arg[0] );

        NodeIterator nodeIterator = graph.nodeIterator();
        int curr, d;
        int[] suc;
        FileOutputStream file = new FileOutputStream(arg[0] + ".net");
        BufferedOutputStream outStreamB = new BufferedOutputStream(file, 4096);
        PrintStream outStream = new PrintStream( outStreamB );

        while( nodeIterator.hasNext() ) {
            curr = nodeIterator.nextInt();
            d = nodeIterator.outdegree();
            suc = nodeIterator.successorArray();

            for( int j=0; j<d; j++ ) {
                outStream.println(curr + "\t " + (suc[j]) );
            }
        }
    }
}
