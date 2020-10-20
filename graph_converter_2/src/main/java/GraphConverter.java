import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.io.FileWriter;

public class GraphConverter {
    public static void main(String[] args) {
        final ProgressLogger pl = new ProgressLogger();
        final ImmutableGraph graph;
        try {
            graph = ImmutableGraph.load(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        final IntArrayFIFOQueue queue = new IntArrayFIFOQueue();
        final int n = graph.numNodes();
        final int[] dist = new int[n];
        IntArrays.fill(dist, Integer.MAX_VALUE);
        int curr = 0, succ, ecc = 0, reachable = 0;
        FileWriter out;
        try {
            out = new FileWriter("./" + args[0] + ".net");
        }catch (Exception e){
            System.err.println("Failed to open output file");
            return;
        }

        for (int i = 0; i < n; i++) {
            if (dist[i] == Integer.MAX_VALUE) { // Not already visited
                queue.enqueue(i);
                dist[i] = 0;

                LazyIntIterator successors;

                while (!queue.isEmpty()) {
                    curr = queue.dequeueInt();
                    successors = graph.successors(curr);
                    int d = graph.outdegree(curr);
                    while (d-- != 0) {
                        succ = successors.nextInt();
                        if (dist[succ] == Integer.MAX_VALUE) {
                            reachable++;
                            dist[succ] = dist[curr] + 1;
                            ecc = Math.max(ecc, dist[succ]);
                            queue.enqueue(succ);
                            try {
                                out.write(String.valueOf(i) + "\t" + String.valueOf(succ) + "\n" );
                            } catch (Exception e){
                                System.err.println("Failed to write");
                                e.printStackTrace();
                                return;
                            }
                        }
                    }
                }
            }
            pl.update();
        }
        pl.done();
    }
}
