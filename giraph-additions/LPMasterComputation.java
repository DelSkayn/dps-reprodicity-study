package org.apache.giraph.examples;

import org.apache.giraph.master.DefaultMasterCompute;

/**
 * Master Computation for {@link LPComputation}.
 *
 * Halts the computation after a given number if iterations.
 *
 * @author Kevin Gomez (k.gomez@freenet.de)
 * @author Martin Junghanns (junghanns@informatik.uni-leipzig.de)
 */
public class LPMasterComputation extends DefaultMasterCompute {
    /**
     * {@inheritDoc}
     */
    @Override
    public void compute() {
        int iterations = getConf().getInt(LPComputation.NUMBER_OF_ITERATIONS,
                LPComputation.DEFAULT_NUMBER_OF_ITERATIONS);
        if (getSuperstep() == iterations) {
            haltComputation();
        }
    }
}

