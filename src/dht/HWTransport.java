package dht;

import peersim.config.*;
import peersim.core.*;
import peersim.edsim.*;

public class HWTransport implements Protocol {

    // Variables to calculate latency between nodes
    private final long min;
    private final long range;

    public HWTransport(String prefix) {
        System.out.println("Transport Layer Enabled");
        // Recovery of extreme latency values from the configuration file
        min = Configuration.getInt(prefix + ".mindelay");
        long max = Configuration.getInt(prefix + ".maxdelay");
        if (max < min) {
            System.out.println("The maximum latency cannot be smaller than the minimum latency");
            System.exit(1);
        }
        range = max - min + 1;
    }

    public Object clone() {
	    return this;
    }

    // Sending a message, add it to the event queue
    public void send(Node src, Node dest, Object msg, int pid) {
        EDSimulator.add(getLatency(), msg, dest, pid);
    }

    // Random latency between the min terminal and the max terminal
    public long getLatency() {
	    return (range==1?min:min + CommonState.r.nextLong(range));
    }


}

