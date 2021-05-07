package dht;

import peersim.core.*;
import peersim.config.*;

import java.util.logging.Logger;


/*
 * HelloWorld initialization module:
 * For each node, the module is the link between the transport layer and the application layer
 * Then, it sends node 0 a "Hello" message to all the other nodes
 */
public class Initializer implements peersim.core.Control  {
    private Logger logger = Logger.getLogger("logger");
    private int helloWorldPid;

    /*
     * Recovery of the application layer pid
     */
    public Initializer(String prefix) {
		this.helloWorldPid = Configuration.getPid(prefix + ".helloWorldProtocolPid");
    }

    public boolean execute() {
		int nodeNumber;
		Dht emitter, currentDht, messageSender;
		Node destinationNode;
		Message helloMsg;
		int firstNode = this.firstNode();
		// Network size recovery
		nodeNumber = Network.size();
		// Message creation
		helloMsg = new Message(Message.DHT,"Test for dht v");

		if (nodeNumber < 1) {
			logger.warning("Inconsistent size");
			System.exit(1);
		}
		// Recovery of the application layer of the transmitter (node 0)
		emitter = (Dht)Network.get(firstNode).getProtocol(this.helloWorldPid);
		emitter.setTransportLayer(firstNode);
		// For each node, we make the link between the application layer and the transport layer
		// Then we send node 0 a "Hello" message
		for (int i = 0; i < nodeNumber; i++) {
			destinationNode = Network.get(i);
			currentDht = (Dht)destinationNode.getProtocol(this.helloWorldPid);
			currentDht.setTransportLayer(i);
			currentDht.join(emitter);
		}

		for (int i = 0; i < nodeNumber; i++) {
			destinationNode = Network.get(i);
			currentDht = (Dht)destinationNode.getProtocol(this.helloWorldPid);
			currentDht.send(new MessageLong(currentDht), destinationNode);
		}
		Thread.onSpinWait();
		messageSender = (Dht)Network.get(firstNode).getProtocol(this.helloWorldPid);
		messageSender.setTransportLayer(firstNode);
		destinationNode = Network.get(this.firstNode());
		messageSender.send(helloMsg, destinationNode);

		return false;
	}

	/**
	 * @return index of the node with the smallest position
	 */
	private int firstNode() {
		Dht currentDht;
		Node destinationNode;
		int min = Integer.MAX_VALUE;
		int aR = -1;
		for (int i = 0; i < Network.size(); i++) {
			destinationNode = Network.get(i);
			currentDht = (Dht)destinationNode.getProtocol(this.helloWorldPid);
			if(currentDht.getPosition() < min) {
				min = currentDht.getPosition();
				aR = i;
			}
		}
		return aR;
	}

}
