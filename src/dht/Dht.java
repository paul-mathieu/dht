package dht;

import peersim.edsim.*;
import peersim.core.*;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import peersim.config.*;

public class Dht implements EDProtocol {
    // Save all neighbors
    public static ArrayList<Dht> NodesList =  new ArrayList<Dht>();
    // Creation of a logger
    public Logger logger = Logger.getLogger("logger");
    // Transport layer object
    private HWTransport transport;
    // Identifier of the current layer (the application layer)
    private int myPid;
    // Node number
    private int nodeId;
    // Transport layer identifier
    private int transportPid;
    // Layer prefix (name of the protocol variable from the config file)
    private String prefix;
    // Neighbors on the right and left
    private Dht leftNode;
    private Dht rightNode;
    private Dht longNode;
    // Position of the node in the DHT
    private int position;
    private int randomPositionMax = 999;
    // Le tableau des données
    private ArrayList<String> data;
    private ArrayList<String> dataLeft;
    private ArrayList<String> dataRight;

    // ==== Constructor ====

    public Dht(String prefix) {
        // Prefix initialization
        this.prefix = prefix;
        // Initialization of identifiers from the configuration file
        this.transportPid = Configuration.getPid(prefix + ".transport");
        this.myPid = Configuration.getPid(prefix + ".myself");
        this.position = (int) (Math.random() * ( this.randomPositionMax - 1 ));
        this.transport = null;
        this.longNode = null;
        this.data = new ArrayList<>();
        // Left neighbor initialization
        this.leftNode = this;
        this.dataLeft = new ArrayList<>();
        // Right neighbor initialization
        this.rightNode = this;
        this.dataRight = new ArrayList<>();
        // Nodes list affectation
        NodesList.add(this);
    }

    // ==== Main methods ====

    // Method call when a message is received by the HelloWorld protocol of the node
    public void processEvent( Node node, int pid, Object event ) {
        if (event.getClass().getName().equals("dht.Message")) this.receive((Message)event);
        else this.receive((MessageLong)event);
    }

    // Method necessary for the creation of the network (which is done by cloning a prototype)
    public Object clone() {
        return new Dht(this.prefix);
    }

    public void join(Dht primaire) {
        if(this.getPosition() > primaire.getPosition()) {
            while (this.position > primaire.rightNode.getPosition()) {
                if(primaire.rightNode.getPosition() <= primaire.getPosition()) {
                    break;
                }
                primaire = primaire.rightNode;
            }
            this.rightNode = primaire.rightNode;
            this.leftNode = primaire;
            primaire.rightNode.leftNode = this;
            primaire.rightNode = this;
        } else {
            while (this.position < primaire.leftNode.getPosition()) {
                if (primaire.leftNode.getPosition() <= primaire.getPosition()) {
                    break;
                }
                primaire = primaire.leftNode;
            }
            this.rightNode = primaire;
            this.leftNode = primaire.leftNode;
            primaire.leftNode.rightNode = this;
            primaire.leftNode = this;
        }
    }

    /**
     * Go through the whole character string and sum the chars (with the ASCII code).
     * We take the modulo of this value to have a hash function between [0, lengthDHt]
     * @param entry character chain to hash
     * @return hash of the character chain
     */
    public int doHash(String entry) {
        AtomicInteger somme = new AtomicInteger();
        for (int i = 0; i < entry.length(); i++)
            somme.set(somme.get() + entry.charAt(i));
        return somme.get() % this.randomPositionMax;
    }

    /*
     * sending a message (done via the transport layer)
     */
    public void send(Message msg, Node destination) {
        this.transport.send(getMyNode(), destination, msg, this.myPid);
    }

    public void send(MessageLong msg, Node destination) {
        this.transport.send(getMyNode(), destination, msg, this.myPid);
    }

    /*
     * display reception
     */
    private void receive(Message msg) {
        float meanRight, meanLeft;
        int hash = this.doHash(msg.getContent());
        logger.info(this + "| Received message : " + msg.getContent() + " | h is: " + hash + " | " + this.position);
        if (this.position > this.leftNode.getPosition() && hash < this.position) {
            meanLeft = -1;
        } else {
            meanLeft = (this.getPosition() + this.leftNode.getPosition()) / 2;
        }

        meanRight = (this.getPosition() + this.rightNode.getPosition()) / 2;
        if ((hash > meanLeft && hash <= meanRight) || (this.position > this.rightNode.getPosition())) {
            this.data.add(msg.getContent());
            // Duplication of data on the right node
            this.rightNode.saveLeft(msg.getContent());
            // Duplication of data on the left node
            this.leftNode.saveRight(msg.getContent());
            logger.info("The message has arrived at the right node \n");
        } else {
            if (this.longNode != null && (this.longNode.getPosition()* .3 > hash)) {
                this.send(msg, Network.get(this.longNode.getNodeId()));
            }
            else if (hash > this.position) {
                this.send(msg, Network.get(this.rightNode.getNodeId()));
                logger.info("Send message to neighbor on the right " + this.rightNode + "\n");
            } else if (hash < this.position) {
                this.send(msg, Network.get(this.leftNode.getNodeId())); //Imposible dans la config actu
                logger.info("Envoie du message aux voisin de gauche" + this.leftNode + "\n");
            }
        }
    }

    public String toString() {
        return "Node " + this.nodeId;
    }

    public void saveLeft(String val){
        this.dataLeft.add(val);
    }

    public void saveRight(String val) {
        this.dataRight.add(val);
    }


    // ==== Long neighbor search ====

    /**
     * Points calculation
     */
    public int longLine() {
        return (this.getPosition() + (this.randomPositionMax/2)) % this.randomPositionMax;

    }

    public void receive(MessageLong msg) {
        if (msg.getVal() > this.leftNode.getPosition() && msg.getVal() < this.rightNode.getPosition()) {
            msg.getApplicant().longNode = this;
            logger.info(this + " is long link of " + msg.getApplicant());
        } else if (msg.getVal() > this.rightNode.getPosition())
            this.send(msg, Network.get(this.rightNode.getNodeId()));
        else
            this.send(msg, Network.get(this.leftNode.getNodeId()));
    }

    // ==== Getters ====

    private Node getMyNode() {
        return Network.get(this.nodeId);
    }

    public int getNodeId(){
        return this.nodeId;
    }

    public  ArrayList<String> getData() {
        return this.data;
    }

    public int getPosition() {
        return this.position;
    }

    // ==== Setters ====

    /**
     * Link between an application layer object and a transport layer object located on the same node
     */
    public void setTransportLayer(int nodeId) {
        this.nodeId = nodeId;
        this.transport = (HWTransport) Network.get(this.nodeId).getProtocol(this.transportPid);
    }

}