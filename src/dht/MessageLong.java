package dht;

public class MessageLong {
    private Dht applicant;
    private int val;

    MessageLong(Dht applicant) {
        this.val = applicant.longLine();
        this.applicant = applicant;
    }

    public Dht getApplicant() {
	    return this.applicant;
    }

    public int getVal() {
	    return this.val;
    }
}
