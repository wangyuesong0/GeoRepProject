package helios.message;

import java.io.Serializable;
import java.util.UUID;

/**
 * @Project: helios
 * @Title: Message.java
 * @Package helios.message
 * @Description: TODO
 * @author YuesongWang
 * @date Feb 28, 2016 1:34:56 PM
 * @version V1.0
 */
public class Message implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1615003866865258912L;
    private UUID uid;

    public Message() {
        super();
        this.uid = UUID.randomUUID();
    }

    public UUID getUid() {
        return uid;
    }

    public void setUid(UUID uid) {
        this.uid = uid;
    }

}
