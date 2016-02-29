
package helios.message;

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
public class Message {
    private UUID uid;
    private MessageType type;
    private String routingKey;
    
    public Message(MessageType type, String routingKey) {
        super();
        this.uid = UUID.randomUUID();
        this.type = type;
        this.routingKey = routingKey;
    }

    public UUID getUid() {
        return uid;
    }

    public void setUid(UUID uid) {
        this.uid = uid;
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }
    
    
    
    
}
