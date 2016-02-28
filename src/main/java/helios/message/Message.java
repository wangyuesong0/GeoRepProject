
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
    public UUID uid;
    public MessageType type;
    public String routingKey;
    
    public Message(MessageType type, String routingKey) {
        super();
        this.uid = UUID.randomUUID();
        this.type = type;
        this.routingKey = routingKey;
    }
    
}
