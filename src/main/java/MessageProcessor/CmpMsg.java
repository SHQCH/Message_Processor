package MessageProcessor;

import java.io.Serializable;

public class CmpMsg implements Serializable {
    String attributeIndex;
    String group;
    String eventId;
    String origin;

    public CmpMsg(String attributeIndex, String group, String eventId, String origin)

    {
        this.attributeIndex = attributeIndex;
        this.group = group;
        this.eventId = eventId;
        this.origin = origin;
    }

    public String getAttributeIndex() {
        return attributeIndex;
    }

    public void setAttributeIndex(String attributeIndex) {
        this.attributeIndex = attributeIndex;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }
}
