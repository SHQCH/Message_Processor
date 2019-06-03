package MessageProcessor;

import MessageProcessor.Event;

import java.util.*;

public class MessageA {
    String group;
    List<Event> events;

    public MessageA(int eventSize, int attIdxBound) {
        this.group = "update";
        this.events = new ArrayList<Event>();
        Random rnd = new Random();
        for(int i=0; i<eventSize; i++)
        events.add(new Event(rnd.nextInt(10000),rnd.nextInt(attIdxBound)));
    }


}
