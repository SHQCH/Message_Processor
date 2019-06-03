package MessageProcessor;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class MessageB {

    Integer attributeIndex;
    String origin;

    public MessageB(int attIdxBound) {
        Random rmd = new Random();
        this.attributeIndex = rmd.nextInt(attIdxBound);
        this.origin = "Country" + rmd.nextInt(100);
    }
}
