package MessageProcessor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import com.google.gson.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import java.io.File;

public class SparkMsgProcessor {


    public static void main(String[] args) throws Exception {
        String userDir = System.getProperty("user.dir");
        String checkPtDir = userDir+"/Checkpoint";
        Long windowDurationInMs = new Long(1);

        SparkConf sparkConf = new SparkConf().setAppName("messageProcessor").setMaster("local[4]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(windowDurationInMs));
        jsc.checkpoint(checkPtDir);

        Random rand = new Random();
        /**
         * attIndexBound: attributeIndex of msgA and msgB are random numbers in the range [0, attIndexBound]
         */
        Integer attIndexBound = 1000;
        Integer listSize = 1000;
        Integer queSize = 1;
        Queue<JavaRDD<String>> rddQueue = new LinkedList<>();

        List<String> list = new ArrayList<>();
        for(int j=0; j<listSize; j++) {
            int bar = rand.nextInt(2);
            if(bar>=1) {
                MessageA temp = new MessageA(2, attIndexBound);
                list.add(new Gson().toJson(temp));
            }
            else {
                MessageB tempB = new MessageB(attIndexBound);
                list.add(new Gson().toJson(tempB));
            }
        }

        /**
         * save the input messages in "./input.json"
         */
        File fl = new File(userDir+"/input.json");
        FileWriter fw = new FileWriter(fl);
        BufferedWriter bw = new BufferedWriter(fw);
        for(String e : list) {
            bw.write(e);
            bw.newLine();
        }
        bw.close();
        fw.close();


        for(int i=0; i<queSize; i++) rddQueue.add(jsc.sparkContext().parallelize(list));

        JavaDStream<String> inputStream = jsc.queueStream(rddQueue);
        inputStream.dstream().saveAsTextFiles(userDir+"/output/JsonIn", "json");

        FlatMapFunction<String, CmpMsg> flatMapFun = new FlatMapFunction<String, CmpMsg>() {
            @Override
            public Iterator<CmpMsg> call(String s) throws Exception {
                JsonParser parser=new JsonParser();
                JsonObject object=(JsonObject) parser.parse(s);
                List<CmpMsg> eventList = new ArrayList<CmpMsg>();

                if(object.get("group")!=null) {  // if this is messageA,
                    JsonArray eventArray=object.get("events").getAsJsonArray();
                    String groupName = object.get("group").getAsString();
                    for(JsonElement event : eventArray) {
                        JsonObject subObject = event.getAsJsonObject();
                        String eventID = subObject.get("eventId").getAsString();
                        String attIdx = subObject.get("attributeIndex").getAsString();
                        String origin = "";
                        eventList.add(new CmpMsg(attIdx, groupName, eventID, origin));
                    }
                }
                else {  // if this is messageB
                    String attIdx = object.get("attributeIndex").getAsString();
                    String groupName = "";
                    String eventID ="";
                    String origin = object.get("origin").getAsString();
                    eventList.add(new CmpMsg(attIdx, groupName, eventID, origin));
                }
                return eventList.iterator();
            }
        };

        Function3<String, Optional<CmpMsg>, State<CmpMsg>, String> stateFuncTest = new Function3<String, Optional<CmpMsg>, State<CmpMsg>, String>() {
            @Override
            public String call(String key, Optional<CmpMsg> value, State<CmpMsg> state) throws Exception {
                CmpMsg newMsg;
                CmpMsg msg = value.get();
                if(state.exists()) {
                    CmpMsg firstMsg = state.get();
                    String group = firstMsg.getGroup()+msg.getGroup();
                    String eventID = firstMsg.getEventId()+msg.getEventId();
                    String origin = firstMsg.getOrigin()+msg.getOrigin();
                    /**if the matched messages are of the same type, ignore this match**/
                    if(group.length()<2 || origin.length()<2) return "invalid";
                    newMsg = new CmpMsg(key, group, eventID, origin);
                    state.remove();
                    return new Gson().toJson(newMsg)+"Time:"+ System.currentTimeMillis();
                }
                else
                {
                    state.update(msg);
                    return "invalid";
                }
            }
        };

        Function2<CmpMsg, CmpMsg, CmpMsg> reduceFunction = new Function2<CmpMsg, CmpMsg, CmpMsg>() {
            @Override
            public CmpMsg call(CmpMsg value1, CmpMsg value2) throws Exception {
                String group = value1.getGroup()+value2.getGroup();
                String eventID = value1.getEventId()+value2.getEventId();
                String origin = value1.getOrigin()+value2.getOrigin();
                return new CmpMsg("000", group, eventID, origin);
            }
        };


       JavaDStream<CmpMsg>  msgStream = inputStream.flatMap(flatMapFun);

       JavaPairDStream<String, CmpMsg> pairDstream = msgStream.mapToPair(msg-> new Tuple2<>(msg.getAttributeIndex(), msg));

        JavaMapWithStateDStream<String, CmpMsg, CmpMsg, String> msgWithState =
               pairDstream.mapWithState(StateSpec.function(stateFuncTest));

        msgWithState.filter(s->s.length()>10).dstream().saveAsTextFiles(userDir+"/output/", "json");

        jsc.start();
        long startTime=System.currentTimeMillis();
        while((System.currentTimeMillis()-startTime)<2000) Thread.sleep(10);
        jsc.stop();

    }
}
