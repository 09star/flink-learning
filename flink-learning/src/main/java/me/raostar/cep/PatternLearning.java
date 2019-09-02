package me.raostar.cep;

import me.raostar.model.SimpleEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PatternLearning {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final List<SimpleEvent> inputList = new ArrayList<>();
        inputList.add(new SimpleEvent("a", 1.00));
        inputList.add(new SimpleEvent("b", 10.00));
        inputList.add(new SimpleEvent("c", 10.00));
        inputList.add(new SimpleEvent("a", 2.00));
        inputList.add(new SimpleEvent("a", 3.00));



        DataStream<SimpleEvent> events = env.fromCollection(inputList);

        Pattern<SimpleEvent,?> pattern = Pattern.<SimpleEvent>begin("end").where(new IterativeCondition<SimpleEvent>() {
            @Override
            public boolean filter(SimpleEvent simpleEvent, Context<SimpleEvent> context) throws Exception {
                return true;
            }
        }).timesOrMore(2);

        PatternStream<SimpleEvent> patternStream = CEP.pattern(events.keyBy(SimpleEvent::getIdNo), pattern);

        DataStream<SimpleEvent> results = patternStream.select(
                new PatternSelectFunction<SimpleEvent, SimpleEvent>() {
                    @Override
                    public SimpleEvent select(Map<String, List<SimpleEvent>> map) throws Exception {
                        System.out.println("-----start----");
                        List<SimpleEvent> selectEvents = map.get("end");
                        for (SimpleEvent event:selectEvents
                             ) {
                            System.out.println(event);

                        }
                        System.out.println("-----end----");
                        return selectEvents.get(selectEvents.size()-1);
                    }
                }
        );


        // execute program
        env.execute("Flink CEP Pattern Learning");
    }
}

