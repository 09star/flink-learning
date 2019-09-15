package me.raostar.cep;

import me.raostar.model.SimpleTwoKeyEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PatternLearning {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final List<SimpleTwoKeyEvent> inputList = new ArrayList<>();
        inputList.add(new SimpleTwoKeyEvent("a", "2",1.00));
        inputList.add(new SimpleTwoKeyEvent("a", "2",10.00));
        inputList.add(new SimpleTwoKeyEvent("a", "3",10.00));
        inputList.add(new SimpleTwoKeyEvent("a", "1",2.00));
        inputList.add(new SimpleTwoKeyEvent("a", "1",3.00));
        inputList.add(new SimpleTwoKeyEvent("a", "2",4.00));
        inputList.add(new SimpleTwoKeyEvent("a","1", 5.00));



        env.setParallelism(1);

        DataStream<SimpleTwoKeyEvent> events = env.fromCollection(inputList);

//        Pattern<SimpleTwoKeyEvent,?> pattern1 = Pattern.<SimpleTwoKeyEvent>begin("end").where(new IterativeCondition<SimpleTwoKeyEvent>() {
//            @Override
//            public boolean filter(SimpleTwoKeyEvent SimpleTwoKeyEvent, Context<SimpleTwoKeyEvent> context) throws Exception {
//                return true;
//            }
//        }).timesOrMore(2);
//
//        PatternStream<SimpleTwoKeyEvent> patternStream1 = CEP.pattern(events.keyBy(SimpleTwoKeyEvent::getIdNo), pattern1);
//
//        DataStream<SimpleTwoKeyEvent> results1 = patternStream1.select(
//                new PatternSelectFunction<SimpleTwoKeyEvent, SimpleTwoKeyEvent>() {
//                    @Override
//                    public SimpleTwoKeyEvent select(Map<String, List<SimpleTwoKeyEvent>> map) throws Exception {
//                        System.out.println("----- pattern1 start----");
//                        List<SimpleTwoKeyEvent> selectEvents = map.get("end");
//                        for (SimpleTwoKeyEvent event:selectEvents
//                             ) {
//                            System.out.println(event);
//
//                        }
//                        System.out.println("-----pattern1 end----");
//                        return selectEvents.get(selectEvents.size()-1);
//                    }
//                }
//        );

        /**
         * AfterMatchSkipStrategy.noSkip()
         *
         */
//        Pattern<SimpleTwoKeyEvent,?> pattern2 = Pattern.<SimpleTwoKeyEvent>begin("end", AfterMatchSkipStrategy.noSkip()).where(new IterativeCondition<SimpleTwoKeyEvent>() {
//            @Override
//            public boolean filter(SimpleTwoKeyEvent SimpleTwoKeyEvent, Context<SimpleTwoKeyEvent> context) throws Exception {
//                return true;
//            }
//        }).timesOrMore(2);
//
//        PatternStream<SimpleTwoKeyEvent> patternStream2 = CEP.pattern(events.keyBy(SimpleTwoKeyEvent::getIdNo), pattern2);
//
//        DataStream<SimpleTwoKeyEvent> results2 = patternStream2.select(
//                new PatternSelectFunction<SimpleTwoKeyEvent, SimpleTwoKeyEvent>() {
//                    @Override
//                    public SimpleTwoKeyEvent select(Map<String, List<SimpleTwoKeyEvent>> map) throws Exception {
//                        System.out.println("-----pattern2 start----");
//                        List<SimpleTwoKeyEvent> selectEvents = map.get("end");
//                        for (SimpleTwoKeyEvent event:selectEvents
//                        ) {
//                            System.out.println(event);
//
//                        }
//                        System.out.println("-----pattern2 end----");
//                        return selectEvents.get(selectEvents.size()-1);
//                    }
//                }
//        );
        /**
         * AfterMatchSkipStrategy.skipToFirst()
         *
         */
        /**
         * Discards every partial match that contains event of the match preceding the first of *PatternName*.
         * 丢弃每一个部分匹配。 这些部分匹配包含事件，这些事件 先于 *PatternName*事件中第一个
         * @param patternName the pattern name to skip to
         * @return the created AfterMatchSkipStrategy
         */
        Pattern<SimpleTwoKeyEvent,?> pattern3 = Pattern.<SimpleTwoKeyEvent>begin("end", AfterMatchSkipStrategy.skipToFirst("end")).where(new IterativeCondition<SimpleTwoKeyEvent>() {
            @Override
            public boolean filter(SimpleTwoKeyEvent SimpleTwoKeyEvent, Context<SimpleTwoKeyEvent> context) throws Exception {
                return true;
            }
        }).timesOrMore(3).within(Time.minutes(20));

        PatternStream<SimpleTwoKeyEvent> patternStream3 = CEP.pattern(events.keyBy(SimpleTwoKeyEvent::getIdNo).keyBy(SimpleTwoKeyEvent::getKey2), pattern3);

        DataStream<SimpleTwoKeyEvent> results3 = patternStream3.select(
                new PatternSelectFunction<SimpleTwoKeyEvent, SimpleTwoKeyEvent>() {
                    @Override
                    public SimpleTwoKeyEvent select(Map<String, List<SimpleTwoKeyEvent>> map) throws Exception {
                        System.out.println("-----pattern3 start----");
                        List<SimpleTwoKeyEvent> selectEvents = map.get("end");
                        for (SimpleTwoKeyEvent event:selectEvents
                        ) {
                            System.out.println(event);

                        }
                        System.out.println("-----pattern3 end----");
                        return selectEvents.get(selectEvents.size()-1);
                    }
                }
        );

        /**
         * AfterMatchSkipStrategy.skipToFirst()
         *
         */
        /**
         * Discards every partial match that contains event of the match preceding the last of *PatternName*.
         * 丢弃每一个部分匹配。 这些部分匹配包含事件，这些事件 先于 *PatternName*事件中最后一个
         * @param patternName the pattern name to skip to
         * @return the created AfterMatchSkipStrategy
         */
//        Pattern<SimpleTwoKeyEvent,?> pattern4 = Pattern.<SimpleTwoKeyEvent>begin("end", AfterMatchSkipStrategy.skipToLast("end")).where(new IterativeCondition<SimpleTwoKeyEvent>() {
//            @Override
//            public boolean filter(SimpleTwoKeyEvent SimpleTwoKeyEvent, Context<SimpleTwoKeyEvent> context) throws Exception {
//                return true;
//            }
//        }).timesOrMore(2);
//
//        PatternStream<SimpleTwoKeyEvent> patternStream4 = CEP.pattern(events.keyBy(SimpleTwoKeyEvent::getIdNo), pattern4);
//
//        DataStream<SimpleTwoKeyEvent> results4 = patternStream4.select(
//                new PatternSelectFunction<SimpleTwoKeyEvent, SimpleTwoKeyEvent>() {
//                    @Override
//                    public SimpleTwoKeyEvent select(Map<String, List<SimpleTwoKeyEvent>> map) throws Exception {
//                        System.out.println("-----pattern4 start----");
//                        List<SimpleTwoKeyEvent> selectEvents = map.get("end");
//                        for (SimpleTwoKeyEvent event:selectEvents
//                        ) {
//                            System.out.println(event);
//
//                        }
//                        System.out.println("-----pattern4 end----");
//                        return selectEvents.get(selectEvents.size()-1);
//                    }
//                }
//        );

        // execute program
        env.execute("Flink CEP Pattern Learning");
    }
}

