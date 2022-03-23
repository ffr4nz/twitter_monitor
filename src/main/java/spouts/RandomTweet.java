package spouts;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class RandomTweet extends BaseRichSpout {
    private static final String[] CHOICES = {
            "RT @edubayon_: El Constitucional falla que Toni Cantó siga fuera de las listas electorales https://t.co/WZkuyN7YWC",
            "RT @tcourtois1i: Meet Fernando, Real Madrid's bus driver of the past 20 years, he has been to more Champions League finals than Liverpool",
            "RT @Arrizabalager: Chelsea v Real Madrid is a European heavyweight tie, there’s 14 European Cups between the two teams.",
            "Estoy en Roma y me voy a London"
    };
    SpoutOutputCollector collector;
    Random rand;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        rand = ThreadLocalRandom.current();
    }

    @Override
    public void nextTuple() {
        String sentence = CHOICES[rand.nextInt(CHOICES.length)];
        Utils.sleep(1000);
        collector.emit(new Values(sentence), sentence);
    }

    @Override
    public void ack(Object id) {
        //Ignored
    }

    @Override
    public void fail(Object id) {
        collector.emit(new Values(id), id);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declare(new Fields("message"));
    }

}
