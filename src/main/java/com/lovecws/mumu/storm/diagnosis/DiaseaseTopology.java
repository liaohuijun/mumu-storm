package com.lovecws.mumu.storm.diagnosis;

import com.lovecws.mumu.storm.diagnosis.filter.DiseaseFilter;
import com.lovecws.mumu.storm.diagnosis.function.CityAssignment;
import com.lovecws.mumu.storm.diagnosis.function.DispatchAlert;
import com.lovecws.mumu.storm.diagnosis.function.HourAssignment;
import com.lovecws.mumu.storm.diagnosis.function.OutbreakDetector;
import com.lovecws.mumu.storm.diagnosis.spout.DiagnosisEventSpout;
import com.lovecws.mumu.storm.diagnosis.state.OutbreakTridentFactory;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: triden
 * @date 2017-11-06 17:26
 */
public class DiaseaseTopology {

    public static void main(String[] args) {
        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("event", new DiagnosisEventSpout())
                .each(new Fields("event"), new DiseaseFilter())
                .each(new Fields("event"), new CityAssignment(), new Fields("city"))
                .each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))
                .persistentAggregate(new OutbreakTridentFactory(), new Count(), new Fields("count"))
                .newValuesStream()
                .each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
                .each(new Fields("alert"), new DispatchAlert(), new Fields());
        StormTopology topology = tridentTopology.build();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("DiaseaseTopology", new HashMap(), topology);
    }
}
