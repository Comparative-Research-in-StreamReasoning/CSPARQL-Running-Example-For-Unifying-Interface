import IO.Serialization.VariableBindings;
import IO.Serialization.TransferObject;
import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.common.streams.format.GenericObservable;
import eu.larkc.csparql.engine.RDFStreamFormatter;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

public class CSPARQLResultObserver extends RDFStreamFormatter {
    private String queryId;

    public CSPARQLResultObserver(String queryId) throws FileNotFoundException {
        super(queryId);
        this.queryId = queryId;
    }

    public void update(final GenericObservable<RDFTable> observed, final RDFTable q) {
        System.out.println("Receiving");
        long timestamp = System.currentTimeMillis();
        List<VariableBindings> timestampedVariableBindings = new ArrayList<>();
        for (final RDFTuple t : q) {
            List<String> bindings = new ArrayList<>();
            if(q.getNames().size() > 3) {
                String[] resultArr = t.toString().replaceAll("\t", " ").trim().split(" ");
                for(int i = 0; i < resultArr.length; i++)
                    bindings.add(resultArr[i]);
            } else {
                for (int i = 0; i < q.getNames().size(); i++) {
                    bindings.add(t.get(i));
                }
            }
            timestampedVariableBindings.add(new VariableBindings(bindings));
        }
        double usedMB = getCurrentMemoryUsage();
        CSPARQLRunningExample.answers.add(new TransferObject(queryId.split("-")[0], timestampedVariableBindings, usedMB, timestamp));
    }

    private double getCurrentMemoryUsage() {
        System.gc();
        Runtime rt = Runtime.getRuntime();
        return ((rt.totalMemory() - rt.freeMemory()) / 1024.0 / 1024.0);
    }
}