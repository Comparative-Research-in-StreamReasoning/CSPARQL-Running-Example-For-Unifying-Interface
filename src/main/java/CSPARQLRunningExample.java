import IO.FileServer;
import IO.Serialization.Configuration;
import IO.Serialization.TransferObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import eu.larkc.csparql.engine.CsparqlEngineImpl;
import eu.larkc.csparql.engine.CsparqlQueryResultProxy;
import org.apache.commons.io.IOUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;

public class CSPARQLRunningExample {
    private static final Logger logger = LoggerFactory.getLogger(CSPARQLRunningExample.class);
    static CsparqlEngineImpl csparqlEngine;
    public static List<TransferObject> answers = new ArrayList<>();
    public static boolean streamsRunning = true;

    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration config = new Gson().fromJson(IOUtils.toString(new URL("http://localhost:11111/configuration.json"), StandardCharsets.UTF_8), Configuration.class);
        csparqlEngine = new CsparqlEngineImpl();
        csparqlEngine.initialize(false);
        for(String queryId : config.getQueries().keySet()) {
            startCSPARQLStreams(config.getQueries().get(queryId));
            registerCSPARQLQueries(queryId, config.getQueries().get(queryId));
        }
        while (streamsRunning) {
            Thread.sleep(200);
        }
        writeAnswersToURL("http://localhost:11112/answers.json");
    }

    private static void registerCSPARQLQueries(String queryId, String query) {
        CsparqlQueryResultProxy cqrp = null;
        CSPARQLResultObserver cro = null;
        try {
            cqrp = csparqlEngine.registerQuery(query);
            cro = new CSPARQLResultObserver(queryId);
            logger.debug("Registering result observer: " + cro.getIRI());
            csparqlEngine.registerStream(cro);
        } catch (ParseException | FileNotFoundException e) {
            e.printStackTrace();
        }

        cqrp.addObserver(cro);
    }

    private static void startCSPARQLStreams(String query) {
        for(String streamName : getStreamNamesFromQuery(query)) {
            CSPARQLStreamReceiver csr = new CSPARQLStreamReceiver(streamName, getPortNumberFromURL(streamName));
            csparqlEngine.registerStream(csr);
            new Thread(csr).start();
        }
        Thread.yield();
    }

    private static void writeAnswersToURL(String url) {
        try (OutputStream out = new FileOutputStream("answers.json")){
            System.out.println("Answers online");
            out.write(new GsonBuilder().setPrettyPrinting().create().toJson(answers).getBytes(StandardCharsets.UTF_8));
            FileServer fileServer = new FileServer(getPortNumberFromURL(url), url, new Gson().toJson(answers));
            Thread.sleep(20000);
            System.out.println("Answers offline");
            fileServer.stop();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Set<String> getStreamNamesFromQuery(String query) {
        System.out.println(query);
        Set<String> streamNames = new HashSet<>();
        for(String line : query.split("\n")) {
            if(line.matches("(.)*>(\\s)+\\[range(.)*")) {
                streamNames.add(line.split("> \\[range")[0].split("<")[1]);
                System.out.println("Stream found: " + line.split("> \\[range")[0].split("<")[1]);
            }
            if(line.matches("(.)*>(\\s)+\\[RANGE(.)*")) {
                streamNames.add(line.split("> \\[RANGE")[0].split("<")[1]);
                System.out.println("Stream found: " + line.split("> \\[RANGE")[0].split("<")[1]);
            }
        }
        return streamNames;
    }

    private static int getPortNumberFromURL(String url) {
        return Integer.parseInt(url.split(":")[2].split("/")[0]);
    }
}
