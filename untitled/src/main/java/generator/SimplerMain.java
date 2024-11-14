package generator;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.connection.TransportSettings;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


@SuppressWarnings("NullableProblems")
public class SimplerMain {
    private static final Logger LOGGER = LoggerFactory.getLogger("server-monitor-listener");
    private static int WRITE_QUEUE_DEPTH = 1; // High write volumes can easily overwhelm lower tier clusters
    private static int READ_QUEUE_DEPTH = 1000; // They handle crazy read ops perfectly

    @Nullable
    private static String error_reporting_url = null;


    private static <R extends Runnable> List<R> startThreads(final Supplier<R> runnableSupplier, int n) {
        final List<R> runnableList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            R runnable = runnableSupplier.get();
            runnableList.add(runnable);
            Thread t = new Thread(runnable);
            t.setDaemon(true);
            t.start();
        }
        return runnableList;
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("h", "help", false, "print this message");
        options.addOption("n", "networkType", true, "network type to use");
        options.addOption("a", "uri", true, "mongodb uri");
        options.addOption("w", "cpuWasters", true, "Threads to spin and do nothing");
        options.addOption("wt", "writeThreads", true, "Number of write threads");
        options.addOption("wq", "writeQueueDepth", true,
            "Number of max outstanding async ops per write thread");
        options.addOption("rt", "readThreads", true, "Number of read threads");
        options.addOption("rq", "readQueueDepth", true,
            "Number of max outstanding async ops per read thread");
        options.addOption("cp", "connectionPool", true, "Connection pool size");
        options.addOption("st", "serverTimeout", true, "Server selection timeout in ms");
        options.addOption("ht", "heartbeatFrequency", true, "Heartbeat frequency in ms");
        options.addOption("ee", "errorEndpoint", true, "Endpoint to POST errors to");
        DefaultParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp("java -jar mt.jar", options);
            return;
        }

        ConnectionString connectionString = new ConnectionString(
            !cmd.hasOption("uri")
                ? "mongodb://localhost/?directConnection=false"
                : cmd.getOptionValue("uri"));

        var settingsBuilder = MongoClientSettings.builder()
            .applyConnectionString(connectionString);

        if (cmd.hasOption("n")) {
            System.out.println("Using netty stack");
            settingsBuilder.transportSettings(TransportSettings.nettyBuilder()
                .build());
        } else {
            System.out.println("Using default transport");
        }

        final int numWasters;
        if (cmd.hasOption("w")) {
            numWasters = Integer.parseInt(cmd.getOptionValue("w"));
        } else {
            numWasters = 0;
        }

        settingsBuilder.applyToServerSettings(builder ->
            builder.addServerMonitorListener(new ServerMonitorListener() {
                @Override
                public void serverHearbeatStarted(ServerHeartbeatStartedEvent event) {
                    LOGGER.info("Starting heartbeat on {}",
                        event.getConnectionId().getServerId().getAddress());
                }

                @Override
                public void serverHeartbeatSucceeded(ServerHeartbeatSucceededEvent event) {
                    LOGGER.info("Heartbeat succeeded on {} in {}",
                        event.getConnectionId().getServerId().getAddress(),
                        event.getElapsedTime(TimeUnit.MILLISECONDS));
                }

                @Override
                public void serverHeartbeatFailed(ServerHeartbeatFailedEvent event) {
                    LOGGER.info("Heartbeat failed on {} in {}",
                        event.getConnectionId().getServerId().getAddress(),
                        event.getElapsedTime(TimeUnit.MILLISECONDS), event.getThrowable());
                }
            }));

        if (cmd.hasOption("cp")) {
            settingsBuilder.applyToConnectionPoolSettings(builder ->
                builder.maxSize(Integer.parseInt(cmd.getOptionValue("cp"))));
        }
        if(cmd.hasOption("st")) {
            settingsBuilder.applyToClusterSettings(builder -> builder.serverSelectionTimeout(Integer.parseInt(cmd.getOptionValue("st")), TimeUnit.MILLISECONDS));
        }

        if(cmd.hasOption("ht")){
            settingsBuilder.applyToServerSettings(builder -> builder.heartbeatFrequency(Integer.parseInt(cmd.getOptionValue("ht")), TimeUnit.MILLISECONDS));
        }

        MongoClientSettings settings = settingsBuilder.build();

        final int numWriters;
        if (cmd.hasOption("wt")) {
            numWriters = Integer.parseInt(cmd.getOptionValue("wt"));
        } else {
            numWriters = 0;
        }

        final int numReaders;
        if (cmd.hasOption("rt")) {
            numReaders = Integer.parseInt(cmd.getOptionValue("rt"));
        } else {
            numReaders = 0;
        }
        if (cmd.hasOption("rq")) {
            READ_QUEUE_DEPTH = Integer.parseInt(cmd.getOptionValue("rq"));
        }
        if(cmd.hasOption("wq")){
            WRITE_QUEUE_DEPTH = Integer.parseInt(cmd.getOptionValue("wq"));
        }

        if(cmd.hasOption("ee")){
            error_reporting_url = cmd.getOptionValue("ee");
            postCurrentTime("starting up now - using " + connectionString);
        }

        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoCollection<Document> collection = mongoClient.getDatabase("test")
                .getCollection("test");
            List<CpuWaster> wasters = startThreads(CpuWaster::new, numWasters);
            List<TrivialWriter> writers = startThreads(() -> new TrivialWriter(collection),
                numWriters);
            List<TrivialReader> readers = startThreads(() -> new TrivialReader(collection),
                numReaders);
            Runtime.getRuntime().addShutdownHook(new Thread(() ->
            {
                wasters.forEach(CpuWaster::stop);
                writers.forEach(TrivialWriter::stop);
                readers.forEach(TrivialReader::stop);
            }));

            final Logger logger = LoggerFactory.getLogger("timings");
            //noinspection InfiniteLoopStatement
            while (true) {
                try {
                    Mono.from(collection.find().first()).block();
                } catch (MongoException e) {
                    logger.error("Exception caught in main", e);
                }
            }
        }


    }


    // copilot generated - probably waaay more verbose than necessary
    public static void postCurrentTime(String body) {
        if(error_reporting_url == null){
            return;
        }
        try {
            URL url = new URL(error_reporting_url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setDoOutput(true);

            var escapedJson = body.replace("\"", "\\\"");
            String jsonInputString = "{\"currentTime\": \"" + Instant.now().toString() + "\", \"body\":\"" + escapedJson +  "\"}";

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                LOGGER.error("POST request failed with response code: " + responseCode);
            }
        } catch (Exception e) {
            LOGGER.error("Exception caught while posting current time", e);
        }
    }

    public static class TrivialReader implements Runnable{
        private volatile boolean flag = true;
        private final MongoCollection<Document> collection;
        private final Logger logger = LoggerFactory.getLogger("timings");
        private int NUM_IN_PROGRESS_OPS = 0;

        public TrivialReader(MongoCollection<Document> collection){
            this.collection = collection;
        }

        @Override
        public void run()
        {
            while(flag){
                try{
                    collection.find().limit(1).subscribe(new ReadSubscriber());
                    NUM_IN_PROGRESS_OPS++;

                    var doc = Mono.from(collection.find().limit(1)).block();
                } catch (MongoException e){
                    postCurrentTime(e.toString());
                    logger.error("Exception caught", e);
                }

                if(NUM_IN_PROGRESS_OPS > READ_QUEUE_DEPTH){
                    logger.trace("Queue depth exceeded");
                    try{
                        Thread.sleep(1000);
                    } catch (InterruptedException e){
                        logger.error("Interrupted", e);
                    }
                }
            }
        }

        public void stop(){
            flag = false;
        }

        private class ReadSubscriber implements org.reactivestreams.Subscriber<Document>{
            private long nanoTime;

            @Override
            public void onSubscribe(org.reactivestreams.Subscription s){
                s.request(1);
                nanoTime = System.nanoTime();
            }

            @Override
            public void onNext(Document result){
                long elapsedMs = (System.nanoTime() - nanoTime) / 1_000_000;
                logger.info("Found document {} took {}", result, elapsedMs);
            }

            @Override
            public void onError(Throwable t){
                long elapsedMs = (System.nanoTime() - nanoTime) / 1_000_000;
                postCurrentTime(t.toString());
                logger.error("Error inserting document; took {}", elapsedMs, t);
            }

            @Override
            public void onComplete(){
                long elapsedMs = (System.nanoTime() - nanoTime) / 1_000_000;
                NUM_IN_PROGRESS_OPS--;
                logger.info("Complete; took {}, depth {}", elapsedMs, NUM_IN_PROGRESS_OPS);
            }

        }
    }



    public static class TrivialWriter implements Runnable{
        private volatile boolean flag = true;
        private final MongoCollection<Document> collection;
        private final Logger logger = LoggerFactory.getLogger("timings");
        private int NUM_IN_PROGRESS_OPS = 0;

        public TrivialWriter(MongoCollection<Document> collection){
            this.collection = collection;
        }

        @Override
        public void run()
        {
            while(flag){
                try{
                    collection.insertOne(new Document("key", "value")).subscribe(new WriteSubscriber());
                    NUM_IN_PROGRESS_OPS++;
                } catch (MongoException e){
                    postCurrentTime(e.toString());
                    logger.error("Exception caught", e);
                }

                if(NUM_IN_PROGRESS_OPS > WRITE_QUEUE_DEPTH){
                    logger.trace("Queue depth exceeded");
                    try{
                        Thread.sleep(1000);
                    } catch (InterruptedException e){
                        logger.error("Interrupted", e);
                    }
                }
            }
        }

        public void stop(){
            flag = false;
        }

        private class WriteSubscriber implements org.reactivestreams.Subscriber<com.mongodb.client.result.InsertOneResult> {
            private long nanoTime;

            @Override
            public void onSubscribe(org.reactivestreams.Subscription s){
                s.request(1);
                nanoTime = System.nanoTime();
            }

            @Override
            public void onNext(InsertOneResult result){
                long elapsedMs = (System.nanoTime() - nanoTime) / 1_000_000;
                logger.info("Inserted document with id {} took {}", result.getInsertedId(), elapsedMs);
            }

            @Override
            public void onError(Throwable t){
                long elapsedMs = (System.nanoTime() - nanoTime) / 1_000_000;
                postCurrentTime(t.toString());
                logger.error("Error inserting document; took {}", elapsedMs, t);
            }

            @Override
            public void onComplete(){
                long elapsedMs = (System.nanoTime() - nanoTime) / 1_000_000;
                NUM_IN_PROGRESS_OPS--;
                logger.info("Complete; took {}, depth {}", elapsedMs, NUM_IN_PROGRESS_OPS);
            }

        }
    }

    public static class CpuWaster implements Runnable {
        private volatile boolean flag = true;
        @SuppressWarnings("unused")
        private double sum;

        @Override
        public void run() {
            while (flag) {
                // Perform some calculations to waste CPU
                double x = Math.random();
                double y = Math.random();
                double z = Math.pow(x, y);
                sum += z;
            }
        }

        public void stop() {
            flag = false;
        }
    }
}
