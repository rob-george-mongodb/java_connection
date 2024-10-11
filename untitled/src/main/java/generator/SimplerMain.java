package generator;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.connection.TransportSettings;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
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


@SuppressWarnings("NullableProblems")
public class SimplerMain {
    private static final Logger LOGGER = LoggerFactory.getLogger("server-monitor-listener");


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
            numWasters = 16;
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

        MongoClientSettings settings = settingsBuilder.build();

        try (MongoClient mongoClient = MongoClients.create(settings)) {
            MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("test");
            List<CpuWaster> wasters = startThreads(CpuWaster::new, numWasters);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> wasters.forEach(CpuWaster::stop)));

            //noinspection InfiniteLoopStatement
            while (true) {
                try {
                    collection.find().first();
                } catch (MongoException e) {
                    // ignore
                }
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
