
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.thrift.transport.TTransportException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DrpcResultsPrinter {
    private static final Logger LOG = LoggerFactory.getLogger(DrpcResultsPrinter.class);

    private final DistributedRPC.Iface drpcClient;

    public DrpcResultsPrinter(DistributedRPC.Iface drpcClient) {
        this.drpcClient = drpcClient;
    }

    /**
     * @return local DRPC client running on the same JVML
     */
    public static DrpcResultsPrinter localClient() {
        return new DrpcResultsPrinter(new LocalDRPC());
    }

    /**
     * @return remote DRPC client running on local host, on port 3772, with defaults.yaml config
     */
    public static DrpcResultsPrinter remoteClient() {
        return remoteClient(Utils.readDefaultConfig(), "localhost", 3772);
    }

    /**
     * @return remote DRPC client running on the specified host, port, with the provided config
     */
    public static DrpcResultsPrinter remoteClient(Map<String, Object> config, String host, int port) {
        try {
            return new DrpcResultsPrinter(new DRPCClient(config, host,port));
        } catch (TTransportException e) {
            throw new RuntimeException(String.format("DRPC Client failed to connect to DRPC server. " +
                    "[host = %s], [port = %s], [config = %s]", host, port, config));
        }
    }

    /**
     * Prints the DRPC results for the number of times specified, sleeping the specified time in between prints
     */
    public void printResults(int num, int sleepTime, TimeUnit sleepUnit) {
        for (int i = 0; i < num; i++) {
            try {
                LOG.info("--- DRPC RESULT: " + drpcClient.execute("words", "the and apple snow jumped"));
                System.out.println();
                Thread.sleep(sleepUnit.toMillis(sleepTime));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(String[] args) {
        DrpcResultsPrinter.remoteClient().printResults(60, 1, TimeUnit.SECONDS);
    }
}
