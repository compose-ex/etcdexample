package compose;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.api.TxnResponse;
import com.ibm.etcd.client.*;
import com.ibm.etcd.client.config.ComposeTrustManagerFactory;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.WatchUpdate;

import io.grpc.stub.StreamObserver;
import org.slf4j.LoggerFactory;
import static com.ibm.etcd.client.KeyUtils.bs;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class App {

    private final static org.slf4j.Logger log = LoggerFactory.getLogger(App.class.getName());

    public static void main(String[] args) {
        System.out.println("Starting");
        KvStoreClient client = null;

        // Connecting

        try {
            client = EtcdClient.forEndpoints("hostportal1.threetcd.compose-3.composedb.com:18279,hostpostal2.threetcd.compose-3.composedb.com:18279")
                    .withCredentials("root", "secret")
                    .withTrustManager(new ComposeTrustManagerFactory("threetcd"))
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        }

        // Setting up a threadpool

        ExecutorService threadpool=Executors.newFixedThreadPool(10);

        KvClient kvclient = client.getKvClient();

        final StreamObserver<WatchUpdate> observer = new StreamObserver<WatchUpdate>() {

            @Override
            public void onNext(WatchUpdate value) {
                System.out.println("watch event: "+value);
            }
            @Override
            public void onError(Throwable t) {
                System.out.println("watch error: "+t);
            }
            @Override
            public void onCompleted() {
                System.out.println("watch completed");
            }
        };
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        // Set the watcher to report all key changes
        KvClient.Watch watch = kvclient.watch(KvClient.ALL_KEYS).executor(threadpool).start(observer);

        // A simple sync put
        kvclient.put(bs("hello"), bs("test")).sync();

        // A simple sync get
        RangeResponse result = kvclient.get(bs("hello")).sync();

        dumpRangeResponse(result);

        // Another simple sync put
        kvclient.put(bs("hello"), bs("test update")).sync();

        // Creating a listener for async results
        ListeningExecutorService service = MoreExecutors.listeningDecorator(threadpool);

        // Running an async get
        final ListenableFuture<RangeResponse> fresult = kvclient.get(bs("hello")).async();

        // Adding a listener to the async future
        fresult.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    final RangeResponse rr = fresult.get();
                    dumpRangeResponse(rr);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                } catch (ExecutionException e) {
                    log.error("Exception in task", e.getCause());
                }
            }
        }, service);

        // Close the watch so we can make a new one
        watch.close();

        watch = kvclient.watch(bs("/hello/")).asPrefix().executor(threadpool).start(observer);

        for(int n=1;n<100;n++) {
            kvclient.put(bs("/hello/" + n), bs("test")).sync();
        }

        // Clearing out all the keys
        kvclient.delete(KvClient.ALL_KEYS).sync();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        service.shutdown();

        System.out.println("Done");
    }

    public static void dumpRangeResponse(RangeResponse rr) {
        for (int i = 0; i < rr.getKvsCount(); i++) {
            KeyValue kv = rr.getKvs(i);
            System.out.println(i + " : " + kv.getKey().toStringUtf8() + " : " + kv.getValue().toStringUtf8());
        }
    }
}
