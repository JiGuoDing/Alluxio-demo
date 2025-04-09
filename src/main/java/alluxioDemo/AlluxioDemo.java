package alluxioDemo;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import com.google.common.util.concurrent.AtomicDouble;
import io.netty.util.internal.ThreadLocalRandom;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AlluxioDemo {
    private static FileSystem fileSystem;
    private static CreateFilePOptions options;
    private static final int THREAD_COUNT = 20;
    private static final List<String> FILE_PATHS = new ArrayList<>(Arrays.asList("/store_sales_1_16.dat", "/store_sales_1_16.dat-1", "/store_sales_1_16.dat-2", "/store_sales_1_16.dat-3", "/store_sales_1_16.dat-4", "/store_sales_1_16.dat-5", "/store_sales_1_16.dat-6", "/store_sales_1_16.dat-7"));

    @Before
    public void init(){
        AlluxioProperties properties = new AlluxioProperties();
        properties.set(PropertyKey.MASTER_HOSTNAME, "pasak8s-15");
        properties.set(PropertyKey.MASTER_RPC_PORT, 30096);
        properties.set(PropertyKey.WORKER_RPC_PORT, 30095);
        properties.set(PropertyKey.SECURITY_LOGIN_USERNAME, "jgd");
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        fileSystem = FileSystem.Factory.create(conf);
        options = CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).buildPartial();
    }

    @Test
    public void read() throws IOException, AlluxioException {
        AlluxioURI path = new AlluxioURI("/warehouse_1_16.dat");
        // 确认文件是否存在
        // boolean exists = fileSystem.exists(path);
        // System.out.println("File exists: " + exists);
        // 生成输入流
        FileInStream fileInStream = fileSystem.openFile(path);

        // 生成输入流后使用 java 的文件 IO 指令读文件
        InputStreamReader reader = new InputStreamReader(fileInStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(reader);
        bufferedReader.lines().forEach(System.out::println);
        fileInStream.close();
    }

    /**
     * 测试文件读取时间
     */
    public double performTimedRead(AlluxioURI path) throws IOException, AlluxioException {
        long startTime = System.nanoTime();

        FileInStream fileInStream = fileSystem.openFile(path);

        byte[] buffer = new byte[8192];
        long totalBytes = 0;

        while(fileInStream.read(buffer) != -1){
            totalBytes += buffer.length;
        }

        double duration = System.nanoTime() - startTime;
        double throughput = (double) (totalBytes / (1024 * 1024)) / (duration / 1000000000);
        System.out.printf("Duration: %d ms | Size: %.2f MB | Throughput: %.2f MB/s%n",
                duration,
                totalBytes / (1024.0 * 1024.0),
                throughput);
        fileInStream.close();
        return duration;
    }

    @Test
    public void benchmarkRead() throws IOException, AlluxioException {
        AlluxioURI path = new AlluxioURI(FILE_PATHS.get(0));

        performTimedRead(path);
    }

    @Test
    public void concurrentReadBenchmark() throws InterruptedException {
        // 并发测试
        System.out.println("\n=== Concurrent Test (" + THREAD_COUNT + " threads) ===");
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        AtomicDouble totalTime = new AtomicDouble(0);

        long startTime = System.nanoTime();

        // 提交10个读取任务
        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                try {
                    int randomIndex = ThreadLocalRandom.current().nextInt(FILE_PATHS.size());
                    String path = FILE_PATHS.get(randomIndex);
                    // String path = FILE_PATHS.get(0);

                    double threadDuration = performTimedRead(new AlluxioURI(path));
                    System.out.println("Reading " + path);
                    totalTime.addAndGet(threadDuration);
                } catch (IOException | AlluxioException e) {
                    throw new RuntimeException(e);
                } finally {
                    latch.countDown();
                }
            });
        }

        // 等待所有线程完成
        latch.await();
        long totalDurationNs = System.nanoTime() - startTime;
        executor.shutdown();

        // 计算结果
        double totalTimeSec = totalDurationNs / 1_000_000_000.0;
        double avgThreadTimeMs = (totalTime.get() / THREAD_COUNT) / 1_000_000.0;

        System.out.printf("Total elapsed time: %.2f s\n", totalTimeSec);
        System.out.printf("Average thread execution time: %.2f ms\n", avgThreadTimeMs);
    }
}
