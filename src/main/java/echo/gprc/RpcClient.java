package echo.gprc;

import echo.rpc.EchoRequest;
import echo.rpc.EchoResponse;
import echo.rpc.EchoServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Клиент задания 1.3.
 *
 * Режимы:
 *   rtt    — последовательный обмен: одно сообщение -> ответ -> замер RTT -> следующее.
 *            Сопоставимо с TCP и асинхронным клиентом.
 *   stream — конвейерный: отправляем все N, потом собираем N ответов.
 *            Даёт честный throughput, но не per-message RTT.
 *
 * Перед замером выполняются два warm-up'а:
 *   1) глобальный (прогрев канала, HTTP/2, Netty, JIT);
 *   2) на каждом новом стриме (установка конкретного HTTP/2-стрима вне замера).
 *
 * Запуск: java echo.gprc.RpcClient <N> [rtt|stream]
 */
public class RpcClient {
    private static final String HOST = "localhost";
    private static final int PORT = 8080;
    private static final int GLOBAL_WARMUP = 200;
    private static final int STREAM_WARMUP = 5;
    private static final long TIMEOUT_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        int messagesToSend = args.length > 0 ? Integer.parseInt(args[0]) : 1000;
        String mode = args.length > 1 ? args[1].toLowerCase(Locale.ROOT) : "rtt";
        if (!mode.equals("rtt") && !mode.equals("stream")) {
            System.err.println("Режим должен быть 'rtt' или 'stream'");
            System.exit(1);
        }

        Thread.sleep(1000);

        ManagedChannel channel = ManagedChannelBuilder.forAddress(HOST, PORT)
                .usePlaintext()
                .build();

        try {
            System.out.println("Global warm-up (" + GLOBAL_WARMUP + " сообщений)...");
            runTest(channel, GLOBAL_WARMUP, mode, false);
            System.out.println("Warm-up завершён.");

            runTest(channel, messagesToSend, mode, true);
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static void runTest(ManagedChannel channel, int n, String mode, boolean print)
            throws InterruptedException {

        EchoServiceGrpc.EchoServiceStub stub = EchoServiceGrpc.newStub(channel);

        BlockingQueue<EchoResponse> queue = new LinkedBlockingQueue<>();
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        StreamObserver<EchoResponse> responseObserver = new StreamObserver<>() {
            @Override public void onNext(EchoResponse r) { queue.offer(r); }
            @Override public void onError(Throwable t) { error.set(t); completed.countDown(); }
            @Override public void onCompleted() { completed.countDown(); }
        };

        StreamObserver<EchoRequest> requestObserver = stub.streamEcho(responseObserver);

        // Warm-up на этом стриме: устанавливаем HTTP/2-стрим вне замера.
        int streamWarmup = print ? STREAM_WARMUP : 0;
        for (int i = 0; i < streamWarmup; i++) {
            requestObserver.onNext(EchoRequest.newBuilder()
                    .setMessage("warmup")
                    .setSequenceNumber(-1)
                    .build());
            if (queue.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS) == null) {
                throw new RuntimeException("Таймаут на stream warm-up #" + i);
            }
        }

        List<Double> rtts = new ArrayList<>(n);
        long wallStart = System.nanoTime();

        if (mode.equals("rtt")) {
            for (int i = 0; i < n; i++) {
                EchoRequest req = EchoRequest.newBuilder()
                        .setMessage("Hello " + i)
                        .setSequenceNumber(i)
                        .build();

                long start = System.nanoTime();
                requestObserver.onNext(req);

                EchoResponse resp = queue.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (resp == null) throw new RuntimeException("Таймаут ответа #" + i);

                long end = System.nanoTime();
                if (resp.getSequenceNumber() != i) {
                    System.err.println("Неожиданный seq: " + resp.getSequenceNumber()
                            + " (ожидали " + i + ")");
                }
                rtts.add((end - start) / 1_000_000.0);
            }
        } else {
            for (int i = 0; i < n; i++) {
                requestObserver.onNext(EchoRequest.newBuilder()
                        .setMessage("Hello " + i)
                        .setSequenceNumber(i)
                        .build());
            }
            for (int i = 0; i < n; i++) {
                if (queue.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS) == null) {
                    throw new RuntimeException("Таймаут ответа #" + i);
                }
            }
        }

        long wallEnd = System.nanoTime();
        requestObserver.onCompleted();
        completed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (error.get() != null) {
            throw new RuntimeException("Ошибка gRPC: " + error.get().getMessage(), error.get());
        }

        if (print) {
            double wallMs = (wallEnd - wallStart) / 1_000_000.0;
            printStats(mode, n, rtts, wallMs);
        }
    }

    private static void printStats(String mode, int n, List<Double> rtts, double wallMs) {
        System.out.println("\n=== Результаты тестирования (gRPC, режим " + mode + ") ===");

        if (!rtts.isEmpty()) {
            List<Double> sorted = new ArrayList<>(rtts);
            Collections.sort(sorted);

            int size = sorted.size();
            double sum = 0.0;
            for (double v : sorted) sum += v;
            double avg = sum / size;
            int idx = Math.max(0, Math.min((int) Math.ceil(0.95 * size) - 1, size - 1));

            System.out.printf(Locale.US, "Messages sent: %d%n", size);
            System.out.printf(Locale.US, "Average RTT: %f ms%n", avg);
            System.out.printf(Locale.US, "Min RTT: %f ms%n", sorted.get(0));
            System.out.printf(Locale.US, "Max RTT: %f ms%n", sorted.get(size - 1));
            System.out.printf(Locale.US, "95th percentile RTT: %f ms%n", sorted.get(idx));
            System.out.printf(Locale.US, "Sum RTT: %f ms%n", sum);
        }

        System.out.printf(Locale.US, "Transmission wall-clock time (excl. connect): %f ms%n", wallMs);
        System.out.printf(Locale.US, "Throughput: %.2f msg/sec%n", n * 1000.0 / wallMs);
    }
}