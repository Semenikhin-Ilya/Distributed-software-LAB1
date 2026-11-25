package echo.asyncio;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncClient implements Runnable {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    private final int messageCount;
    private final List<Double> rttList;
    private final CountDownLatch readyLatch;
    private final CountDownLatch startLatch;

    public AsyncClient(int messageCount, List<Double> rttList,
                       CountDownLatch readyLatch, CountDownLatch startLatch) {
        this.messageCount = messageCount;
        this.rttList = rttList;
        this.readyLatch = readyLatch;
        this.startLatch = startLatch;
    }

    @Override
    public void run() {
        try (Socket socket = new Socket(HOST, PORT);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

            socket.setTcpNoDelay(true);

            // Warm-up на этом соединении, вне замера.
            out.write("warmup\n");
            out.flush();
            in.readLine();

            // Сигналим «я подключился и прогрет», ждём команды «начали».
            readyLatch.countDown();
            startLatch.await();

            for (int i = 0; i < messageCount; i++) {
                String clientMessage = "Hello " + i + "\n";

                long timeStart = System.nanoTime();
                out.write(clientMessage);
                out.flush();

                String serverMessage = in.readLine();
                long timeEnd = System.nanoTime();

                if (serverMessage == null) return;
                rttList.add((timeEnd - timeStart) / 1_000_000.0);
            }

            socket.shutdownOutput();

        } catch (UnknownHostException e) {
            System.err.println("Неизвестный хост: " + HOST);
        } catch (IOException e) {
            System.err.println("Ошибка ввода-вывода: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Использование: AsyncClient <сообщений на клиента> <количество клиентов>");
            System.exit(1);
        }
        int messagesToSend = Integer.parseInt(args[0]);
        int concurrentClients = Integer.parseInt(args[1]);

        List<Double> rttList = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch readyLatch = new CountDownLatch(concurrentClients);
        CountDownLatch startLatch = new CountDownLatch(1);

        System.out.println("Запуск " + concurrentClients + " клиентов по "
                + messagesToSend + " сообщений...");

        Thread.sleep(1000);

        ExecutorService executor = Executors.newFixedThreadPool(concurrentClients);
        for (int i = 0; i < concurrentClients; i++) {
            executor.submit(new AsyncClient(messagesToSend, rttList, readyLatch, startLatch));
        }

        // Ждём, пока все клиенты установили соединение и прогрелись.
        if (!readyLatch.await(5, TimeUnit.MINUTES)) {
            System.err.println("Не все клиенты подключились за 5 минут, прерываем.");
            executor.shutdownNow();
            return;
        }

        // Стартуем замер только на окне передачи сообщений.
        long wallStart = System.nanoTime();
        startLatch.countDown();

        executor.shutdown();
        if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
            System.err.println("Тест не завершился за 10 минут, прерываем.");
            executor.shutdownNow();
        }

        long wallEnd = System.nanoTime();
        double wallMs = (wallEnd - wallStart) / 1_000_000.0;

        printStatistics(rttList, wallMs, concurrentClients);
    }

    private static void printStatistics(List<Double> rttList, double wallMs, int concurrentClients) {
        if (rttList.isEmpty()) {
            System.out.println("Нет данных для статистики.");
            return;
        }

        List<Double> sorted = new ArrayList<>(rttList);
        Collections.sort(sorted);

        int n = sorted.size();
        double minRtt = sorted.get(0);
        double maxRtt = sorted.get(n - 1);

        double sumRtt = 0.0;
        for (double rtt : sorted) sumRtt += rtt;
        double avgRtt = sumRtt / n;

        int idx = Math.max(0, Math.min((int) Math.ceil(0.95 * n) - 1, n - 1));
        double p95Rtt = sorted.get(idx);

        double throughput = n * 1000.0 / wallMs;

        System.out.println("\n=== Результаты тестирования (" + concurrentClients + " клиентов) ===");
        System.out.printf(Locale.US, "Всего сообщений: %d%n", n);
        System.out.printf(Locale.US, "Average RTT: %f ms%n", avgRtt);
        System.out.printf(Locale.US, "Min RTT: %f ms%n", minRtt);
        System.out.printf(Locale.US, "Max RTT: %f ms%n", maxRtt);
        System.out.printf(Locale.US, "95th percentile RTT: %f ms%n", p95Rtt);
        System.out.printf(Locale.US, "Sum RTT: %f ms%n", sumRtt);
        System.out.printf(Locale.US, "Transmission wall-clock time (excl. connect): %f ms%n", wallMs);
        System.out.printf(Locale.US, "Throughput: %.2f msg/sec%n", throughput);
    }
}