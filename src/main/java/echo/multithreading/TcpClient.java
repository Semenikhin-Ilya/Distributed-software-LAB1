package echo.multithreading;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class TcpClient {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 1) {
            System.err.println("Использование: TcpClient <количество сообщений>");
            System.exit(1);
        }
        int messagesToSend = Integer.parseInt(args[0]);
        List<Double> rttList = new ArrayList<>(messagesToSend);

        Thread.sleep(2000);

        double wallMs = 0.0;

        try (Socket socket = new Socket(HOST, PORT);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

            socket.setTcpNoDelay(true);

            // Warm-up: одно сообщение вне замера — исключаем TCP slow start и JIT.
            out.write("warmup\n");
            out.flush();
            in.readLine();

            // Таймер стартует только на передаче сообщений, без соединения.
            long wallStart = System.nanoTime();

            for (int i = 0; i < messagesToSend; i++) {
                String clientMessage = "Hello " + i + "\n";

                long timeStart = System.nanoTime();
                out.write(clientMessage);
                out.flush();

                String serverMessage = in.readLine();
                long timeEnd = System.nanoTime();

                if (serverMessage == null) {
                    System.err.println("Сервер закрыл соединение раньше времени на сообщении " + i);
                    break;
                }
                rttList.add((timeEnd - timeStart) / 1_000_000.0);
            }

            long wallEnd = System.nanoTime();
            wallMs = (wallEnd - wallStart) / 1_000_000.0;

            socket.shutdownOutput();

        } catch (UnknownHostException e) {
            System.err.println("Неизвестный хост: " + HOST);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Ошибка ввода-вывода: " + e.getMessage());
            e.printStackTrace();
        }

        printStatistics(rttList, wallMs);
    }

    private static void printStatistics(List<Double> rttList, double wallMs) {
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

        System.out.printf(Locale.US, "Messages sent: %d%n", n);
        System.out.printf(Locale.US, "Average RTT: %f ms%n", avgRtt);
        System.out.printf(Locale.US, "Min RTT: %f ms%n", minRtt);
        System.out.printf(Locale.US, "Max RTT: %f ms%n", maxRtt);
        System.out.printf(Locale.US, "95th percentile RTT: %f ms%n", p95Rtt);
        System.out.printf(Locale.US, "Sum RTT: %f ms%n", sumRtt);
        System.out.printf(Locale.US, "Transmission wall-clock time (excl. connect): %f ms%n", wallMs);
        System.out.printf(Locale.US, "Throughput: %.2f msg/sec%n", throughput);
    }
}