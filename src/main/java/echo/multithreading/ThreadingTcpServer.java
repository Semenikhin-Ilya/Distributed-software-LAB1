package echo.multithreading;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Задание 1.1: Блокирующий многопоточный TCP-сервер.
 * Каждое соединение обрабатывается в отдельном потоке из пула.
 */
public class ThreadingTcpServer {

    private static final int PORT = 8080;
    // Пул с запасом: требование >= 100 одновременных клиентов, берём 200.
    private static final ExecutorService poolThread = Executors.newFixedThreadPool(200);

    public static void main(String[] args) {
        try (ServerSocket socket = new ServerSocket(PORT)) {
            System.out.println("Сервер запущен на порту " + PORT);

            while (true) {
                Socket clientSocket = socket.accept();
                // Отключаем алгоритм Нейгла для честного замера RTT малых сообщений.
                clientSocket.setTcpNoDelay(true);
                poolThread.submit(() -> clientHandler(clientSocket));
            }

        } catch (IOException e) {
            System.err.println("Ошибка при работе сервера: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void clientHandler(Socket socket) {
        try (socket;
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             BufferedWriter out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))) {

            String clientMessage;
            while ((clientMessage = in.readLine()) != null) {
                Instant now = Instant.now();
                long seconds = now.getEpochSecond();
                int micros = now.getNano() / 1_000;

                String serverMessage = String.format(Locale.US, "ECHO: %s [t=%d.%06d]\n",
                        clientMessage, seconds, micros);

                out.write(serverMessage);
                out.flush();
            }

        } catch (IOException e) {
            System.err.println("Ошибка при обработке клиента: " + e.getMessage());
        }
    }
}