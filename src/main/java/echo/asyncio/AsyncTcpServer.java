package echo.asyncio;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;

/**
 * Задание 1.2: Асинхронный сервер на основе событий.
 * Один поток + Selector + неблокирующий I/O.
 *
 * Отличие от первой версии: на каждое соединение хранится собственный буфер
 * чтения, накопитель незавершённых строк и очередь записи. Это корректно
 * обрабатывает частичные чтения (сообщение пришло кусками) и частичные
 * записи (ответ не поместился в сокет за один write).
 */
public class AsyncTcpServer {

    private static final int PORT = 8080;
    private static final int READ_BUFFER_SIZE = 4096;

    /** Состояние одного клиентского соединения. */
    private static final class ClientAttachment {
        final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
        final ByteArrayOutputStream pending = new ByteArrayOutputStream();
        final Deque<ByteBuffer> writeQueue = new ArrayDeque<>();
    }

    public static void main(String[] args) {
        try (Selector selector = Selector.open();
             ServerSocketChannel serverChannel = ServerSocketChannel.open()) {

            serverChannel.bind(new InetSocketAddress(PORT), 1024);
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            System.out.println("Асинхронный сервер запущен на порту " + PORT);

            while (true) {
                selector.select();

                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();

                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();

                    if (!key.isValid()) continue;

                    try {
                        if (key.isAcceptable()) {
                            handleAccept(key);
                        }
                        if (key.isValid() && key.isReadable()) {
                            handleRead(key);
                        }
                        if (key.isValid() && key.isWritable()) {
                            handleWrite(key);
                        }
                    } catch (IOException e) {
                        System.err.println("Ошибка I/O, закрываем соединение: " + e.getMessage());
                        closeAndCancel(key);
                    }
                }
            }

        } catch (IOException e) {
            System.err.println("Ошибка при работе сервера: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel server = (ServerSocketChannel) key.channel();
        SocketChannel client = server.accept();
        if (client != null) {
            client.configureBlocking(false);
            client.setOption(StandardSocketOptions.TCP_NODELAY, true);
            client.register(key.selector(), SelectionKey.OP_READ, new ClientAttachment());
        }
    }

    private static void handleRead(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientAttachment att = (ClientAttachment) key.attachment();
        ByteBuffer buf = att.readBuffer;

        buf.clear();
        int bytesRead = client.read(buf);

        if (bytesRead == -1) {
            closeAndCancel(key);
            return;
        }

        if (bytesRead > 0) {
            buf.flip();
            while (buf.hasRemaining()) {
                byte b = buf.get();
                if (b == '\n') {
                    String clientMessage = att.pending.toString(StandardCharsets.UTF_8);
                    att.pending.reset();

                    String response = formatResponse(clientMessage);
                    att.writeQueue.addLast(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
                } else if (b != '\r') {
                    att.pending.write(b);
                }
            }
        }

        flushWrites(key);
    }

    private static void handleWrite(SelectionKey key) throws IOException {
        flushWrites(key);
    }

    private static void flushWrites(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        ClientAttachment att = (ClientAttachment) key.attachment();

        while (!att.writeQueue.isEmpty()) {
            ByteBuffer out = att.writeQueue.peekFirst();
            client.write(out);

            if (out.hasRemaining()) {
                // Частичная запись — ждём готовности OP_WRITE.
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                return;
            }
            att.writeQueue.pollFirst();
        }

        // Очередь пуста — OP_WRITE больше не нужен.
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    private static String formatResponse(String msg) {
        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        int micros = now.getNano() / 1_000;
        return String.format(Locale.US, "ECHO: %s [t=%d.%06d]\n", msg, seconds, micros);
    }

    private static void closeAndCancel(SelectionKey key) {
        try {
            key.channel().close();
        } catch (IOException ignored) {
        }
        key.cancel();
    }
}