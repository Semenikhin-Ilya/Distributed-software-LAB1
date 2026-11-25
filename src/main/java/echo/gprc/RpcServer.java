package echo.gprc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.time.Instant;

/**
 * Задание 1.3: Потоковый RPC сервер (gRPC + Protobuf, двунаправленный поток).
 */
public class RpcServer {
    private static final int PORT = 8080;

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new EchoServiceImpl())
                .build()
                .start();

        System.out.println("gRPC Сервер запущен на порту " + PORT);
        server.awaitTermination();
    }

    static class EchoServiceImpl extends echo.rpc.EchoServiceGrpc.EchoServiceImplBase {

        @Override
        public StreamObserver<echo.rpc.EchoRequest> streamEcho(
                StreamObserver<echo.rpc.EchoResponse> responseObserver) {

            return new StreamObserver<>() {
                @Override
                public void onNext(echo.rpc.EchoRequest request) {
                    long seqNum = request.getSequenceNumber();
                    long timestampMs = Instant.now().toEpochMilli();

                    echo.rpc.EchoResponse response = echo.rpc.EchoResponse.newBuilder()
                            .setMessage("ECHO: " + request.getMessage())
                            .setSequenceNumber(seqNum)
                            .setServerTimestampMs(timestampMs)
                            .build();

                    responseObserver.onNext(response);
                }

                @Override
                public void onError(Throwable t) {
                    System.err.println("Ошибка в потоке клиента: " + t.getMessage());
                }

                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }
}