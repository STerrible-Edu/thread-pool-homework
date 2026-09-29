import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final int CORE_POOL_SIZE = 4;
    private static final int MAX_POOL_SIZE = 8;
    private static final int QUEUE_SIZE = 20;

    public static void main(String[] args) throws Exception {
        String fileName = args.length > 0 ? args[0] : "urls.txt";
        List<String> urls = readUrls(fileName);

        if (urls.isEmpty()) {
            System.out.println("Список URL пуст");
            return;
        }

        AtomicInteger threadNumber = new AtomicInteger(1);
        ThreadFactory threadFactory = task -> {
            Thread thread = new Thread(task);
            thread.setName("http-worker-" + threadNumber.getAndIncrement());
            return thread;
        };

        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(QUEUE_SIZE),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(7))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor();
        monitor.scheduleAtFixedRate(() -> printPoolState(pool), 0, 500, TimeUnit.MILLISECONDS);

        List<Future<RequestResult>> futures = new ArrayList<>();
        for (String url : urls) {
            futures.add(pool.submit(createRequestTask(client, url)));
        }

        List<RequestResult> results = new ArrayList<>();
        for (Future<RequestResult> future : futures) {
            results.add(future.get());
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.MINUTES);
        monitor.shutdownNow();

        printResults(results);
    }

    private static List<String> readUrls(String fileName) throws IOException {
        return Files.readAllLines(Path.of(fileName)).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .toList();
    }

    private static Callable<RequestResult> createRequestTask(HttpClient client, String url) {
        return () -> {
            long started = System.nanoTime();
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(12))
                        .header("User-Agent", "ThreadPoolHomework/1.0")
                        .GET()
                        .build();

                HttpResponse<Void> response = client.send(
                        request,
                        HttpResponse.BodyHandlers.discarding()
                );

                return new RequestResult(
                        url,
                        response.statusCode(),
                        elapsedMillis(started),
                        Thread.currentThread().getName(),
                        null
                );
            } catch (Exception error) {
                return new RequestResult(
                        url,
                        -1,
                        elapsedMillis(started),
                        Thread.currentThread().getName(),
                        error.getClass().getSimpleName()
                );
            }
        };
    }

    private static long elapsedMillis(long started) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    private static void printPoolState(ThreadPoolExecutor pool) {
        System.out.printf(
                "Пул: активных=%d, в очереди=%d, завершено=%d%n",
                pool.getActiveCount(),
                pool.getQueue().size(),
                pool.getCompletedTaskCount()
        );
    }

    private static void printResults(List<RequestResult> results) {
        System.out.println("\nРезультаты запросов:");
        System.out.printf("%-4s %-7s %-9s %-16s %s%n", "№", "Статус", "Время", "Поток", "URL");

        int successful = 0;
        long totalTime = 0;

        for (int i = 0; i < results.size(); i++) {
            RequestResult result = results.get(i);
            if (result.statusCode() >= 200 && result.statusCode() < 400) {
                successful++;
            }
            totalTime += result.responseTimeMs();

            String status = result.error() == null
                    ? String.valueOf(result.statusCode())
                    : result.error();

            System.out.printf(
                    "%-4d %-7s %-9s %-16s %s%n",
                    i + 1,
                    status,
                    result.responseTimeMs() + " мс",
                    result.threadName(),
                    result.url()
            );
        }

        double averageTime = (double) totalTime / results.size();
        System.out.println("\nСводка:");
        System.out.println("Всего запросов: " + results.size());
        System.out.println("Успешных запросов: " + successful);
        System.out.println("Ошибок: " + (results.size() - successful));
        System.out.printf("Среднее время ответа: %.1f мс%n", averageTime);
    }

    private record RequestResult(
            String url,
            int statusCode,
            long responseTimeMs,
            String threadName,
            String error
    ) {
    }
}


