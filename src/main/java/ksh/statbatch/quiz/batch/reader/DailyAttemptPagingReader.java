package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailySongAggregation;
import ksh.statbatch.quiz.dto.QuizResult;
import ksh.statbatch.quiz.repository.QuizAttemptHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

@Component
@StepScope
@RequiredArgsConstructor
@Slf4j
public class DailyAttemptPagingReader implements ItemStreamReader<DailySongAggregation> {

    private static final String CONTEXT_LAST_ID = "daily.attempt.paging.reader.lastId";
    private static final String CONTEXT_IS_FINISHED = "daily.attempt.paging.reader.isFinished";

    private final QuizAttemptHistoryRepository quizAttemptHistoryRepository;

    @Value("#{jobParameters['aggregationDay']}")
    private String aggregationDayParam;

    @Value("#{jobParameters['chunkSize']}")
    private int pageSize;

    private LocalDateTime startOfDay;
    private LocalDateTime endOfDay;
    private LocalDate monthStartDate;

    private long lastId = 0L;
    private boolean isFinished = false;

    private List<DailySongAggregation> page;
    private int index = 0;

    @Override
    public void open(ExecutionContext executionContext) {
        ZoneId KST = ZoneId.of("Asia/Seoul");
        LocalDate aggregationDay = LocalDate.parse(aggregationDayParam);
        ZonedDateTime kstStart = aggregationDay.atStartOfDay(KST);
        ZonedDateTime kstEnd   = aggregationDay.plusDays(1).atStartOfDay(KST);

        this.startOfDay = kstStart.toLocalDateTime();
        this.endOfDay   = kstEnd.toLocalDateTime();
        this.monthStartDate = aggregationDay.withDayOfMonth(1);
        log.info(startOfDay.toString());
        log.info(endOfDay.toString());
        log.info(monthStartDate.toString());

        if (executionContext.containsKey(CONTEXT_LAST_ID)) {
            lastId = executionContext.getLong(CONTEXT_LAST_ID);
        }

        if (executionContext.containsKey(CONTEXT_IS_FINISHED)) {
            isFinished = (boolean) executionContext.get(CONTEXT_IS_FINISHED);
        }

        page = null;
        index = 0;
    }
    
    @Override
    public DailySongAggregation read() {
        if (isFinished) return null;

        if (page == null || index >= page.size()) {
            page = fetchNextPage();
            log.info("page size: " + page.size());
            index = 0;

            if (page.isEmpty()) {
                isFinished = true;
                return null;
            }
        }

        return page.get(index++);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        executionContext.put(CONTEXT_LAST_ID, lastId);
        executionContext.put(CONTEXT_IS_FINISHED, isFinished);
    }

    private List<DailySongAggregation> fetchNextPage() {

        List<QuizResult> results = quizAttemptHistoryRepository
            .findQuizResultByCreatedAtBetween(startOfDay, endOfDay, lastId, pageSize);

        if (results.isEmpty()) return Collections.emptyList();


        lastId = results.getLast().getId();

        Map<Long, long[]> aggregationBySong = countResultBySong(results);
        return convertInToDailyAggregation(aggregationBySong);
    }

    private Map<Long, long[]> countResultBySong(List<QuizResult> songAttemptResults) {
        Map<Long, long[]> aggregationMap = new HashMap<>();

        for (QuizResult r : songAttemptResults) {
            long[] stats = aggregationMap.computeIfAbsent(r.getSongId(), k -> new long[2]);

            if (!r.isCorrect()) stats[0]++;
            stats[1]++;
        }

        return aggregationMap;
    }

    private List<DailySongAggregation> convertInToDailyAggregation(Map<Long, long[]> aggregationBySong) {
        List<DailySongAggregation> dailyAggregations = new ArrayList<>();
        for (Map.Entry<Long, long[]> aggregationOfSong : aggregationBySong.entrySet()) {
            long songId = aggregationOfSong.getKey();
            long wrongCount = aggregationOfSong.getValue()[0];
            long totalTries = aggregationOfSong.getValue()[1];

            DailySongAggregation dailyAggregation = new DailySongAggregation(monthStartDate, songId, wrongCount, totalTries);
            dailyAggregations.add(dailyAggregation);
        }
        return dailyAggregations;
    }
}
