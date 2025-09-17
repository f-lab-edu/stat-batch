package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailySongAggregation;
import ksh.statbatch.quiz.repository.QuizAttemptHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAggregationPagingReader implements ItemStreamReader<DailySongAggregation> {

    private static final String CONTEXT_LAST_SONG_ID = "daily.aggregation.paging.reader.lastSongId";
    private static final String CONTEXT_IS_FINISHED = "daily.aggregation.paging.reader.isFinished";

    private final QuizAttemptHistoryRepository queryAttemptHistoryRepository;

    @Value("#{jobParameters['aggregationDay']}")
    private String aggregationDayParam;

    @Value("#{jobParameters['chunkSize']}")
    private int pageSize;

    private LocalDateTime startOfDay;
    private LocalDateTime endOfDay;
    private LocalDate monthStartDate;

    private long lastSongId = 0L;
    private boolean isFinished = false;

    private List<DailySongAggregation> page;
    private int index = 0;

    @Override
    public void open(ExecutionContext executionContext) {
        LocalDate aggregationDay = LocalDate.parse(aggregationDayParam);
        startOfDay = aggregationDay.atStartOfDay();
        endOfDay = aggregationDay.plusDays(1).atStartOfDay();
        monthStartDate = aggregationDay.withDayOfMonth(1);

        if (executionContext.containsKey(CONTEXT_LAST_SONG_ID)) {
            lastSongId = executionContext.getLong(CONTEXT_LAST_SONG_ID);
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

        if (index >= page.size()) {
            page = fetchNextPage();
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
        executionContext.put(CONTEXT_LAST_SONG_ID, lastSongId);
        executionContext.put(CONTEXT_IS_FINISHED, isFinished);
    }

    private List<DailySongAggregation> fetchNextPage() {
        List<DailySongAggregation> rows = queryAttemptHistoryRepository
            .aggregateDailyAttemptsBySong(monthStartDate, startOfDay, endOfDay, lastSongId, pageSize);

        if (rows.isEmpty()) return Collections.emptyList();


        lastSongId = rows.getLast().getSongId();

        return rows;
    }
}
