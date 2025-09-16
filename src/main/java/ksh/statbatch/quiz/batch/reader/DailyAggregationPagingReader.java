package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailySongAggregation;
import ksh.statbatch.quiz.repository.QuizAttemptHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAggregationPagingReader implements ItemReader<DailySongAggregation>, InitializingBean {

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
    public void afterPropertiesSet() {
        LocalDate aggregationDay = LocalDate.parse(aggregationDayParam);
        startOfDay = aggregationDay.atStartOfDay();
        endOfDay = aggregationDay.plusDays(1).atStartOfDay();
        monthStartDate = aggregationDay.withDayOfMonth(1);
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

    private List<DailySongAggregation> fetchNextPage() {
        List<DailySongAggregation> rows = queryAttemptHistoryRepository
            .aggregateDailyAttemptsBySong(monthStartDate, startOfDay, endOfDay, lastSongId, pageSize);

        if (rows.isEmpty()) return Collections.emptyList();


        lastSongId = rows.getLast().getSongId();

        return rows;
    }
}
