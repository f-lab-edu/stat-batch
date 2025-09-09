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
import java.util.Iterator;
import java.util.List;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAggregationCursorReader implements ItemReader<DailySongAggregation>, InitializingBean {

    private final QuizAttemptHistoryRepository quizAttemptHistoryRepository;

    @Value("#{jobParameters['aggregationDay']}")
    private String aggregationDayParam;

    private LocalDateTime startOfDay;
    private LocalDateTime endOfDay;
    private LocalDate monthStartDate;

    private Iterator<DailySongAggregation> iterator = Collections.emptyIterator();
    private boolean loaded = false;

    @Override
    public void afterPropertiesSet() {
        LocalDate aggregationDay = LocalDate.parse(aggregationDayParam);
        startOfDay = aggregationDay.atStartOfDay();
        endOfDay = aggregationDay.plusDays(1).atStartOfDay();
        monthStartDate = aggregationDay.withDayOfMonth(1);
    }

    @Override
    public DailySongAggregation read() {
        if (!loaded) {
            List<DailySongAggregation> list = quizAttemptHistoryRepository
                .aggregateDailyAttemptsBySong(monthStartDate, startOfDay, endOfDay);

            loaded = true;
            if (list.isEmpty()) return null;

            iterator = list.iterator();
        }
        return iterator.hasNext() ? iterator.next() : null;
    }
}
