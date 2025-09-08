package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailyAggregation;
import ksh.statbatch.quiz.dto.QuizResultWithoutId;
import ksh.statbatch.quiz.repository.QuizAttemptHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAttemptInMemoryReader implements ItemReader<DailyAggregation>, InitializingBean {

    public static final int WRONG_COUNT_INDEX = 0;
    public static final int TOTAL_TRIES_INDEX = 1;

    private final QuizAttemptHistoryRepository  quizAttemptHistoryRepository;

    private Iterator<DailyAggregation> iterator;

    @Value("#{jobParameters['aggregationDay']}")
    private String aggregationDayParam;

    private LocalDate monthStartDate;
    private LocalDateTime startOfDay;
    private LocalDateTime endOfDay;


    @Override
    public DailyAggregation read() {
        if (iterator == null) {
            List<QuizResultWithoutId> results = loadResults();
            List<DailyAggregation> dailyAggregations = accumulateAggregationBySong(results);
            iterator = dailyAggregations.iterator();
        }

        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void afterPropertiesSet() {
        LocalDate aggregationDay = LocalDate.parse(aggregationDayParam);
        monthStartDate = aggregationDay.withDayOfMonth(1);
        startOfDay = aggregationDay.atStartOfDay();
        endOfDay = aggregationDay.plusDays(1).atStartOfDay();
    }

    private List<QuizResultWithoutId> loadResults() {
        return quizAttemptHistoryRepository
            .findQuizResultByCreatedAtBetween(startOfDay, endOfDay);
    }

    private List<DailyAggregation> accumulateAggregationBySong(List<QuizResultWithoutId> attempts) {
        Map<Long, long[]> aggregationBySong = countResultsBySong(attempts);
        return convertIntoDailyAggregation(aggregationBySong);
    }

    private static Map<Long, long[]> countResultsBySong(List<QuizResultWithoutId> attempts) {
        Map<Long, long[]> aggregationBySong = new HashMap<>();
        for (QuizResultWithoutId attempt : attempts) {
            long[] pair = aggregationBySong.computeIfAbsent(attempt.getSongId(), k -> new long[2]);
            if (!attempt.isCorrect()) pair[WRONG_COUNT_INDEX]++;
            pair[TOTAL_TRIES_INDEX]++;
        }
        return aggregationBySong;
    }

    private List<DailyAggregation> convertIntoDailyAggregation(Map<Long, long[]> aggregationBySong) {
        List<DailyAggregation> dailyAggregations = new ArrayList<>(aggregationBySong.size());
        for (Map.Entry<Long, long[]> e : aggregationBySong.entrySet()) {
            dailyAggregations.add(new DailyAggregation(
                monthStartDate,
                e.getKey(),
                e.getValue()[WRONG_COUNT_INDEX],
                e.getValue()[TOTAL_TRIES_INDEX]
            ));
        }
        return dailyAggregations;
    }
}
