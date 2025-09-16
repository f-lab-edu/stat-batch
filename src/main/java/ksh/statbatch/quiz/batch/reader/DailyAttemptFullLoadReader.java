package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailySongAggregation;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAttemptFullLoadReader implements ItemReader<DailySongAggregation>, InitializingBean {

    public static final int WRONG_COUNT_INDEX = 0;
    public static final int TOTAL_TRIES_INDEX = 1;

    private final QuizAttemptHistoryRepository  quizAttemptHistoryRepository;

    @Value("#{jobParameters['aggregationDay']}")
    private String aggregationDayParam;

    private LocalDate monthStartDate;
    private LocalDateTime startOfDay;
    private LocalDateTime endOfDay;

    private List<DailySongAggregation> list;
    private int index = 0;


    @Override
    public DailySongAggregation read() {
        if (list == null) {
            List<QuizResultWithoutId> results = loadResults();
            list = accumulateAggregationBySong(results);
        }

        if(index >= list.size()) return null;

        return list.get(index++);
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

    private List<DailySongAggregation> accumulateAggregationBySong(List<QuizResultWithoutId> attempts) {
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

    private List<DailySongAggregation> convertIntoDailyAggregation(Map<Long, long[]> aggregationBySong) {
        List<DailySongAggregation> dailyAggregations = new ArrayList<>(aggregationBySong.size());
        for (Map.Entry<Long, long[]> e : aggregationBySong.entrySet()) {
            dailyAggregations.add(new DailySongAggregation(
                monthStartDate,
                e.getKey(),
                e.getValue()[WRONG_COUNT_INDEX],
                e.getValue()[TOTAL_TRIES_INDEX]
            ));
        }
        return dailyAggregations;
    }
}
