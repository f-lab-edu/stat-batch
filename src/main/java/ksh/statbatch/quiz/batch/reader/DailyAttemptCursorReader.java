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
import java.util.*;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAttemptCursorReader implements ItemReader<DailySongAggregation>, InitializingBean {

    private final QuizAttemptHistoryRepository quizAttemptHistoryRepository;

    @Value("#{jobParameters['aggregationDay']}")
    private String aggregationDayParam;


    private LocalDate monthStartDate;
    private LocalDateTime startOfDay;
    private LocalDateTime endOfDay;

    private Iterator<DailySongAggregation> iterator;
    private boolean isFinished = false;

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

        if (iterator == null) {
            List<DailySongAggregation> page = fetch();

            if (page.isEmpty()) {
                isFinished = true;
                return null;
            }
            iterator = page.iterator();
        }

        if (iterator.hasNext()) {
            return iterator.next();
        }

        isFinished = true;
        return null;
    }

    private List<DailySongAggregation> fetch() {

        List<QuizResultWithoutId> results = quizAttemptHistoryRepository
            .findQuizResultByCreatedAtBetween(startOfDay, endOfDay);

        if (results.isEmpty()) {
            isFinished = true;
            return Collections.emptyList();
        }

        Map<Long, long[]> aggregationBySong = countResultBySong(results);
        return convertInToDailyAggregation(aggregationBySong);
    }

    private Map<Long, long[]> countResultBySong(List<QuizResultWithoutId> songAttemptResults) {
        Map<Long, long[]> aggregationMap = new HashMap<>();

        for (QuizResultWithoutId r : songAttemptResults) {
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
