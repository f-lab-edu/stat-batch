package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailySongAggregation;
import ksh.statbatch.quiz.dto.QuizResult;
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
public class DailyAttemptPagingReader implements ItemReader<DailySongAggregation>, InitializingBean {

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
