package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailyAggregation;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAggregationInMemoryReader implements ItemReader<DailyAggregation>, InitializingBean {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Value("#{jobParameters['aggregationDay']}") private String aggregationDayParam;

    private LocalDate aggregationDay;
    private LocalDate monthStartDate;
    private Iterator<DailyAggregation> iterator;

    @Override
    public DailyAggregation read() {
        if (iterator == null) {
            List<Map<String, Object>> resultSet = loadAttempts();
            iterator = aggregateAttempts(resultSet);
        }

        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void afterPropertiesSet() {
        aggregationDay = LocalDate.parse(aggregationDayParam);
        monthStartDate = aggregationDay.withDayOfMonth(1);
    }

    private List<Map<String, Object>> loadAttempts() {
        LocalDateTime startOfDay = aggregationDay.atStartOfDay();
        LocalDateTime endOfDay = aggregationDay.plusDays(1).atStartOfDay();

        String sql = """
            select song_id, is_correct
            from quiz_attempt_history
            where created_at >= :startOfDay
                and created_at < :endOfDay
                and is_deleted = false
            """;

        Map<String, LocalDateTime> params = Map.of(
            "startOfDay", startOfDay,
            "endOfDay", endOfDay
        );
        return jdbcTemplate.queryForList(sql, params);
    }

    private Iterator<DailyAggregation> aggregateAttempts(
        List<Map<String, Object>> resultSet
    ){
        Map<Long, long[]> aggregationBySong = accumulateAggregationBySong(resultSet);

        return convertToDailyAggregation(aggregationBySong).iterator();
    }

    private static Map<Long, long[]> accumulateAggregationBySong(List<Map<String, Object>> resultSet) {
        Map<Long, long[]> aggregationBySong = new HashMap<>();
        for (Map<String, Object> row : resultSet) {
            long songId = ((Number) row.get("song_id")).longValue();
            boolean isCorrect = (Boolean) row.get("is_correct");

            long[] pair = aggregationBySong.computeIfAbsent(songId, k -> new long[2]);
            if(!isCorrect) pair[0]++;
            pair[1]++;
        }
        return aggregationBySong;
    }

    private List<DailyAggregation> convertToDailyAggregation(Map<Long, long[]> aggregationBySong) {
        List<DailyAggregation> dailyAggregations = new ArrayList<>();
        for (Map.Entry<Long, long[]> aggregationOfSong : aggregationBySong.entrySet()) {
            long songId = aggregationOfSong.getKey();
            long wrongCount = aggregationOfSong.getValue()[0];
            long totalTries = aggregationOfSong.getValue()[1];

            DailyAggregation dailyAggregation = new DailyAggregation(monthStartDate, songId, wrongCount, totalTries);
            dailyAggregations.add(dailyAggregation);
        }
        return dailyAggregations;
    }
}
