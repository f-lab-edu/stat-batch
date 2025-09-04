package ksh.statbatch.quiz.batch.reader;

import ksh.statbatch.quiz.dto.DailyAggregation;
import ksh.statbatch.quiz.dto.SongAttemptResultWithoutId;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Component
@StepScope
@RequiredArgsConstructor
public class DailyAggregationInMemoryReader implements ItemReader<DailyAggregation>, InitializingBean {

    private final String SQL = """
        select song_id, is_correct
        from quiz_attempt_history
        where created_at >= :startOfDay
            and created_at < :endOfDay
            and is_deleted = false
        """;

    private RowMapper<SongAttemptResultWithoutId> rowMapper = initRowMapper();

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Value("#{jobParameters['aggregationDay']}")
    private String aggregationDayParam;

    private LocalDate monthStartDate;
    private LocalDateTime startOfDay;
    private LocalDateTime endOfDay;

    private Iterator<DailyAggregation> iterator;

    @Override
    public DailyAggregation read() {
        if (iterator == null) {
            List<SongAttemptResultWithoutId> attempts = loadAttempts();
            iterator = accumulateAggregationBySong(attempts);
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

    private List<SongAttemptResultWithoutId> loadAttempts() {
        Map<String, LocalDateTime> params = Map.of(
            "startOfDay", startOfDay,
            "endOfDay", endOfDay
        );
        return jdbcTemplate.query(SQL, params, rowMapper);
    }

    private Iterator<DailyAggregation> accumulateAggregationBySong(List<SongAttemptResultWithoutId> attempts) {
        Map<Long, long[]> agg = new HashMap<>();
        for (SongAttemptResultWithoutId attempt : attempts) {
            long[] pair = agg.computeIfAbsent(attempt.getSongId(), k -> new long[2]);
            if (!attempt.isCorrect()) pair[0]++;
            pair[1]++;
        }

        List<DailyAggregation> dailyAggregations = new ArrayList<>(agg.size());
        for (Map.Entry<Long, long[]> e : agg.entrySet()) {
            dailyAggregations.add(new DailyAggregation(
                monthStartDate,
                e.getKey(),
                e.getValue()[0],
                e.getValue()[1]
            ));
        }
        return dailyAggregations.iterator();
    }

    private RowMapper<SongAttemptResultWithoutId> initRowMapper() {
        return (rs, i) -> new SongAttemptResultWithoutId(
            rs.getLong("song_id"),
            rs.getBoolean("is_correct")
        );
    }

}
