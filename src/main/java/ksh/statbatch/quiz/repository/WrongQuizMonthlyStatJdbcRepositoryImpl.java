package ksh.statbatch.quiz.repository;

import ksh.statbatch.quiz.dto.DailySongAggregation;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;

import java.util.List;

@RequiredArgsConstructor
public class WrongQuizMonthlyStatJdbcRepositoryImpl implements WrongQuizMonthlyStatJdbcRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    public void upsertAll(List<DailySongAggregation> dailySongAggregations) {
        String sql = """
            INSERT INTO wrong_quiz_monthly_stat
                (base_date, song_id, wrong_count, total_tries, wrong_rate)
            VALUES
                (:baseDate, :songId, :wrongInc, :triesInc, ROUND(:wrongInc * 1.0 / :triesInc, 4))
            ON DUPLICATE KEY UPDATE
                wrong_count = wrong_quiz_monthly_stat.wrong_count + VALUES(wrong_count),
                total_tries = wrong_quiz_monthly_stat.total_tries + VALUES(total_tries),
                wrong_rate  = ROUND(wrong_count * 1.0 / total_tries, 4)
            """;

        SqlParameterSource[] args = SqlParameterSourceUtils.createBatch(dailySongAggregations);
        jdbcTemplate.batchUpdate(sql, args);
    }
}
