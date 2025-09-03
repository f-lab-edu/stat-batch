package ksh.statbatch.quiz.batch.writer;

import jakarta.annotation.PostConstruct;
import ksh.statbatch.quiz.dto.DailyAggregation;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
@StepScope
@RequiredArgsConstructor
public class MonthlyAggregationUpsertWriter extends JdbcBatchItemWriter<DailyAggregation> {

    private final DataSource dataSource;

    @PostConstruct
    public void init() {
        setDataSource(dataSource);
        setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());

        final String sql = """
            INSERT INTO wrong_quiz_monthly_stat
                (base_date, song_id, wrong_count, total_tries, wrong_rate)
            VALUES
                (:baseDate, :songId, :wrongInc, :triesInc, ROUND(:wrongInc * 1.0 / :triesInc, 4))
            ON DUPLICATE KEY UPDATE
                wrong_count = wrong_quiz_monthly_stat.wrong_count + VALUES(wrong_count),
                total_tries = wrong_quiz_monthly_stat.total_tries + VALUES(total_tries),
                wrong_rate  = ROUND(wrong_count * 1.0 / total_tries, 4);
            """;
        setSql(sql);

        afterPropertiesSet();
    }
}
