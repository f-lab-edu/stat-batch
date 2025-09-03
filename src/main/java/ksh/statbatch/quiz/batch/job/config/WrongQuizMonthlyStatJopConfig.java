package ksh.statbatch.quiz.batch.job.config;

import ksh.statbatch.quiz.batch.job.listener.JobTimeListener;
import ksh.statbatch.quiz.batch.reader.DailyAggregationInMemoryReader;
import ksh.statbatch.quiz.batch.writer.MonthlyAggregationUpsertWriter;
import ksh.statbatch.quiz.dto.DailyAggregation;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class WrongQuizMonthlyStatJopConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job wrongQuizDailyAccumulateJob(
        Step dailyAccumulateStep,
        JobTimeListener listener
    ) {
        return new JobBuilder("wrong-quiz-daily-accumulate-job", jobRepository)
            .incrementer(new RunIdIncrementer())
            .validator(params -> {
                if (!params.getParameters().containsKey("aggregationDay")) {
                    throw new JobParametersInvalidException("파라미터가 누락되었습니다 : aggregationDay");
                }
            })
            .listener(listener)
            .start(dailyAccumulateStep)
            .build();
    }

    @Bean
    public Step dailyAccumulateStep(
        DailyAggregationInMemoryReader reader,
        MonthlyAggregationUpsertWriter writer
    ) {
        return new StepBuilder("daily-accumulate-step", jobRepository)
            .<DailyAggregation, DailyAggregation>chunk(10000, transactionManager)
            .reader(reader)
            .writer(writer)
            .build();
    }
}
