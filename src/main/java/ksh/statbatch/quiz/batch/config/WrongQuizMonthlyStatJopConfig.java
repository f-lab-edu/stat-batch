package ksh.statbatch.quiz.batch.config;

import ksh.statbatch.quiz.batch.listener.JobTimeListener;
import ksh.statbatch.quiz.batch.reader.*;
import ksh.statbatch.quiz.batch.writer.MonthlyAggregationUpsertWriter;
import ksh.statbatch.quiz.dto.DailySongAggregation;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class WrongQuizMonthlyStatJopConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    @Bean
    public Job wrongQuizDailyAggregationJob(
        Step dailyAccumulateStep,
        JobTimeListener listener
    ) {
        return new JobBuilder("wrong-quiz-daily-aggregation-job", jobRepository)
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

//    @Bean
    public Step dailyAttemptFullLoadStep(
        DailyAttemptFullLoadReader reader,
        MonthlyAggregationUpsertWriter writer
    ) {
        return new StepBuilder("daily-attempt-full-load-step", jobRepository)
            .<DailySongAggregation, DailySongAggregation>chunk(20000000, transactionManager)
            .reader(reader)
            .writer(writer)
            .build();
    }

//    @Bean
//    @JobScope
    public Step dailyAttemptPagingStep(
        DailyAttemptPagingReader reader,
        MonthlyAggregationUpsertWriter writer,
        @Value("#{jobParameters['chunkSize']}") Integer chunkSize
    ) {
        return new StepBuilder("daily-attempt-paging-step", jobRepository)
            .<DailySongAggregation, DailySongAggregation>chunk(chunkSize, transactionManager)
            .reader(reader)
            .writer(writer)
            .build();
    }

//    @Bean
//    @JobScope
    public Step dailyAttemptCursorStep(
        DailyAttemptCursorReader reader,
        MonthlyAggregationUpsertWriter writer,
        @Value("#{jobParameters['chunkSize']}") Integer chunkSize
    ) {
        return new StepBuilder("daily-attempt-cursor-step", jobRepository)
            .<DailySongAggregation, DailySongAggregation>chunk(chunkSize, transactionManager)
            .reader(reader)
            .writer(writer)
            .build();
    }

//    @Bean
//    @JobScope
    public Step dailyAggregationPagingStep(
        DailyAggregationPagingReader reader,
        MonthlyAggregationUpsertWriter writer,
        @Value("#{jobParameters['chunkSize']}") Integer chunkSize
    ) {
        return new StepBuilder("daily-aggregation-paging-step", jobRepository)
            .<DailySongAggregation, DailySongAggregation>chunk(chunkSize, transactionManager)
            .reader(reader)
            .writer(writer)
            .build();
    }

    @Bean
    @JobScope
    public Step dailyAggregationCursorStep(
        DailyAggregationCursorReader reader,
        MonthlyAggregationUpsertWriter writer,
        @Value("#{jobParameters['chunkSize']}") Integer chunkSize
    ) {
        return new StepBuilder("daily-aggregation-cursor-step", jobRepository)
            .<DailySongAggregation, DailySongAggregation>chunk(chunkSize, transactionManager)
            .reader(reader)
            .writer(writer)
            .build();
    }
}
