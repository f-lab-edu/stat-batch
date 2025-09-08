package ksh.statbatch.quiz.batch.writer;

import ksh.statbatch.quiz.dto.DailySongAggregation;
import ksh.statbatch.quiz.repository.WrongQuizMonthlyStatRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@StepScope
@RequiredArgsConstructor
public class MonthlyAggregationUpsertWriter implements ItemWriter<DailySongAggregation> {

    private final WrongQuizMonthlyStatRepository wrongQuizMonthlyStatJdbcRepository;

    @Override
    public void write(Chunk<? extends DailySongAggregation> chunk) throws Exception {
        List<DailySongAggregation> items = (List<DailySongAggregation>) chunk.getItems();
        wrongQuizMonthlyStatJdbcRepository.upsertAll(items);
    }
}
