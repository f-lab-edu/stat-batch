package ksh.statbatch.quiz.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.LocalDate;

@Getter
@AllArgsConstructor
public class DailyAggregation {

    private final LocalDate baseDate;
    private final long songId;
    private final long wrongInc;
    private final long triesInc;
}
