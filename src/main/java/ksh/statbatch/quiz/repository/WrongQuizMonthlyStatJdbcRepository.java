package ksh.statbatch.quiz.repository;

import ksh.statbatch.quiz.dto.DailySongAggregation;

import java.util.List;

public interface WrongQuizMonthlyStatJdbcRepository {

    void upsertAll(List<DailySongAggregation> dailySongAggregations);
}
