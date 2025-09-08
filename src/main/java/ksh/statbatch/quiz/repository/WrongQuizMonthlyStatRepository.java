package ksh.statbatch.quiz.repository;

import ksh.statbatch.quiz.entity.WrongQuizMonthlyStat;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.List;


public interface WrongQuizMonthlyStatRepository extends JpaRepository<WrongQuizMonthlyStat, Long>, WrongQuizMonthlyStatJdbcRepository {

    List<WrongQuizMonthlyStat> findByBaseDate(LocalDate baseDate);
}
