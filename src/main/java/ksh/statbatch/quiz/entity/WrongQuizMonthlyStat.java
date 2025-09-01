package ksh.statbatch.quiz.entity;

import jakarta.persistence.*;
import ksh.statbatch.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Entity
@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WrongQuizMonthlyStat extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long songId;

    @Column(nullable = false)
    private LocalDate baseDate;

    @Column(nullable = false)
    private Long wrongCount;

    @Column(nullable = false)
    private Long totalTries;

    @Column(nullable = false)
    private Double wrongRate;
}
