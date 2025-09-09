package ksh.statbatch.quiz.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class QuizResult {

    private long id;
    private long songId;
    private boolean isCorrect;
}
