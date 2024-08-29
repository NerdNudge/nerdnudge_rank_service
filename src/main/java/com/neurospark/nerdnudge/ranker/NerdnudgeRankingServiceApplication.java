package com.neurospark.nerdnudge.ranker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NerdnudgeRankingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(NerdnudgeRankingServiceApplication.class, args);
	}

}
