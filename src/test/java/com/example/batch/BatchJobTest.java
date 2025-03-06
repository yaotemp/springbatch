package com.example.batch;

import com.example.config.BatchConfig;
import com.example.model.Customer;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBatchTest
@SpringBootTest(classes = {BatchConfig.class})
@TestPropertySource(properties = {
    "outputFile=target/test-output/customers.csv"
})
class BatchJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    void testJobExecution() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
            .addString("outputFile", "target/test-output/customers.csv")
            .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }
}