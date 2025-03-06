package com.example.batch;

import com.example.config.BatchConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.*;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {BatchConfig.class})
@TestPropertySource(properties = {
    "outputFile=target/test-output/customers.csv"
})
public class BatchJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void testJobExecution() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
            .addString("outputFile", "target/test-output/customers.csv")
            .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);
        
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }
}