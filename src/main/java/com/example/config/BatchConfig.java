package com.example.config;

import com.example.model.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfig {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public BatchConfig(DataSource dataSource, JobRepository jobRepository,
                      PlatformTransactionManager transactionManager) {
        this.dataSource = dataSource;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public JdbcCursorItemReader<Customer> reader() {
        JdbcCursorItemReader<Customer> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("SELECT id, name, email FROM customers");
        reader.setRowMapper((rs, rowNum) -> new Customer(
            rs.getLong("id"),
            rs.getString("name"),
            rs.getString("email")
        ));
        return reader;
    }

    @Bean
    public FlatFileItemWriter<Customer> writer() {
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output/customers.csv"));
        
        DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        
        BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "name", "email"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        
        writer.setLineAggregator(lineAggregator);
        return writer;
    }

    @Bean
    public Step step1() {
        return new StepBuilder("step1")
            .<Customer, Customer>chunk(10)
            .reader(reader())
            .processor(processor())
            .writer(writer())
            .faultTolerant()
            .skipLimit(10)
            .skip(Exception.class)
            .retryLimit(3)
            .retry(Exception.class)
            .build();
    }

    @Bean
    public Job exportCustomerJob(JobParametersBuilder jobParametersBuilder) {
        return new JobBuilder("exportCustomerJob")
            .start(step1())
            .validator(validator())
            .build();
    }

    @Bean
    public JobParametersValidator validator() {
        return new JobParametersValidator() {
            @Override
            public void validate(JobParameters parameters) throws JobParametersInvalidException {
                if (parameters == null || parameters.getString("outputFile") == null) {
                    throw new JobParametersInvalidException("outputFile parameter is required");
                }
            }
        };
    }

    @Bean
    public ItemProcessor<Customer, Customer> processor() {
        return customer -> {
            if (customer.getEmail() == null || customer.getEmail().isEmpty()) {
                throw new IllegalArgumentException("Email cannot be empty");
            }
            return customer;
        };
    }

    @Bean
    public FlatFileItemWriter<Customer> writer(@Value("${outputFile}") String outputFile) {
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource(outputFile));
        
        DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        
        BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "name", "email"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        
        writer.setLineAggregator(lineAggregator);
        return writer;
    }
}