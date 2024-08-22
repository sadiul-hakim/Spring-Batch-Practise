package com.batch_practise_1;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class BatchConfig {
    @Value("classpath:static/Data.json")
    public Resource jsonFilePath;

    @Value("file:Prices.csv")
    public WritableResource pricesFile;

    @Bean
    public Job job(JobRepository jobRepository,
                   @Qualifier("processJsonData") Step step) {
        return new JobBuilder("json-to-csv-job", jobRepository)
                .start(step)
                .build();
    }

    @Bean
    @Qualifier
    public Step processJsonData(JobRepository jobRepository, PlatformTransactionManager transactionManager, CustomJsonReader customJsonReader) {
        return new StepBuilder("processJsonData-step", jobRepository)
                .<JsonData, CsvData>chunk(1, transactionManager)
                .reader(customJsonReader)
                .processor(new JsonDataProcessor())
                .writer(new FlatFileItemWriterBuilder<CsvData>()
                        .name("csv-writer")
                        .resource(pricesFile)
                        .delimited()
                        .delimiter(",")
                        .names(new String[]{"date", "price"})
                        .build()
                )
                .build();
    }

    @Bean
    public CustomJsonReader jsonReader(ObjectMapper objectMapper) {
        var jsonReader = new CustomJsonReader(objectMapper);
        jsonReader.setResource(jsonFilePath);
        return jsonReader;
    }
}
