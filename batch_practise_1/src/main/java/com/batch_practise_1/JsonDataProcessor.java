package com.batch_practise_1;

import org.springframework.batch.item.ItemProcessor;

import java.text.DecimalFormat;

public class JsonDataProcessor implements ItemProcessor<JsonData, CsvData> {
    private final DecimalFormat format = new DecimalFormat("#####.##");

    @Override
    public CsvData process(JsonData item) throws Exception {
        double price = item.systemMarginalPrice();
        String formatedPrice = format.format(price);
        return new CsvData(item.date(), Double.parseDouble(formatedPrice));
    }
}
