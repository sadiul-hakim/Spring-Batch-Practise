package com.batch_practise_1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.core.io.Resource;

import java.util.Iterator;

public class CustomJsonReader implements ItemReader<JsonData>, ResourceAwareItemReaderItemStream<JsonData> {

    private Iterator<JsonNode> itemIterator;
    private final ObjectMapper mapper;
    private Resource resource;

    public CustomJsonReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public JsonData read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if(itemIterator == null){
            loadItems();
        }

        if(itemIterator != null && itemIterator.hasNext()){
            return mapper.treeToValue(itemIterator.next(), JsonData.class);
        }

        return null;
    }

    private void loadItems() throws Exception{
        JsonNode rootNode = mapper.readTree(resource.getInputStream());
        ArrayNode array = (ArrayNode) rootNode.get("items");
        itemIterator = array.iterator();
    }

    @Override
    public void setResource(Resource resource) {
        this.resource=resource;
    }
}
