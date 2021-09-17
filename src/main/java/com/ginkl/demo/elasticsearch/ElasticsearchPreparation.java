package com.ginkl.demo.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ginkl.demo.elasticsearch.entity.Car;
import com.opencsv.CSVReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * If there is an ES cluster to use, please change the configuration in ElasticsearchConfig.java.
 * If there is no ES cluster, please use the following command to run a docker container for ES.
 * docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.14.1
 */
@Slf4j
public class ElasticsearchPreparation {
    RestHighLevelClient restHighLevelClient;
    private final String INDEX = "cars";
    private final String DATASET_PATH = "autoscout24-germany-dataset.csv";
    private final int DATA_NUM = 1000;

    public ElasticsearchPreparation() {
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200))
        );
    }

    public static void main(String[] args) throws Exception {
        new ElasticsearchPreparation().run();
    }

    public void run() throws Exception {
        if (indexExists()) {
            log.warn("Index exists");
        } else {
            log.info("Index not exists, create the index");
            createIndex();
        }
        log.info("import the data into the cluster...");
        writeData();
        restHighLevelClient.close();
        log.info("Finish import the data...");
    }

    private void createIndex() throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX);
        restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    private boolean indexExists() {
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX);
        try {
            return restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException("Cannot connect to elasticsearch", e);
        }
    }

    private void writeData() throws IOException {
        List<Car> cars = prepareTheData();
        ObjectMapper om = new ObjectMapper();

        for (int i = 0; i < cars.size(); i++) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id("cars-" + i);
            indexRequest.index(INDEX);
            indexRequest.source(om.writeValueAsString(cars.get(i)), XContentType.JSON);
            restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        }
    }

    private List<Car> prepareTheData() {
        return readFirstNumOfCarsInCSV(DATA_NUM, DATASET_PATH);
    }

    public static String getFilePathFromResource(String filePath) throws URISyntaxException {
        URL resource = ElasticsearchPreparation.class.getClassLoader().getResource(filePath);
        assert resource != null;
        return resource.toURI().getPath();
    }

    public static List<Car> readFirstNumOfCarsInCSV(int num, String csvPath) {
        List<Car> list = new ArrayList<>();
        try (CSVReader csvReader = new CSVReader(new FileReader(getFilePathFromResource(csvPath)))) {
            String[] values;
            csvReader.readNext();
            while ((values = csvReader.readNext()) != null && num > 0) {
                num--;
                Car car = Car.builder()
                        .mileage(Long.parseLong(values[0]))
                        .make(values[1])
                        .model(values[2])
                        .fuel(values[3])
                        .gear(values[4])
                        .offType(values[5])
                        .price(Long.parseLong(values[6]))
                        .hp(!StringUtils.isBlank(values[7]) ? Long.parseLong(values[7]) : 0)
                        .year(values[8])
                        .build();
                list.add(car);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

}
