package com.ginkl.demo.elasticsearch.repo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ginkl.demo.elasticsearch.entity.Car;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@SpringBootTest
class CarRepositoryTest {
    @Autowired
    CarRepository carRepository;

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Test
    public void getAllCars() {
        List<Car> carList =
                StreamSupport.stream(carRepository.findAll().spliterator(), false).collect(Collectors.toList());
        Assertions.assertTrue(carList.size() > 0);
    }

    @Test
    public void saveGetAndDeleteCar() {
        Car car = Car.builder()
                .make("BMW")
                .gear("automatic")
                .model("750i")
                .offType("new")
                .mileage(0L)
                .price(80000L)
                .fuel("Gasoline")
                .id("cars-leisun")
                .hp(500L)
                .year("2001")
                .build();
        carRepository.save(car);
        Optional<Car> actualCar = carRepository.findById("cars-leisun");
        Assertions.assertTrue(actualCar.isPresent());
        Assertions.assertEquals(car, actualCar.get());
        carRepository.deleteById("cars-leisun");
        actualCar = carRepository.findById("cars-leisun");
        Assertions.assertFalse(actualCar.isPresent());
    }

    @Test
    public void customizeQueryWithSpringData() {
        Pageable p = Pageable.ofSize(3).withPage(0);
        Page<Car> bmwCars =
                carRepository.findCarWithCustomQuery("BMW", 100000L, 150000L, p);
        Assertions.assertFalse(bmwCars.isEmpty());

        bmwCars.get().forEach(car -> {
            Assertions.assertEquals("BMW", car.getMake());
            Assertions.assertTrue(car.getMileage() <= 150000L);
            Assertions.assertTrue(car.getMileage() >= 100000L);
        });
    }

    @Test
    public void customizeQueryWithoutSpringData() throws IOException {
        ObjectMapper om = new ObjectMapper();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("cars");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(3);
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("make", "BMW");
        boolQueryBuilder.must(matchQueryBuilder);
        boolQueryBuilder.filter(
                new RangeQueryBuilder("mileage").gte(100000L).lte(150000L)
        );
        searchSourceBuilder.query(boolQueryBuilder);

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Assertions.assertEquals(3, searchResponse.getHits().getHits().length);

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Car car = om.readValue(hit.getSourceAsString(), Car.class);
            Assertions.assertEquals("BMW", car.getMake());
            Assertions.assertTrue(car.getMileage() <= 150000L);
            Assertions.assertTrue(car.getMileage() >= 100000L);
        }
    }
}