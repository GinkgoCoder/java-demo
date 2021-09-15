package com.ginkl.demo.elasticsearch.repo;

import com.ginkl.demo.elasticsearch.entity.Car;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface CarRepository extends ElasticsearchRepository<Car, String> {
    @Query("{\"bool\":{\"must\":[{\"match\":{\"make\":\"?0\"}},{\"range\":{\"mileage\":{\"gte\":\"?1\",\"lte\":\"?2\"}}}]}}")
    Page<Car> findCarWithCustomQuery(String brand, Long start, Long end, Pageable pageable);
}
