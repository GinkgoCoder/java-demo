package com.ginkl.demo.elasticsearch.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

@Document(indexName = "cars")
@Builder
@AllArgsConstructor
@Data
@NoArgsConstructor
public class Car {
    @Id
    private String id;

    private Long mileage;

    //field is used for converttion between the java object field name and elasticsearch document field name
    @Field("make")
    private String make;

    private String model;

    private String fuel;

    private String gear;

    private String offType;

    private Long price;

    private Long hp;

    private String year;


//    Nested Type is used for to group parts of the fields into a class
//    @Field(type = FieldType.Nested, includeInParent = true)
//    private List<Author> authors;


}
