package com.spring.kafka.domain.model;

import lombok.*;

@Getter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class User {
    private String id;
    private String name;
    private int age;
}
