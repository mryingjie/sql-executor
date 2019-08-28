package com.heitaox.sql.executor.core.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;



@Data
@AllArgsConstructor
@NoArgsConstructor
public class Tuple2<K,V> {

    private K v1;

    private V v2;



}
