package com.heitaox.sql.executor.core.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * @Author ZhengYingjie
 * @Date 2019-08-09
 * @Description
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Tuple2<K,V> {

    private K v1;

    private V v2;



}
