package com.kafka.domain;

import java.util.ArrayList;
import java.util.List;

public class StaticCustomers {

    public static List<Customer> getListOfCustomers(){
        List<Customer> list = new ArrayList<>();
        list.add(new Customer(1, "Ram", "20", "India"));
        list.add(new Customer(2, "Sita", "21", "India"));
        list.add(new Customer(3, "Lakshman", "23", "India"));
        list.add(new Customer(4, "Handuman", "45", "India"));
        list.add(new Customer(5, "Sugriva", "30", "India"));
        return list;
    }
}
