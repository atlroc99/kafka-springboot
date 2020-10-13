package com.example.consumer.service;

import com.example.consumer.dto.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
@Slf4j
public class CustomerService {

    public void saveCustomer(Customer customer) {
        if (Objects.nonNull(customer)) {
            log.info("\nSaving customer ");
            System.out.println("name : " + customer.getID());
            System.out.println("name : " + customer.getName());
            System.out.println("name : " + customer.getDept());
            System.out.println("name : " + customer.getSalary());
            customer.getAddressList().forEach(System.out:: println);
        }
    }
}
