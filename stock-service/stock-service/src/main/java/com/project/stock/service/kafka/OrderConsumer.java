package com.project.stock.service.kafka;

import com.project.basedomains.dto.OrderEvent;
import com.project.stock.service.kafka.entity.Stock;
import com.project.stock.service.repository.StockRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    @Autowired
    private StockRepository stockRepository;
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumer.class);
    @KafkaListener(topics = "${spring.kafka.topic.name}",
                   groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(OrderEvent event){
        //save the order data into database
        LOGGER.info("order event received in stock service:" + event.toString());
        Stock stock = new Stock();
        stock.setOrderId(event.getOrder().getOrderId());
        stock.setOrderName(event.getOrder().getName());
        stock.setOrderQuantity(event.getOrder().getQty());
        stock.setOrderPrice(event.getOrder().getPrice());
        stock.setOrderStatus(event.getStatus());
        stock.setOrderMessage(event.getMessage());
        stockRepository.save(stock);
    }
}
