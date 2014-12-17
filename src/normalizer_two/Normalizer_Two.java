/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package normalizer_two;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import dk.cphbusiness.connection.ConnectionCreator;
import java.io.IOException;
import org.json.JSONObject;
import org.json.XML;

public class Normalizer_Two {

    private static final String IN_QUEUE = "bank_two_normalizer_gr1";
    private static final String OUT_QUEUE = "aggregator_gr1";

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionCreator creator = ConnectionCreator.getInstance();
        com.rabbitmq.client.Channel channelIn = creator.createChannel();
        com.rabbitmq.client.Channel channelOut = creator.createChannel();
        channelIn.queueDeclare(IN_QUEUE, false, false, false, null);
        channelOut.queueDeclare(OUT_QUEUE, false, false, false, null);

        QueueingConsumer consumer = new QueueingConsumer(channelIn);
        channelIn.basicConsume(IN_QUEUE, true, consumer);

        while (true) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery();
            //channelIn.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            System.out.println(new String(delivery.getBody()));
            System.out.println("CorrelationID" + delivery.getProperties().getCorrelationId());
            String message = translateMessage(delivery);
            BasicProperties prop = new BasicProperties().builder().correlationId(delivery.getProperties().getCorrelationId()).build();
            channelOut.basicPublish("", OUT_QUEUE, prop, message.getBytes());
        }
    }

    private static String translateMessage(QueueingConsumer.Delivery delivery) {
        String message = new String(delivery.getBody());
        JSONObject json = new JSONObject(message);
        json.append("bankName", "JASON the bank of incredible spellingness");
        String xml = "<LoanResponse>" + XML.toString(json) + "</LoanResponse>";
        System.out.println(xml);
        return xml;
    }
    
}
