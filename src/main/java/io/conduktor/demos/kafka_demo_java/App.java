package io.conduktor.demos.kafka_demo_java;

import kafka.basics.ProducerDemo;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Sending to Producer" );
        ProducerDemo producerDemo = new ProducerDemo();
        producerDemo.produce();
        System.out.println( "Sent" );
    }
}
